# fmt: off
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple, Union, DefaultDict
from collections import defaultdict

import pandas as pd

# Main Sequence SDK import rule (guardrail): import as `msc`
import mainsequence.client as msc  # noqa: F401
from mainsequence.tdag import DataNode  # typing/interface only

# Optional progress bar
try:
    from tqdm import tqdm
except Exception:  # pragma: no cover
    def tqdm(iterable, **kwargs):  # type: ignore
        return iterable

UTC = dt.timezone.utc


@dataclass
class MissingnessResult:
    """
    Output container for missingness analysis.
    """
    # One row per contiguous missing block per asset.
    # Columns: ["unique_identifier","missing_start","missing_end","expected_points","duration",
    #           "missing_start_day","missing_end_day"]
    missing_runs: pd.DataFrame

    # One row per timestamp (UTC).
    # Columns: ["present_assets","expected_assets","missing_assets","coverage_ratio"]
    summary_by_time: pd.DataFrame

    # One row per asset. Columns: ["present_points","expected_points","coverage_ratio"]
    asset_coverage: pd.DataFrame

    # Grouped-by-window view: how many assets are missing in each (missing_start, missing_end)
    # Columns: ["missing_start","missing_end","assets_missing","expected_points","duration","start_day","end_day"]
    missing_windows: pd.DataFrame


def analyze_missing_prices(
    prices_node: DataNode,
    start_date: dt.datetime,
    end_date: dt.datetime,
    *,
    frequency: str,
    asset_universe: Optional[Sequence[Union[str, Any]]] = None,
    asset_calendar_map: Optional[Mapping[str, str]] = None,
    # If provided, used to ask the node to return only certain value columns (we only need the index for this analysis)
    # Chunk size: if None, defaults to 1 day for minute data, 90 days for daily
    chunk_size: Optional[dt.timedelta] = None,
    # If False, skips building the potentially large per-timestamp summary
    emit_time_summary: bool = True,
    column_filter: Optional[Sequence[str]] = None,
) -> MissingnessResult:
    """
    Analyze where asset prices are missing, using exchange calendars to define the *expected* time grid per asset.

    Parameters
    ----------
    prices_node : DataNode
        A DataNode whose table uses a UTC time index "time_index" and, for multi-asset data, a MultiIndex
        ("time_index","unique_identifier"). This is the Main Sequence DataNode contract.
    start_date, end_date : datetime
        UTC datetimes. `end_date` is treated as *exclusive* (analyze [start_date, end_date)).
    frequency : {"minute","min","1min","1T","daily","day","1d","d"}
        Sampling frequency of the table (determines the expected grid density).
    asset_universe : Optional[Sequence[str|Any]]
        The assets to analyze (either unique_identifier strings or objects with .unique_identifier).
        If omitted, the universe = union of assets observed + any keys in asset_calendar_map.
    asset_calendar_map : Optional[Mapping[str, str]]
        Maps unique_identifier -> market calendar code (e.g., "XNYS"). Codes like "ARCA"/"AMEX" are normalized to "XNYS".
        Assets without a mapping are treated as having *no expected grid* (they will not contribute to expected counts
        and will have expected_points=0 unless they appear via another mechanism).

    chunk_size : Optional[timedelta]
        Size of time window per call to `get_df_between_dates`. Defaults:
          - minute data: 1 day
          - daily  data: 90 days
    emit_time_summary : bool
        Whether to accumulate a per-timestamp summary (can be large for minute data).
    column_filter : Optional[Sequence[str]]
        Forwarded to `prices_node.get_df_between_dates(..., columns=column_filter)` if supported by your node.

    Returns
    -------
    MissingnessResult
        - missing_runs: contiguous missing intervals per asset with expected point counts (calendar-aware)
        - summary_by_time: per-timestamp present/expected/missing and coverage ratio (calendar-aware)
        - asset_coverage: per-asset coverage computed against the asset's calendar schedule
        - missing_windows: grouped view of identical (missing_start, missing_end) windows

    Notes
    -----
    * Uses exchange calendars (via pandas_market_calendars) to define the expected grid per asset.
    * Keeps memory bounded by chunking time and avoiding giant dense grids.
    """
    # ---- imports required for calendar logic ----
    try:
        import pandas_market_calendars as mcal  # type: ignore
    except Exception as e:  # pragma: no cover
        raise ImportError(
            "pandas_market_calendars is required for calendar-aware missingness. "
            "Install with `pip install pandas-market-calendars`."
        ) from e

    # ---- normalize inputs & frequency ----
    start = _normalize_utc(start_date)
    end_excl = _normalize_utc(end_date)
    if end_excl <= start:
        raise ValueError("end_date must be greater than start_date (exclusive end).")

    step = _freq_to_step(frequency)
    if chunk_size is None:
        chunk_size = dt.timedelta(days=1) if step == dt.timedelta(minutes=1) else dt.timedelta(days=90)

    # ---- universe coercion ----
    universe_uids: Optional[List[str]] = None
    if asset_universe is not None:
        universe_uids = _coerce_to_uid_list(asset_universe)

    # Build the working universe = provided universe ∪ calendar-mapped assets (so unmapped assets aren't dropped)
    calendar_assets = set(asset_calendar_map.keys()) if asset_calendar_map else set()
    working_universe: set[str] = set(universe_uids or []) | calendar_assets

    # ---- prepare calendars (per distinct code) ----
    def _norm_cal_code(c: str) -> str:
        return c.upper().replace("ARCA", "XNYS").replace("AMEX", "XNYS")

    calendars: Dict[str, Any] = {}
    if asset_calendar_map:
        for code in { _norm_cal_code(c) for c in asset_calendar_map.values() }:
            calendars[code] = mcal.get_calendar(code)

    # ---- accumulators ----
    # Per-asset present/expected counts
    present_pts: DefaultDict[str, int] = defaultdict(int)
    expected_pts: DefaultDict[str, int] = defaultdict(int)

    # Missing runs (appended rows)
    missing_rows: List[Dict[str, Any]] = []

    # Per-timestamp series lists
    present_series_list: List[pd.Series] = []
    expected_series_list: List[pd.Series] = []

    # Cross-chunk state to stitch runs
    open_missing_start: Dict[str, Optional[dt.datetime]] = {}          # where an open missing run started (UTC)
    open_missing_len: DefaultDict[str, int] = defaultdict(int)         # how many expected points missing so far
    last_missing_ts: Dict[str, Optional[dt.datetime]] = {}             # last missing expected ts we saw

    # ---- iterate over time in chunks ----
    for b_start, b_end_excl in tqdm(_iterate_time_ranges(start, end_excl, chunk_size), desc="calendar-missingness"):
        # 1) Fetch observed pairs once for the chunk

        update_range_map={k:{"start_date":b_start,"end_date":b_end_excl,"start_date_operand":">","end_date_operand":"<="} for k in asset_universe}
        df = prices_node.get_ranged_data_per_asset(
            range_descriptor=update_range_map
        )

        times_by_asset: Dict[str, List[dt.datetime]] = {}
        if df is not None and not df.empty:
            idx = df.index
            # Validate MultiIndex with names ("time_index","unique_identifier") — DataNode contract
            if not isinstance(idx, pd.MultiIndex) or {"time_index", "unique_identifier"} - set(idx.names):
                raise ValueError("Expected MultiIndex with levels ['time_index','unique_identifier'].")

            pairs = (
                idx.to_frame(index=False)[["time_index", "unique_identifier"]]
                .drop_duplicates()
                .sort_values(["unique_identifier", "time_index"])
            )
            # Ensure UTC tz-aware times
            if pairs["time_index"].dt.tz is None:
                pairs["time_index"] = pairs["time_index"].dt.tz_localize(UTC)
            else:
                pairs["time_index"] = pairs["time_index"].dt.tz_convert(UTC)

            # present-by-time for this chunk (observed)
            if emit_time_summary:
                s_present = pairs.groupby("time_index")["unique_identifier"].nunique()
                s_present.name = "present_assets"
                present_series_list.append(s_present)

            # times per asset (observed in this chunk)
            times_by_asset = pairs.groupby("unique_identifier")["time_index"].apply(list).to_dict()

        # 2) Determine assets to process this batch
        if universe_uids:
            assets_this_batch = set(universe_uids)
        else:
            # Union of observed, calendar-declared, and any assets with open runs
            assets_this_batch = set(times_by_asset).union(calendar_assets).union(open_missing_start.keys())

        # Optionally trim to working universe if one was provided
        if working_universe:
            assets_this_batch &= working_universe

        if not assets_this_batch:
            continue

        # 3) Group assets by calendar code (unmapped assets fall into None bucket)
        assets_by_cal: DefaultDict[Optional[str], List[str]] = defaultdict(list)
        for uid in assets_this_batch:
            cal_code = None
            if asset_calendar_map:
                cal_code = _norm_cal_code(asset_calendar_map.get(uid, None)) if uid in asset_calendar_map else None
            assets_by_cal[cal_code].append(uid)

        # 4) For each calendar, build the expected grid once; reuse across its assets
        #    Also accumulate expected counts per timestamp with a single vector add × (#assets on that calendar).
        if emit_time_summary:
            # chunk-level expected counts
            expected_counts_map: DefaultDict[pd.Timestamp, int] = defaultdict(int)
        else:
            expected_counts_map = None  # type: ignore

        for cal_code, uids in assets_by_cal.items():
            if cal_code is None:
                # No calendar -> no expectations for these assets in this chunk
                # We still count their observed presence in present_series_list (already done).
                continue

            cal = calendars.get(cal_code)
            if cal is None:
                # Shouldn't happen; but be defensive
                calendars[cal_code] = cal = mcal.get_calendar(cal_code)

            # Build expected timestamps for this calendar & chunk
            expected_ts_list = _expected_ts_for_calendar(cal, b_start, b_end_excl, step)
            if not expected_ts_list:
                # Nothing expected in this chunk on this calendar (e.g., full holiday/weekend window)
                # If some assets have open missing runs, we leave them open and move on.
                continue

            expected_ts_list = sorted(expected_ts_list)  # ensure monotonic
            expected_ts_set = set(expected_ts_list)

            # Per-timestamp expected count for the calendar (vector add by #assets in this calendar)
            if emit_time_summary:
                n_assets_cal = len(uids)
                for t in expected_ts_list:
                    expected_counts_map[t] += n_assets_cal

            # 5) Asset-level coverage & runs for this calendar
            for uid in uids:
                observed_list = times_by_asset.get(uid, [])
                observed_set = set(observed_list)

                # Count present only where expected
                if observed_set:
                    present_pts[uid] += sum(1 for t in observed_set if t in expected_ts_set)

                # Count expected for this asset in this chunk
                expected_pts[uid] += len(expected_ts_list)

                # (A) If an open missing run existed from prior chunk and the first expected ts here is PRESENT,
                #     we must close the prior run at the last_missing_ts we saw earlier.
                if open_missing_start.get(uid) is not None and expected_ts_list and (expected_ts_list[0] in observed_set):
                    _record_missing_points(
                        rows=missing_rows,
                        uid=uid,
                        start=open_missing_start[uid],
                        end=last_missing_ts.get(uid),
                        num_points=open_missing_len.get(uid, 0),
                        step=step,
                    )
                    open_missing_start[uid] = None
                    open_missing_len[uid] = 0
                    last_missing_ts[uid] = None

                # (B) Walk the expected grid; open/extend/close missing runs
                for e in expected_ts_list:
                    if e in observed_set:
                        # Close any open run right before this present stamp
                        if open_missing_start.get(uid) is not None:
                            _record_missing_points(
                                rows=missing_rows,
                                uid=uid,
                                start=open_missing_start[uid],
                                end=last_missing_ts.get(uid),
                                num_points=open_missing_len.get(uid, 0),
                                step=step,
                            )
                            open_missing_start[uid] = None
                            open_missing_len[uid] = 0
                            last_missing_ts[uid] = None
                        # else: nothing to close
                    else:
                        # Missing at e
                        if open_missing_start.get(uid) is None:
                            open_missing_start[uid] = e
                            open_missing_len[uid] = 1
                        else:
                            open_missing_len[uid] += 1
                        last_missing_ts[uid] = e

        # 6) Commit expected counts for this chunk to the series list
        if emit_time_summary and expected_counts_map:
            s_expected = pd.Series(expected_counts_map, dtype="int64").sort_index()
            s_expected.index.name = "time_index"
            s_expected.name = "expected_assets"
            expected_series_list.append(s_expected)

    # ---- finalize across all chunks ----
    # Close any runs still open at the last expected timestamp we’ve seen for each asset
    for uid, start_ts in list(open_missing_start.items()):
        if start_ts is not None and last_missing_ts.get(uid) is not None:
            _record_missing_points(
                rows=missing_rows,
                uid=uid,
                start=start_ts,
                end=last_missing_ts[uid],
                num_points=open_missing_len.get(uid, 0),
                step=step,
            )
        # Clear state
        open_missing_start[uid] = None
        open_missing_len[uid] = 0
        last_missing_ts[uid] = None

    # Missing runs table (+ day-of-week)
    if missing_rows:
        missing_runs = (
            pd.DataFrame(missing_rows)
            .sort_values(["unique_identifier", "missing_start"])
            .reset_index(drop=True)
        )
        # Normalize dtypes & add weekday labels
        missing_runs["missing_start"] = pd.to_datetime(missing_runs["missing_start"], utc=True)
        missing_runs["missing_end"]   = pd.to_datetime(missing_runs["missing_end"],   utc=True)
        missing_runs["missing_start_day"] = missing_runs["missing_start"].dt.day_name()
        missing_runs["missing_end_day"]   = missing_runs["missing_end"].dt.day_name()
    else:
        missing_runs = pd.DataFrame(
            columns=["unique_identifier","missing_start","missing_end","expected_points","duration",
                     "missing_start_day","missing_end_day"]
        )

    # Grouped windows
    if not missing_runs.empty:
        missing_windows = (
            missing_runs
            .groupby(["missing_start","missing_end"], as_index=False)
            .agg(
                assets_missing=("unique_identifier","nunique"),
                expected_points=("expected_points","first"),
                duration=("duration","first"),
            )
            .sort_values(["missing_start","missing_end"])
        )
        missing_windows["start_day"] = missing_windows["missing_start"].dt.day_name()
        missing_windows["end_day"]   = missing_windows["missing_end"].dt.day_name()
    else:
        missing_windows = pd.DataFrame(
            columns=["missing_start","missing_end","assets_missing","expected_points","duration","start_day","end_day"]
        )

    # Asset coverage
    # If an asset had no calendar mapping (expected_pts=0), coverage_ratio will be NaN
    asset_rows = []
    for uid in sorted(set(list(present_pts.keys()) + list(expected_pts.keys()))):
        exp_ = int(expected_pts.get(uid, 0))
        pres = int(present_pts.get(uid, 0))
        asset_rows.append(
            {
                "unique_identifier": uid,
                "present_points": pres,
                "expected_points": exp_,
                "coverage_ratio": (pres / exp_) if exp_ > 0 else float("nan"),
            }
        )
    asset_coverage = pd.DataFrame(asset_rows).set_index("unique_identifier").sort_index()

    # Time summary
    if emit_time_summary:
        if expected_series_list:
            expected_by_time = pd.concat(expected_series_list).groupby(level=0).sum().sort_index()
        else:
            expected_by_time = pd.Series(dtype="int64", name="expected_assets")
            expected_by_time.index.name = "time_index"

        if present_series_list:
            present_by_time = pd.concat(present_series_list).groupby(level=0).sum().sort_index()
        else:
            present_by_time = pd.Series(dtype="int64", name="present_assets")
            present_by_time.index.name = "time_index"

        # Align present to the expected grid; outside the expected grid, we consider no expectation
        summary_by_time = pd.DataFrame({"expected_assets": expected_by_time})
        summary_by_time["present_assets"] = present_by_time.reindex(summary_by_time.index, fill_value=0)
        summary_by_time["missing_assets"] = summary_by_time["expected_assets"] - summary_by_time["present_assets"]
        # Avoid divide-by-zero; where expected==0, coverage is NaN (no expectation at that ts)
        with pd.option_context("mode.use_inf_as_na", True):
            summary_by_time["coverage_ratio"] = summary_by_time["present_assets"] / summary_by_time["expected_assets"]
    else:
        summary_by_time = pd.DataFrame(columns=["present_assets","expected_assets","missing_assets","coverage_ratio"])
        summary_by_time.index.name = "time_index"

    return MissingnessResult(
        missing_runs=missing_runs,
        summary_by_time=summary_by_time,
        asset_coverage=asset_coverage,
        missing_windows=missing_windows,
    )


# ----------------- helpers ----------------- #

def _normalize_utc(x: dt.datetime) -> dt.datetime:
    if x.tzinfo is None:
        return x.replace(tzinfo=UTC)
    return x.astimezone(UTC)

def _freq_to_step(freq: str) -> dt.timedelta:
    f = freq.strip().lower()
    if f in {"minute","min","1min","1t"}:
        return dt.timedelta(minutes=1)
    if f in {"daily","day","1d","d"}:
        return dt.timedelta(days=1)
    raise ValueError("Unsupported frequency. Use: minute|min|1min|1T or daily|day|1d|d.")

def _iterate_time_ranges(
    start: dt.datetime, end_excl: dt.datetime, chunk: dt.timedelta
) -> Iterable[Tuple[dt.datetime, dt.datetime]]:
    cur = start
    while cur < end_excl:
        nxt = min(cur + chunk, end_excl)
        yield cur, nxt
        cur = nxt

def _coerce_to_uid_list(items: Sequence[Union[str, Any]]) -> List[str]:
    out: List[str] = []
    for it in items:
        if isinstance(it, str):
            out.append(it)
        else:
            uid = getattr(it, "unique_identifier", None)
            if not isinstance(uid, str):
                raise TypeError("Items must be unique_identifier strings or objects with a .unique_identifier str")
            out.append(uid)
    return out

def _expected_ts_for_calendar(cal, start_dt: dt.datetime, end_dt: dt.datetime, step: dt.timedelta) -> List[pd.Timestamp]:
    """
    Build expected timestamps for a calendar in [start_dt, end_dt) in UTC.
    For 'daily', uses market_close; for 'minute', builds per-minute grid within [open, close).
    """
    # schedule returns a DataFrame indexed by session dates with market_open/market_close (tz-aware)
    # Use end_dt - 1 second to ensure we include the last session that has close < end_dt
    schedule = cal.schedule(start_dt.date(), (end_dt - dt.timedelta(seconds=1)).date())
    if schedule.empty:
        return []

    if step == dt.timedelta(days=1):
        closes = schedule["market_close"]
        # Ensure UTC
        if getattr(closes.dt, "tz", None) is None:
            closes = closes.dt.tz_localize("UTC")
        else:
            closes = closes.dt.tz_convert("UTC")
        # Keep closes in [start_dt, end_dt)
        out = closes[(closes >= start_dt) & (closes < end_dt)]
        return list(out.sort_values().to_list())

    # Minute frequency
    expected: List[pd.Timestamp] = []
    opens = schedule["market_open"]
    closes = schedule["market_close"]
    # Ensure UTC tz
    if getattr(opens.dt, "tz", None) is None:
        opens = opens.dt.tz_localize("UTC")
    else:
        opens = opens.dt.tz_convert("UTC")
    if getattr(closes.dt, "tz", None) is None:
        closes = closes.dt.tz_localize("UTC")
    else:
        closes = closes.dt.tz_convert("UTC")

    for o, c in zip(opens, closes):
        # Clip each session to [start_dt, end_dt)
        session_start = max(o, start_dt)
        session_end_excl = min(c, end_dt)
        if session_end_excl <= session_start:
            continue
        # Build minute grid [open, close) — last minute ends at close-1min
        rng = pd.date_range(start=session_start, end=session_end_excl - dt.timedelta(minutes=1), freq="T", tz="UTC")
        if len(rng):
            expected.extend(rng.to_list())
    return expected

def _record_missing_points(
    rows: List[Dict[str, Any]],
    uid: str,
    start: Optional[dt.datetime],
    end: Optional[dt.datetime],
    num_points: int,
    step: dt.timedelta,
) -> None:
    """
    Record a missing run with an exact expected_points count (calendar-aware).
    """
    if start is None or end is None or num_points <= 0:
        return
    rows.append(
        {
            "unique_identifier": uid,
            "missing_start": start,
            "missing_end": end,
            "expected_points": int(num_points),
            "duration": (end - start + step),
        }
    )


def update_calendar_holes(data_node:DataNode):

    """

    """
    asset_list=data_node.get_asset_list()
    asset_list=[a for a in asset_list if a.ticker=="VICI"] #TEST

    #1 Get Price Summary
    from data_connectors.helpers.asset_consistency import analyze_missing_prices


    update_stats=data_node.local_persist_manager.metadata.sourcetableconfiguration.get_data_updates()
    update_stats=update_stats.update_assets(asset_list=asset_list,)
    update_stats._initial_fallback_date=data_node.OFFSET_START
    last_update_in_list=update_stats.get_max_time_in_update_statistics()





    asset_calendar_map= {a.unique_identifier: a.get_calendar().name for a in asset_list}

    results = analyze_missing_prices(
        data_node,
        start_date=data_node.OFFSET_START,
        end_date=last_update_in_list,
        frequency=data_node.frequency_id,
        asset_universe=list(asset_calendar_map.keys()),
        asset_calendar_map=asset_calendar_map,
        chunk_size=dt.timedelta(days=90),
        column_filter=["close"],
    )

    uid_ticker_map={a.unique_identifier:a.ticker for a in asset_list}
    results.missing_runs["ticker"]=results.missing_runs["unique_identifier"].map(uid_ticker_map)

    missing_runs=results.missing_runs
    for _,row in results.missing_windows.iterrows():

        asset_in_row=missing_runs[(missing_runs.missing_start==row.missing_start)
                                    & (missing_runs.missing_end==row.missing_end)
        ]

        tmp_asset_list=[a for a in asset_list if a.unique_identifier in asset_in_row.unique_identifier.to_list()]

        tmp_update_stats= data_node.local_persist_manager.metadata.sourcetableconfiguration.get_data_updates()
        tmp_update_stats=tmp_update_stats.update_assets(asset_list=tmp_asset_list,)
        tmp_update_stats.limit_update_time=row.missing_end+dt.timedelta(days=1)

        tmp_update_stats.asset_time_statistics={k:row.missing_start-dt.timedelta(days=1) for k in tmp_update_stats.asset_time_statistics.keys()}
        tmp_update_stats.max_time_index_value=row.missing_end

        data_node.run(debug_mode=True,force_update=True,override_update_stats=tmp_update_stats)

        a=5

# fmt: on


