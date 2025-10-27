# fmt: off
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple, Union, DefaultDict
from collections import defaultdict

import pandas as pd
import numpy as np
# Main Sequence SDK import rule (guardrail): import as `msc`
import mainsequence.client as msc  # noqa: F401
from mainsequence.tdag import DataNode  # typing/interface only




from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Optional, Sequence, Union, Any, Dict, List




# Optional progress bar
try:
    from tqdm import tqdm
except Exception:  # pragma: no cover
    def tqdm(iterable, **kwargs):  # type: ignore
        return iterable

UTC = dt.timezone.utc



# ----------------- helpers ----------------- #

def _normalize_utc(x: dt.datetime) -> dt.datetime:
    if x.tzinfo is None:
        return x.replace(tzinfo=UTC)
    return x.astimezone(UTC)

def _freq_to_step(freq: str) -> dt.timedelta:
    f = freq.strip().lower()
    if f in {"minute","min","1min","1t","1m"}:
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
        rng = pd.date_range(start=session_start, end=session_end_excl - dt.timedelta(minutes=1), freq="min", tz="UTC")
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

    uid_minima:Optional[Dict[str, dt.datetime]]=None

# Assume other necessary imports like tqdm, mcal, MissingnessResult, etc., are present
from bisect import bisect_right

def _process_calendar_chunk(
    cal_code: str, uids: List[str], b_start: dt.datetime, b_end_excl: dt.datetime,
    step: dt.timedelta, times_by_asset: Dict[str, List[dt.datetime]],
    open_missing_start: Dict[str, Optional[dt.datetime]],
    open_missing_len: Dict[str, int], last_missing_ts: Dict[str, Optional[dt.datetime]],
    emit_time_summary: bool, uid_minima: Optional[Dict] = None
) -> Dict[str, Any]:

    import pandas_market_calendars as mcal
    mcal_calendar = mcal.get_calendar(cal_code)
    expected_ts_list = sorted(_expected_ts_for_calendar(mcal_calendar, b_start, b_end_excl, step))
    if not expected_ts_list:
        return {"status": "skipped"}

    expected_ts_set = set(expected_ts_list)

    # --- Build observed matrix (UID x time) ---
    uid_to_idx = {uid: i for i, uid in enumerate(uids)}
    ts_to_idx  = {ts:  i for i, ts  in enumerate(expected_ts_list)}
    M, N = len(uids), len(expected_ts_list)
    observed = np.zeros((M, N), dtype=bool)
    for uid in uids:
        for ts in set(times_by_asset.get(uid, [])) & expected_ts_set:
            observed[uid_to_idx[uid], ts_to_idx[ts]] = True

    # --- Build expected mask from uid_minima (what we *expect* to exist) ---
    expected_mask = np.zeros((M, N), dtype=bool)
    first_expected_idx = np.zeros(M, dtype=int)  # first in-scope idx per uid
    if uid_minima:
        for i, uid in enumerate(uids):
            min_ts = uid_minima.get(uid)
            cut = bisect_right(expected_ts_list, min_ts) if min_ts is not None else 0
            first_expected_idx[i] = cut
            if cut < N:
                expected_mask[i, cut:] = True
    else:
        expected_mask[:] = True
        # already zero for first_expected_idx

    # --- Present/expected counts *in scope* ---
    observed_in_scope = observed & expected_mask
    present_counts_vec   = observed_in_scope.sum(axis=1).astype(np.int64)
    expected_counts_vec  = expected_mask.sum(axis=1).astype(np.int64)
    present_counts  = {uid: int(present_counts_vec[i])  for i, uid in enumerate(uids)}
    expected_counts = {uid: int(expected_counts_vec[i]) for i, uid in enumerate(uids)}

    # --- Time summaries (cross‑section at each ts) ---
    if emit_time_summary:
        s_present  = pd.Series(observed_in_scope.sum(axis=0), index=expected_ts_list, dtype="int64")
        s_expected = pd.Series(expected_mask.sum(axis=0),     index=expected_ts_list, dtype="int64")
    else:
        s_present  = pd.Series(dtype="int64")
        s_expected = pd.Series(dtype="int64")

    # --- Close any open run that ends because the *first expected* point is present ---
    is_first_ts_present_arr = np.zeros(M, dtype=bool)
    for i, uid in enumerate(uids):
        idx0 = first_expected_idx[i]
        if idx0 < N:
            is_first_ts_present_arr[i] = observed[i, idx0]  # OK: idx0 is “expected” by construction

    chunk_missing_rows = []
    for i, uid in enumerate(uids):
        if open_missing_start.get(uid) and is_first_ts_present_arr[i]:
            chunk_missing_rows.append({
                "uid": uid,
                "start": open_missing_start.pop(uid),
                "end":   last_missing_ts.pop(uid),
                "num_points": open_missing_len.pop(uid, 0),
            })

    # --- Missing matrix is: expected and not observed ---
    missing = expected_mask & (~observed)
    if not missing.any():
        return {
            "status": "success",
            "present_counts": present_counts,
            "expected_counts": expected_counts,      # <-- NEW
            "missing_rows": chunk_missing_rows,
            "open_missing_start": open_missing_start,
            "open_missing_len": open_missing_len,
            "last_missing_ts": last_missing_ts,
            "s_expected": s_expected,                # per-time expected assets
            "s_present": s_present,                  # per-time present assets
        }

    # --- Run-length encode missing per UID to emit windows ---
    run_groups = (missing != np.roll(missing, 1, axis=1)).cumsum(axis=1)
    run_groups[~missing] = 0

    for i, uid in enumerate(uids):
        row = run_groups[i, :]
        if not row.any():
            continue
        run_ids, run_starts_idx, run_counts = np.unique(row, return_index=True, return_counts=True)
        if run_ids[0] == 0:
            run_starts_idx = run_starts_idx[1:]
            run_counts = run_counts[1:]
        for start_idx, count in zip(run_starts_idx, run_counts):
            run_start_ts = expected_ts_list[start_idx]
            run_end_ts   = expected_ts_list[start_idx + count - 1]
            if open_missing_start.get(uid) and (run_start_ts - last_missing_ts.get(uid, run_start_ts) <= step):
                open_missing_len[uid] += count
                last_missing_ts[uid] = run_end_ts
            else:
                if open_missing_start.get(uid):
                    chunk_missing_rows.append({
                        "uid": uid,
                        "start": open_missing_start.pop(uid),
                        "end":   last_missing_ts.pop(uid),
                        "num_points": open_missing_len.pop(uid, 0),
                    })
                open_missing_start[uid] = run_start_ts
                open_missing_len[uid]   = count
                last_missing_ts[uid]    = run_end_ts

    return {
        "status": "success",
        "present_counts": present_counts,
        "expected_counts": expected_counts,          # <-- NEW
        "missing_rows": chunk_missing_rows,
        "open_missing_start": open_missing_start,
        "open_missing_len": open_missing_len,
        "last_missing_ts": last_missing_ts,
        "s_expected": s_expected,
        "s_present": s_present,
    }


def analyze_missing_data(
        data_node: 'DataNode', start_date: dt.datetime, end_date: dt.datetime, *,
        frequency: str, asset_universe: Optional[Sequence[Union[str, Any]]] = None,
        chunk_size: Optional[dt.timedelta] = None, emit_time_summary: bool = True,
        column_filter: Optional[Sequence[str]] = None, num_workers: int = 1,
        use_minimals: bool = False
) -> MissingnessResult:
    start = _normalize_utc(start_date)
    end_excl = _normalize_utc(end_date)
    step = _freq_to_step(frequency)
    chunk_size = chunk_size or (dt.timedelta(days=1) if step == dt.timedelta(minutes=1) else dt.timedelta(days=90))

    initial_uids = _coerce_to_uid_list(asset_universe) if asset_universe else list(
        data_node.metadata.sourcetableconfiguration.multi_index_stats["max_per_asset_symbol"].keys())
    # Placeholder for your msc.Asset logic
    # universe_assets = msc.Asset.filter(unique_identifier__in=initial_uids)
    # asset_calendar_map = {a.unique_identifier: a.get_calendar().name for a in universe_assets}
    asset_calendar_map = {uid: 'XNYS' for uid in initial_uids}

    def _norm_cal_code(c: str) -> str:
        return c.upper().replace("ARCA", "XNYS").replace("AMEX", "XNYS")

    assets_by_cal = defaultdict(list)
    for uid, cal_name in asset_calendar_map.items():
        assets_by_cal[_norm_cal_code(cal_name)].append(uid)

    present_pts, expected_pts, missing_rows, present_series_list, expected_series_list = defaultdict(int), defaultdict(
        int), [], [], []
    open_missing_start, open_missing_len, last_missing_ts = {}, defaultdict(int), {}

    uid_minima=None
    if use_minimals:
        global_minima,uid_minima=data_node.data_source.related_resource.get_earliest_value(local_metadata=data_node.local_time_serie)

    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        ranges = list(_iterate_time_ranges(start, end_excl, chunk_size))
        for b_start, b_end_excl in tqdm(ranges, desc="calendar-missingness"):
            df = data_node.get_df_between_dates(start_date=b_start, end_date=b_end_excl,
                                                unique_identifier_list=initial_uids, columns=column_filter)
            times_by_asset = defaultdict(list)
            if df is not None and not df.empty:
                for ts, uid in df.index.to_flat_index(): times_by_asset[uid].append(ts)

            futures = {
                executor.submit(_process_calendar_chunk, cal_code, uids, b_start, b_end_excl, step, times_by_asset,
                                {k: v for k, v in open_missing_start.items() if k in uids},
                                defaultdict(int, {k: v for k, v in open_missing_len.items() if k in uids}),
                                {k: v for k, v in last_missing_ts.items() if k in uids}, emit_time_summary,uid_minima): cal_code
                for cal_code, uids in assets_by_cal.items()}
            for future in as_completed(futures):
                result = future.result()
                if result.get('status') == 'success':
                    # (cal_code = futures[future])  # only needed for logging

                    # Per-UID totals (present / expected)
                    for uid, cnt in result['present_counts'].items():
                        present_pts[uid] += int(cnt)
                    for uid, cnt in result.get('expected_counts', {}).items():
                        expected_pts[uid] += int(cnt)

                    # Missing windows
                    for row in result['missing_rows']:
                        _record_missing_points(rows=missing_rows, step=step, **row)

                    # Carry open runs over chunk boundary
                    open_missing_start.update(result['open_missing_start'])
                    open_missing_len.update(result['open_missing_len'])
                    last_missing_ts.update(result['last_missing_ts'])

                    # Time summaries
                    if emit_time_summary:
                        if not result['s_present'].empty:
                            present_series_list.append(result['s_present'])
                        if not result['s_expected'].empty:
                            expected_series_list.append(result['s_expected'])

    for uid, start_ts in list(open_missing_start.items()):
        if start_ts is not None and last_missing_ts.get(uid) is not None:
            _record_missing_points(rows=missing_rows, uid=uid, start=start_ts, end=last_missing_ts[uid],
                                   num_points=open_missing_len.get(uid, 0), step=step)

    if missing_rows:
        missing_runs = pd.DataFrame(missing_rows).sort_values(["unique_identifier", "missing_start"]).reset_index(
            drop=True)
        missing_runs["missing_start"] = pd.to_datetime(missing_runs["missing_start"], utc=True)
        missing_runs["missing_end"] = pd.to_datetime(missing_runs["missing_end"], utc=True)
        missing_runs["missing_start_day"] = missing_runs["missing_start"].dt.day_name()
        missing_runs["missing_end_day"] = missing_runs["missing_end"].dt.day_name()
    else:
        missing_runs = pd.DataFrame(
            columns=["unique_identifier", "missing_start", "missing_end", "expected_points", "duration",
                     "missing_start_day", "missing_end_day"])

    if not missing_runs.empty:
        missing_windows = missing_runs.groupby(["missing_start", "missing_end"], as_index=False).agg(
            assets_missing=("unique_identifier", "nunique"), expected_points=("expected_points", "first"),
            duration=("duration", "first")).sort_values(["missing_start", "missing_end"])
        missing_windows["start_day"] = missing_windows["missing_start"].dt.day_name()
        missing_windows["end_day"] = missing_windows["missing_end"].dt.day_name()
    else:
        missing_windows = pd.DataFrame(
            columns=["missing_start", "missing_end", "assets_missing", "expected_points", "duration", "start_day",
                     "end_day"])

    present_s = pd.Series(present_pts, name="present_points", dtype=int)
    expected_s = pd.Series(expected_pts, name="expected_points", dtype=int)

    # Combine into a single DataFrame, filling missing values with 0
    asset_coverage = pd.concat([present_s, expected_s], axis=1).fillna(0).astype(int)

    if emit_time_summary:
        if expected_series_list:
            expected_by_time = pd.concat(expected_series_list).groupby(level=0).sum().sort_index()
        else:
            expected_by_time = pd.Series(dtype="int64", name="expected_assets")
        if present_series_list:
            present_by_time = pd.concat(present_series_list).groupby(level=0).sum().sort_index()
        else:
            present_by_time = pd.Series(dtype="int64", name="present_assets")

        summary_by_time = pd.DataFrame({"expected_assets": expected_by_time})
        summary_by_time["present_assets"] = present_by_time.reindex(summary_by_time.index, fill_value=0)
        summary_by_time["missing_assets"] = summary_by_time["expected_assets"] - summary_by_time["present_assets"]
        with np.errstate(divide='ignore', invalid='ignore'):
            summary_by_time["coverage_ratio"] = summary_by_time["present_assets"] / summary_by_time["expected_assets"]
        summary_by_time.index.name = "time_index"
    else:
        summary_by_time = pd.DataFrame(
            columns=["present_assets", "expected_assets", "missing_assets", "coverage_ratio"])
        summary_by_time.index.name = "time_index"

    return MissingnessResult(missing_runs=missing_runs, summary_by_time=summary_by_time, asset_coverage=asset_coverage,
                             missing_windows=missing_windows,uid_minima=uid_minima)




def analyze_missing_data_in_table(data_node: DataNode,start_date: dt.datetime, frequency,
                                  chunk_size:dt.timedelta=dt.timedelta(days=90),
                                  num_workers=1,use_minimals=True) -> MissingnessResult:
    from src.data_connectors.helpers.asset_consistency import analyze_missing_data

    update_stats = data_node.local_persist_manager.metadata.sourcetableconfiguration.get_data_updates()
    update_stats._initial_fallback_date = data_node.OFFSET_START
    last_update_in_list = update_stats.get_max_time_in_update_statistics()

    results = analyze_missing_data(
        data_node,
        start_date=start_date,
        end_date=last_update_in_list,
        frequency=frequency,
        asset_universe=None,
        chunk_size=chunk_size,
        column_filter=["close"],
        num_workers=num_workers,
        use_minimals=use_minimals,
    )

    return results

def update_calendar_holes(data_node:DataNode,start_date:dt.datetime,
                          frequency,chunk_size:dt.timedelta=dt.timedelta(days=90),
                          num_workers=1) -> None:

    """

    """

    #1 Get Price Summary
    results=analyze_missing_data_in_table(data_node=data_node,start_date=start_date,frequency=frequency,
                                          use_minimals=True,
                                          chunk_size=chunk_size,num_workers=num_workers)

    relevant_assets=msc.Asset.filter(unique_identifier__in=results.missing_runs.unique_identifier.unique().tolist())
    uid_ticker_map={a.unique_identifier:a.ticker for a in relevant_assets}
    results.missing_runs["ticker"]=results.missing_runs["unique_identifier"].map(uid_ticker_map)

    missing_runs=results.missing_runs
    missing_windows=results.missing_windows


    #filter minute misseing
    missing_windows=missing_windows[missing_windows.duration.dt.total_seconds()>60*60*48]
    missing_windows=missing_windows.sort_values(["expected_points","assets_missing"])
    missing_windows=missing_windows.sort_values(["expected_points", "assets_missing"], ascending=False)

    for _,row in missing_windows.iterrows():

        asset_in_row=missing_runs[(missing_runs.missing_start==row.missing_start)
                                    & (missing_runs.missing_end==row.missing_end)
        ]

        tmp_asset_list=[a for a in relevant_assets if a.unique_identifier in asset_in_row.unique_identifier.to_list()]

        tmp_update_stats= data_node.local_persist_manager.metadata.sourcetableconfiguration.get_data_updates()
        tmp_update_stats=tmp_update_stats.update_assets(asset_list=tmp_asset_list,)
        tmp_update_stats.limit_update_time=row.missing_end

        tmp_update_stats.asset_time_statistics={k:row.missing_start-dt.timedelta(days=1) for k in tmp_update_stats.asset_time_statistics.keys()}
        tmp_update_stats.max_time_index_value=row.missing_end
        tmp_update_stats.is_backfill=True
        #todo:data_node needs to be rebuild from configuration to avoid state settings
        error_on_last_update,updated_df=data_node.run(debug_mode=True,force_update=True,override_update_stats=tmp_update_stats)

        a=5



# fmt: on


