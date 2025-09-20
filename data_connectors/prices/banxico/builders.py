
import pytz
import pandas as pd
from .utils import fetch_banxico_series_batched,to_long_with_aliases
import mainsequence.client as msc
import os
from .settings import TIIE_FIXING_ID_MAP,CETE_FIXING_ID_MAP
import datetime
from typing import Dict, List, Mapping, Optional,Callable
from .bootsrap import bootstrap_from_curve_df


def boostrap_mbono_curve(update_statistics, curve_unique_identifier: str, base_node_curve_points=None):
    """
    For each time_index:
      1) Reads node data from `base_node_curve_points` since the last_update.
      2) Bootstraps the zero curve using:
         - overnight_rate (Banxico target) as a 1-day anchor,
         - zero_coupon (Cetes, face = 10),
         - fixed_bond (Mbonos, 182-day coupons, uses dirty price).
      3) Returns ONE dataframe with columns:
           time_index, days_to_maturity, zero_rate

    Assumptions:
      - Money-market simple yield Act/360 (consistent with your IRS code).
      - MBono coupon schedule approximated as exact 182-day spacing from spot; accrued handled via dirty price.
      - Required columns in input frame: ['time_index','type','tenor_days','clean_price','dirty_price','coupon'].
        For overnight rows, use 'dirty_price' to carry the annual rate (e.g. 0.0725).
    """
    # Last processed point for this curve identifier
    last_update = update_statistics.asset_time_statistics[curve_unique_identifier]

    # Pull nodes since last update (inclusive)
    nodes_data_df = base_node_curve_points.get_df_between_dates(
        start_date=last_update,
        great_or_equal=True
    )

    if nodes_data_df.empty:
        # Return empty frame with the expected schema
        return pd.DataFrame()

    results = []

    # Bootstrap per time_index
    for time_index, curve_df in nodes_data_df.groupby("time_index"):
        curve_df = curve_df.copy()
        # robust numeric casting
        curve_df["tenor_days"] = pd.to_numeric(curve_df["days_to_maturity"], errors="coerce")
        curve_df["clean_price"] = pd.to_numeric(curve_df["clean_price"], errors="coerce")
        curve_df["dirty_price"] = pd.to_numeric(curve_df["dirty_price"], errors="coerce")
        curve_df["coupon"] = pd.to_numeric(curve_df["current_coupon"], errors="coerce")

        # Bootstrap one slice
        try:
            zero_df = bootstrap_from_curve_df(curve_df)
        except Exception as e:
            raise e

        zero_df.insert(0, "time_index", time_index)
        results.append(zero_df)

    final_df = pd.concat(results, ignore_index=True)
    final_df["unique_identifier"]=curve_unique_identifier

    grouped = (
        final_df.groupby(["time_index", "unique_identifier"])
        .apply(lambda g: g.set_index("days_to_maturity")["zero_rate"].to_dict())
        .rename("curve")
        .reset_index()
    )

    # 3. Final index and structure (your original code)
    grouped = grouped.set_index(["time_index", "unique_identifier"])


    return grouped


def _update_banxico_fixings(
    *,
    update_statistics,
    unique_identifier: str,
    id_map: Mapping[str, str],
    instrument_label: str,
    value_to_rate: Optional[Callable[[pd.Series], pd.Series]] = None,
) -> pd.DataFrame:
    """
    Generic Banxico SIE fixing updater.

    Parameters
    ----------
    update_statistics : msc.UpdateStatistics
        Object holding per-asset last ingested timestamps (UTC).
    unique_identifier : str
        One of the aliases in `id_map` (e.g., "TIIE_28D", "CETE_91D", etc.).
    id_map : Mapping[str, str]
        Alias -> SIE series id mapping for the instrument family.
    instrument_label : str
        Human-friendly label used for error messages ("TIIE", "CETE", ...).
    value_to_rate : callable(pd.Series) -> pd.Series, optional
        Transform from raw SIE 'value' (typically percent) to decimal rate.
        Defaults to dividing by 100.0.

    Returns
    -------
    pd.DataFrame
        MultiIndex DataFrame indexed by (time_index, unique_identifier)
        with a single 'rate' column (decimal). Empty if nothing to update.
    """
    # --- 0) Validate + token
    assert unique_identifier in id_map, f"Invalid unique identifier for {instrument_label}"
    token = os.getenv("BANXICO_TOKEN")
    if not token:
        raise RuntimeError("BANXICO_TOKEN environment variable is required for Banxico SIE access.")

    if value_to_rate is None:
        value_to_rate = lambda s: s / 100.0  # default: percent -> decimal

    # --- 1) Update window (global single start for all unique_identifiers)
    yday = datetime.datetime.now(pytz.utc).replace(hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=1)

    # Start = last ingested day + 1 (UTC, floored to midnight)
    start_dt = (
        (update_statistics.asset_time_statistics[unique_identifier] + datetime.timedelta(days=1))
        .astimezone(pytz.utc)
        .replace(hour=0, minute=0, second=0, microsecond=0)
    )
    if start_dt > yday:
        return pd.DataFrame()  # nothing to fetch

    start_date = start_dt.date().isoformat()
    end_date = yday.date().isoformat()

    # --- 2) Build SID universe + alias expansion (handles duplicate SIDs mapping to multiple aliases)
    aliases_by_sid: Dict[str, List[str]] = {}
    for alias, sid in id_map.items():
        aliases_by_sid.setdefault(sid, []).append(alias)
    sids = list(aliases_by_sid.keys())

    # --- 3) Pull once + normalize long
    raw = fetch_banxico_series_batched(sids, start_date=start_date, end_date=end_date, token=token)
    long_df = to_long_with_aliases(raw, aliases_by_sid)  # columns: date(UTC), alias, value
    if long_df.empty:
        return pd.DataFrame()

    # --- 4) Build MultiIndex and scale to decimal
    long_df = long_df.rename(columns={"date": "time_index", "alias": "unique_identifier"})
    long_df["rate"] = value_to_rate(long_df["value"])

    out = (
        long_df[["time_index", "unique_identifier", "rate"]]
        .set_index(["time_index", "unique_identifier"])
        .sort_index()
    )
    return out


# --- Thin wrappers keep your public API stable and clear ----------------------

def update_tiie_fixings(update_statistics, unique_identifier: str) -> pd.DataFrame:
    """
    Backward-compatible wrapper for TIIE fixings using the generic updater.
    """
    return _update_banxico_fixings(
        update_statistics=update_statistics,
        unique_identifier=unique_identifier,
        id_map=TIIE_FIXING_ID_MAP,
        instrument_label="TIIE",
        # TIIE values arrive in percent; convert to decimal:
        value_to_rate=lambda s: s / 100.0,
    )


def update_cete_fixing(update_statistics, unique_identifier: str) -> pd.DataFrame:
    """
    New CETE fixing updater sharing the same Banxico source and flow.
    """
    return _update_banxico_fixings(
        update_statistics=update_statistics,
        unique_identifier=unique_identifier,
        id_map=CETE_FIXING_ID_MAP,
        instrument_label="CETE",
        # CETE yields are also reported in percent; convert to decimal:
        value_to_rate=lambda s: s / 100.0,
    )

    return out