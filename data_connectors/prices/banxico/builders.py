
import pytz
import pandas as pd
from .utils import fetch_banxico_series_batched,to_long_with_aliases
import mainsequence.client as msc
import os
from .settings import TIIE_FIXING_ID_MAP
import datetime
from typing import Dict, List


def update_tiie_fixings(update_statistics:msc.UpdateStatistics,unique_identifier:str) -> pd.DataFrame:


    assert unique_identifier in TIIE_FIXING_ID_MAP, "Invalid unique identifier"

    # --- 0) Token
    token = os.getenv("BANXICO_TOKEN")
    if not token:
        raise RuntimeError("BANXICO_TOKEN environment variable is required for Banxico SIE access.")

    # --- 1) Update window (global single start for all unique_identifiers)
    yday = datetime.datetime.now(pytz.UTC).replace(hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=1)
    start_dt = (update_statistics.asset_time_statistics[unique_identifier]+ datetime.timedelta(days=1)).astimezone(pytz.UTC).replace(
            hour=0, minute=0, second=0, microsecond=0
        )

    if start_dt > yday:
        return pd.DataFrame()

    start_date = start_dt.date().isoformat()
    end_date = yday.date().isoformat()

    # --- 2) Build SID universe + alias expansion (handles duplicate SIDs mapping to multiple aliases)
    aliases_by_sid: Dict[str, List[str]] = {}
    for alias, sid in TIIE_FIXING_ID_MAP.items():
        aliases_by_sid.setdefault(sid, []).append(alias)
    sids = list(aliases_by_sid.keys())

    # --- 3) Pull once + normalize long
    raw = fetch_banxico_series_batched(sids, start_date=start_date, end_date=end_date, token=token)
    long_df = to_long_with_aliases(raw, aliases_by_sid)  # columns: date(UTC), alias, value
    if long_df.empty:
        return pd.DataFrame()

    # --- 4) Build MultiIndex and scale to decimal
    long_df = long_df.rename(columns={"date": "time_index", "alias": "unique_identifier"})
    long_df["rate"] = long_df["value"] / 100.0
    out = (
        long_df[["time_index", "unique_identifier", "rate"]]
        .set_index(["time_index", "unique_identifier"])
        .sort_index()
    )

    return out