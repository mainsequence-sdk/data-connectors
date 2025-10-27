# data_connectors/prices/fred/utils.py
from __future__ import annotations

import os
import time
import datetime as dt
from typing import Mapping, Dict, List, Optional, Callable

import pytz
import requests
import pandas as pd


UTC = pytz.utc
FRED_API_BASE = "https://api.stlouisfed.org/fred"


# --- small HTTP helper (simple, reliable) ---
def _fred_get_observations(
    series_id: str,
    *,
    api_key: str,
    start_date: str,
    end_date: str,
    timeout: float = 30.0,
) -> pd.DataFrame:
    """
    Fetch daily observations for one FRED series_id in [start_date, end_date].
    Returns DataFrame with ['date','value'] (date string, value float or NaN).
    """
    url = f"{FRED_API_BASE}/series/observations"
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start_date,
        "observation_end": end_date,
        "sort_order": "asc",
    }

    # naive backoff on transient failures
    backoff = 0.5
    for _ in range(5):
        r = requests.get(url, params=params, timeout=timeout)
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(backoff)
            backoff = min(backoff * 2, 8.0)
            continue
        r.raise_for_status()
        data = r.json().get("observations", [])
        df = pd.DataFrame(data, columns=["date", "value"])
        if df.empty:
            return pd.DataFrame(columns=["date", "value"])
        # FRED uses "." for missing
        df["value"] = pd.to_numeric(df["value"].replace(".", None), errors="coerce")
        return df

    raise RuntimeError(f"FRED API: exhausted retries for series_id={series_id}")


def fetch_fred_series_batched(
    series_ids: List[str], *, start_date: str, end_date: str, api_key: str
) -> Dict[str, pd.DataFrame]:
    """
    Fetch multiple FRED series into {series_id: DataFrame(['date','value'])}.
    """
    out: Dict[str, pd.DataFrame] = {}
    for sid in series_ids:
        out[sid] = _fred_get_observations(
            sid, api_key=api_key, start_date=start_date, end_date=end_date
        )
    return out


def fred_to_long_with_aliases(
    series_by_sid: Dict[str, pd.DataFrame],
    aliases_by_sid: Mapping[str, List[str]],
) -> pd.DataFrame:
    """
    Expand FRED series dict into long rows with alias fan‑out.
    Output columns: date (UTC), alias, value (float)
    """
    recs = []
    for sid, df in series_by_sid.items():
        target_aliases = aliases_by_sid.get(sid, [])
        if not target_aliases or df is None or df.empty:
            continue
        for _, row in df.iterrows():
            v = row.get("value")
            if pd.isna(v):
                continue
            ts = pd.Timestamp(row["date"]).tz_localize("UTC")
            for alias in target_aliases:
                recs.append({"date": ts, "alias": alias, "value": float(v)})
    return pd.DataFrame.from_records(recs)




def _update_fred_fixings(
    *,
    update_statistics,
    unique_identifier: str,
    id_map: Mapping[str, str],
) -> pd.DataFrame:
    """
    Generic FRED fixing updater.

    Returns
    -------
    pd.DataFrame
        MultiIndex (time_index, unique_identifier) with a single 'rate' column (decimal).
    """
    # 0) Validate + token
    assert unique_identifier in id_map, f"Invalid unique identifier for {unique_identifier}"
    api_key = os.getenv("FRED_API_KEY")
    if not api_key:
        raise RuntimeError("FRED_API_KEY environment variable is required for FRED access.")


    value_to_rate = lambda s: s / 100.0  # FRED rates are typically percent → decimal

    # 1) Update window: from last+1d to “yesterday 00:00 UTC”
    yday = (
        dt.datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
        - dt.timedelta(days=1)
    )
    start_dt = (
        update_statistics.asset_time_statistics[unique_identifier] + dt.timedelta(days=1)
    ).astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    if start_dt > yday:
        return pd.DataFrame()

    start_date = start_dt.date().isoformat()
    end_date = yday.date().isoformat()

    # 2) Alias expansion (supports a SID mapping to multiple aliases)
    aliases_by_sid: Dict[str, List[str]] = {}
    for alias, sid in id_map.items():
        aliases_by_sid.setdefault(sid, []).append(alias)
    sids = list(aliases_by_sid.keys())

    # 3) Pull + normalize long
    raw_map = fetch_fred_series_batched(sids, start_date=start_date, end_date=end_date, api_key=api_key)
    long_df = fred_to_long_with_aliases(raw_map, aliases_by_sid)  # columns: date(UTC), alias, value
    if long_df.empty:
        return pd.DataFrame()

    # 4) MultiIndex + decimal 'rate'
    long_df = long_df.rename(columns={"date": "time_index", "alias": "unique_identifier"})
    long_df["rate"] = value_to_rate(long_df["value"])

    out = (
        long_df[["time_index", "unique_identifier", "rate"]]
        .set_index(["time_index", "unique_identifier"])
        .sort_index()
    )
    return out



