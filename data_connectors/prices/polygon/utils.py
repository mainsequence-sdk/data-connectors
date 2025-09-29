# polygon/utils.py

from __future__ import annotations
import os, time, datetime as dt
from typing import Dict, List, Optional, Callable
import pandas as pd
import requests

from .settings import (
    POLYGON_API_BASE,
    CMT_OBSERVATION_TZ, CMT_OBSERVATION_HOUR, CMT_OBSERVATION_MINUTE,
)

# ------------------------------
# Polygon client (DRY; reusable)
# ------------------------------
class PolygonClient:
    def __init__(self, api_key: Optional[str] = None, base_url: str = POLYGON_API_BASE, timeout: float = 30.0):
        self.api_key = api_key or os.getenv("POLYGON_API_KEY")
        if not self.api_key:
            raise RuntimeError("POLYGON_API_KEY environment variable is required.")
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _get(self, path_or_full_url: str, params: Optional[dict] = None) -> dict:
        params = dict(params or {})
        if "apiKey" not in params:
            params["apiKey"] = self.api_key
        url = path_or_full_url if path_or_full_url.startswith("http") else f"{self.base_url}/{path_or_full_url.lstrip('/')}"
        backoff = 0.5
        for _ in range(7):
            r = requests.get(url, params=params if url.startswith("http") else None, timeout=self.timeout)
            if r.status_code == 429:
                time.sleep(backoff); backoff = min(backoff * 2, 8.0); continue
            r.raise_for_status()
            return r.json()
        raise RuntimeError("Polygon API 429: exhausted retries.")

    def get_all_pages(self, path: str, params: Optional[dict] = None) -> List[dict]:
        data = self._get(path, params)
        out = data.get("results") or []
        next_url = data.get("next_url")
        while next_url:
            data = self._get(next_url, params={})  # apiKey already in next_url
            out.extend(data.get("results") or [])
            next_url = data.get("next_url")
        return out


# ------------------------------
# Time helpers (UTC tz-aware)
# ------------------------------

def to_utc_at(day: dt.date | str, *, tz: str, hour: int, minute: int) -> pd.Timestamp:
    d = pd.to_datetime(day).date()
    local = pd.Timestamp(d.year, d.month, d.day, hour=hour, minute=minute, tz=tz)
    return local.tz_convert("UTC")

def cmt_observed_utc(day: dt.date | str) -> pd.Timestamp:
    # Treasury CMT inputs around 15:30 ET
    return to_utc_at(day, tz=CMT_OBSERVATION_TZ, hour=CMT_OBSERVATION_HOUR, minute=CMT_OBSERVATION_MINUTE)


# ------------------------------
# Normalizers
# ------------------------------

def normalize_cmt_rows(
    raw_rows: List[dict],
    field_by_tenor: Dict[str, str],
    days_by_tenor: Dict[str, int],
    observation_ts: Callable[[dt.date | str], pd.Timestamp] = cmt_observed_utc,
) -> pd.DataFrame:
    """
    Expand each daily Polygon CMT record into long rows (tz-aware UTC timestamps).
    Columns: time_index, unique_identifier, tenor, days_to_maturity, par_yield (decimal)
    """
    recs = []
    for r in raw_rows:
        day = r.get("date")  # 'YYYY-MM-DD'
        if not day:
            continue
        tstamp = observation_ts(day)
        for tenor, fld in field_by_tenor.items():
            v = r.get(fld)
            if v is None:
                continue
            try:
                y = float(str(v).replace(",", ""))
            except Exception:
                continue
            recs.append({
                "time_index": tstamp,
                "unique_identifier": f"UST_{tenor}_CMT",
                "tenor": tenor,
                "days_to_maturity": days_by_tenor.get(tenor),
                "par_yield": y / 100.0,
            })
    df = pd.DataFrame.from_records(recs)
    if not df.empty:
        df = df.sort_values(["time_index", "days_to_maturity"]).reset_index(drop=True)
    return df
