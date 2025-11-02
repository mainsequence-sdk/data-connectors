
import datetime
from typing import Dict, Tuple, Optional, Union, List
import pandas as pd

import requests

import time




# ======================
# 2) Fetch Helpers
# ======================

BANXICO_SIE_BASE = "https://www.banxico.org.mx/SieAPIRest/service/v1"

def _coerce_float(value: str) -> Optional[float]:
    if value is None:
        return None
    v = str(value).strip().replace(",", "")
    if not v or v.lower() in {"n.d.", "na", "nan", "null"}:
        return None
    try:
        return float(v)
    except ValueError:
        return None

def _iso(d: Union[datetime.date, str]) -> str:
    return d if isinstance(d, str) else d.isoformat()


def fetch_banxico_series_detail(series_ids: Tuple[str, ...], token) -> list:
    if not series_ids:
        return []
    url = (
        f"{BANXICO_SIE_BASE}/series/{','.join(series_ids)}/"
        f"?token={token}"
    )
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    return payload.get("bmx", {}).get("series", [])
def fetch_banxico_series(series_ids: Tuple[str, ...], start_date: str, end_date: str, token: str) -> list:
    if not series_ids:
        return []
    url = (
        f"{BANXICO_SIE_BASE}/series/{','.join(series_ids)}/datos/"
        f"{_iso(start_date)}/{_iso(end_date)}?token={token}"
    )
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    return payload.get("bmx", {}).get("series", [])

def normalize_series(raw_series: list, metric_map: Dict[str, str], tenor_map: Dict[str, str]) -> pd.DataFrame:
    rows = []
    for s in raw_series:
        sid = s.get("idSerie")
        title = s.get("titulo")
        for p in s.get("datos", []):
            rows.append({
                "date": pd.to_datetime(p.get("fecha"), errors="coerce"),
                "series_id": sid,
                "title": title,
                "value": _coerce_float(p.get("dato")),
                "metric": metric_map.get(sid),
                "tenor": tenor_map.get(sid)
            })
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(["series_id", "date"]).reset_index(drop=True)
    return df

def _coerce_float(v: str | None) -> float | None:
    if v is None:
        return None
    s = str(v).strip().replace(",", "")
    if s == "" or s.lower() in {"n.d.", "na", "nan", "null"}:
        return None
    try:
        return float(s)
    except ValueError:
        return None

def _fetch_banxico_series(
    series_ids: List[str], start_date: str, end_date: str, token: str, base_url: str = BANXICO_SIE_BASE, timeout: float = 30.0
) -> List[dict]:
    if not series_ids:
        return []
    url = f"{base_url}/series/{','.join(series_ids)}/datos/{start_date}/{end_date}?token={token}"
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    return (data.get("bmx") or {}).get("series") or []


def fetch_banxico_series_batched(
    series_ids,
    start_date: str,
    end_date: str,
    token: str,
    *,
    base_url: str = BANXICO_SIE_BASE,
    max_chunk: int = 10,
    timeout: float = 30.0,
    pause_seconds: float = 0.0,
):
    """
    Fetch SIE 'series' in chunks to avoid 413 (URL too long).
    Automatically halves the chunk size and retries if a 413 occurs.
    Returns a single combined list of the 'series' objects.
    """
    # de-dup while preserving order
    uniq_ids = list(dict.fromkeys(series_ids))
    out = []
    i = 0
    chunk = max(1, int(max_chunk))

    while i < len(uniq_ids):
        ids_slice = uniq_ids[i : i + chunk]
        try:
            part = _fetch_banxico_series(
                series_ids=ids_slice,
                start_date=start_date,
                end_date=end_date,
                token=token,
                base_url=base_url,
                timeout=timeout,
            )
            out.extend(part)
            i += chunk
            if pause_seconds:
                time.sleep(pause_seconds)
        except requests.HTTPError as e:
            # If it's 413, shrink chunk and retry same window
            if getattr(e, "response", None) is not None and e.response.status_code == 413 and chunk > 1:
                chunk = max(1, chunk // 2)
                continue
            raise
    return out

def to_long(raw_series: List[dict], metric_by_sid: Dict[str, str]) -> pd.DataFrame:
    # keep only entries that actually have datos
    items = [s for s in raw_series if s and s.get("datos")]
    if not items:
        return pd.DataFrame(columns=["date", "series_id", "metric", "value"])

    df = pd.json_normalize(items, record_path="datos", meta=["idSerie"])
    # rename + add mapped metric
    df = df.rename(columns={"fecha": "date", "dato": "value", "idSerie": "series_id"})
    df["metric"] = df["series_id"].map(metric_by_sid)

    # vectorized parsing (avoid per-row apply)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")  # add format=... if you know it for extra speed
    # fast-ish number parse incl. comma decimals; tweak if your values are already numeric
    df["value"] = pd.to_numeric(
        df["value"].astype(str).str.replace(",", "", regex=False),
        errors="coerce"
    )

    df = df[["date", "series_id", "metric", "value"]]
    df = df.dropna(subset=["date"]).sort_values(["metric", "date"], kind="stable").reset_index(drop=True)
    return df


def to_long_with_aliases(raw_series: List[dict], aliases_by_sid: Dict[str, List[str]]) -> pd.DataFrame:
    """
    Normalize Banxico response to long rows and expand series_id → aliases.
    Output columns: date (UTC), alias, value
    """
    items = [s for s in raw_series if s and s.get("datos")]
    if not items:
        return pd.DataFrame(columns=["date", "alias", "value"])

    df = pd.json_normalize(items, record_path="datos", meta=["idSerie"])
    df = df.rename(columns={"fecha": "date", "dato": "value", "idSerie": "series_id"})

    # Map series to aliases; explode to one row per alias
    df["aliases"] = df["series_id"].map(lambda sid: aliases_by_sid.get(sid, []))
    df = df[df["aliases"].map(bool)].explode("aliases").rename(columns={"aliases": "alias"})

    # --- CRITICAL: DD/MM/YYYY parse with dayfirst + UTC ---
    df["date"] = pd.to_datetime(
        df["date"].astype(str).str.strip(),
        format="%d/%m/%Y",  # be explicit; faster + avoids ambiguity
        errors="coerce",
        utc=True,
    )

    # Numeric parse (strip thousands commas if any)
    df["value"] = pd.to_numeric(df["value"].astype(str).str.replace(",", "", regex=False), errors="coerce")

    df = df.dropna(subset=["date"]).sort_values(["alias", "date"], kind="stable").reset_index(drop=True)
    return df[["date", "alias", "value"]]


