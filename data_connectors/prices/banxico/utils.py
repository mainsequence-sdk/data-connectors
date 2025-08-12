
import datetime
from typing import Dict, Tuple, Optional, Union, List
import pandas as pd
import requests







run_series = [
    # --- CETES ---
    {"family": "Cetes", "tenor": "28d",  "metric": "plazo",            "id": "SF45422"},
    {"family": "Cetes", "tenor": "28d",  "metric": "precio_limpio",    "id": "SF45438"},
    {"family": "Cetes", "tenor": "28d",  "metric": "precio_sucio",     "id": "SF45439"},
    {"family": "Cetes", "tenor": "28d",  "metric": "tasa_rendimiento", "id": "SF45470"},

    {"family": "Cetes", "tenor": "91d",  "metric": "plazo",            "id": "SF45423"},
    {"family": "Cetes", "tenor": "91d",  "metric": "precio_limpio",    "id": "SF45440"},
    {"family": "Cetes", "tenor": "91d",  "metric": "precio_sucio",     "id": "SF45441"},
    {"family": "Cetes", "tenor": "91d",  "metric": "tasa_rendimiento", "id": "SF45471"},

    {"family": "Cetes", "tenor": "182d", "metric": "plazo",            "id": "SF45424"},
    {"family": "Cetes", "tenor": "182d", "metric": "precio_limpio",    "id": "SF45442"},
    {"family": "Cetes", "tenor": "182d", "metric": "precio_sucio",     "id": "SF45443"},
    {"family": "Cetes", "tenor": "182d", "metric": "tasa_rendimiento", "id": "SF45472"},

    {"family": "Cetes", "tenor": "364d", "metric": "plazo",            "id": "SF45425"},
    {"family": "Cetes", "tenor": "364d", "metric": "precio_limpio",    "id": "SF45444"},
    {"family": "Cetes", "tenor": "364d", "metric": "precio_sucio",     "id": "SF45445"},
    {"family": "Cetes", "tenor": "364d", "metric": "tasa_rendimiento", "id": "SF45473"},

    {"family": "Cetes", "tenor": "2y",   "metric": "plazo",            "id": "SF349886"},
    {"family": "Cetes", "tenor": "2y",   "metric": "precio_limpio",    "id": "SF349887"},
    {"family": "Cetes", "tenor": "2y",   "metric": "precio_sucio",     "id": "SF349888"},
    {"family": "Cetes", "tenor": "2y",   "metric": "tasa_rendimiento", "id": "SF349889"},

    # --- MBONOS (Bonos M) ---
    {"family": "Bonos", "tenor": "0-3y",   "metric": "plazo",          "id": "SF45427"},
    {"family": "Bonos", "tenor": "0-3y",   "metric": "precio_limpio",  "id": "SF45448"},
    {"family": "Bonos", "tenor": "0-3y",   "metric": "precio_sucio",   "id": "SF45449"},
    {"family": "Bonos", "tenor": "0-3y",   "metric": "cupon_vigente",  "id": "SF45475"},

    {"family": "Bonos", "tenor": "3-5y",   "metric": "plazo",          "id": "SF45428"},
    {"family": "Bonos", "tenor": "3-5y",   "metric": "precio_limpio",  "id": "SF45450"},
    {"family": "Bonos", "tenor": "3-5y",   "metric": "precio_sucio",   "id": "SF45451"},
    {"family": "Bonos", "tenor": "3-5y",   "metric": "cupon_vigente",  "id": "SF45476"},

    {"family": "Bonos", "tenor": "5-7y",   "metric": "plazo",          "id": "SF45429"},
    {"family": "Bonos", "tenor": "5-7y",   "metric": "precio_limpio",  "id": "SF45452"},
    {"family": "Bonos", "tenor": "5-7y",   "metric": "precio_sucio",   "id": "SF45453"},
    {"family": "Bonos", "tenor": "5-7y",   "metric": "cupon_vigente",  "id": "SF45477"},

    {"family": "Bonos", "tenor": "7-10y",  "metric": "plazo",          "id": "SF45430"},
    {"family": "Bonos", "tenor": "7-10y",  "metric": "precio_limpio",  "id": "SF45454"},
    {"family": "Bonos", "tenor": "7-10y",  "metric": "precio_sucio",   "id": "SF45455"},
    {"family": "Bonos", "tenor": "7-10y",  "metric": "cupon_vigente",  "id": "SF45478"},

    {"family": "Bonos", "tenor": "10-20y", "metric": "plazo",          "id": "SF45431"},
    {"family": "Bonos", "tenor": "10-20y", "metric": "precio_limpio",  "id": "SF45456"},
    {"family": "Bonos", "tenor": "10-20y", "metric": "precio_sucio",   "id": "SF45457"},
    {"family": "Bonos", "tenor": "10-20y", "metric": "cupon_vigente",  "id": "SF45479"},

    {"family": "Bonos", "tenor": "20-30y", "metric": "plazo",          "id": "SF60720"},
    {"family": "Bonos", "tenor": "20-30y", "metric": "precio_limpio",  "id": "SF60721"},
    {"family": "Bonos", "tenor": "20-30y", "metric": "precio_sucio",   "id": "SF60722"},
    {"family": "Bonos", "tenor": "20-30y", "metric": "cupon_vigente",  "id": "SF60723"},
]


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
