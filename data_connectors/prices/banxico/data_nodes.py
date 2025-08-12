from __future__ import annotations

import datetime
import pytz
from typing import Dict, Tuple, Optional, Union, List
from urllib.parse import quote
import pandas as pd
import requests
from mainsequence.client import MARKETS_CONSTANTS
from pydantic import BaseModel, Field
import datetime as dt

import mainsequence.client as msc
from mainsequence.tdag import DataNode
from mainsequence.client.models_tdag import UpdateStatistics, ColumnMetaData
import numpy as np
import os
UTC = pytz.utc

# -----------------------------
# Banxico series catalogs
# -----------------------------
BANXICO_SIE_BASE = "https://www.banxico.org.mx/SieAPIRest/service/v1"
CETES_SERIES: Dict[str, Dict[str, str]] = {
    # CETES (no coupon series in Banxico vector)

    "28d":  {"plazo": "SF45422", "precio_limpio": "SF45438", "precio_sucio": "SF45439"},
    "91d":  {"plazo": "SF45423", "precio_limpio": "SF45440", "precio_sucio": "SF45441"},
    "182d": {"plazo": "SF45424", "precio_limpio": "SF45442", "precio_sucio": "SF45443"},
    "364d": {"plazo": "SF45425", "precio_limpio": "SF45444", "precio_sucio": "SF45445"},
    # keep if you also track this bucket
    "2y":   {"plazo": "SF349886", "precio_limpio": "SF349887", "precio_sucio": "SF349888"},
}

BONOS_SERIES: Dict[str, Dict[str, str]] = {
    # BONOS M (Mbonos)
    "0-3y":   {"plazo": "SF45427", "precio_limpio": "SF45448", "precio_sucio": "SF45449", "cupon_vigente": "SF45475"},
    "3-5y":   {"plazo": "SF45428", "precio_limpio": "SF45450", "precio_sucio": "SF45451", "cupon_vigente": "SF45476"},
    "5-7y":   {"plazo": "SF45429", "precio_limpio": "SF45452", "precio_sucio": "SF45453", "cupon_vigente": "SF45477"},
    "7-10y":  {"plazo": "SF45430", "precio_limpio": "SF45454", "precio_sucio": "SF45455", "cupon_vigente": "SF45478"},
    "10-20y": {"plazo": "SF45431", "precio_limpio": "SF45456", "precio_sucio": "SF45457", "cupon_vigente": "SF45479"},
    "20-30y": {"plazo": "SF60720", "precio_limpio": "SF60721", "precio_sucio": "SF60722", "cupon_vigente": "SF60723"},
}

FUNDING_RATES={"1d":"SF331451"}

# -----------------------------
# HTTP helpers
# -----------------------------
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


def _fetch_banxico_series_batched(
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

def _to_long(raw_series: List[dict], metric_by_sid: Dict[str, str]) -> pd.DataFrame:
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


class BanxicoMXNOTR(DataNode):
    """
    Pulls historical price & yield for Mexican Mbonos from the Banxico SIE API.

    Output:
        MultiIndex: (time_index [UTC], unique_identifier [series_id])
        Columns: 'title', 'value', 'metric', 'tenor'
    """

    CETES_TENORS: Tuple[str, ...] = tuple(CETES_SERIES.keys())
    BONOS_TENORS: Tuple[str, ...] = tuple(BONOS_SERIES.keys())

    SPANISH_TO_EN = {
        "plazo": "days_to_maturity",
        "precio_limpio": "clean_price",
        "precio_sucio": "dirty_price",
        "cupon_vigente": "current_coupon",
    }
    TARGET_COLS = ("days_to_maturity", "clean_price", "dirty_price", "current_coupon")


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def dependencies(self) -> Dict[str, Union["DataNode", "APIDataNode"]]:
        return {}


    def get_asset_list(self) -> List[msc.Asset]:
        # Discover both families via ticker pattern:contentReference[oaicite:3]{index=3}
        cetes_tickers = [f"MCET_{t}_OTR" for t in self.CETES_TENORS]
        bonos_tickers = [f"MBONO_{t}_OTR" for t in self.BONOS_TENORS]
        wanted = cetes_tickers + bonos_tickers
        assets = msc.Asset.filter(ticker__in=wanted,
                                        security_market_sector=msc.MARKETS_CONSTANTS.FIGI_MARKET_SECTOR_GOVT,
                                        security_type=msc.MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_DOMESTIC,
                                        security_type_2=msc.MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_2_GOVT,
                                        exchange_code="MEXICO",
                                        )
        return list(assets) if assets else []

    def get_column_metadata(self) -> List[ColumnMetaData]:
        return [
            ColumnMetaData(
                column_name="days_to_maturity", dtype="float", label="Days to Maturity",
                description="Number of days until maturity (Banxico vector)."
            ),
            ColumnMetaData(
                column_name="clean_price", dtype="float", label="Clean Price",
                description="Price excluding accrued interest."
            ),
            ColumnMetaData(
                column_name="dirty_price", dtype="float", label="Dirty Price",
                description="Price including accrued interest."
            ),
            ColumnMetaData(
                column_name="current_coupon", dtype="float", label="Current Coupon",
                description="Current coupon rate (mainly for Bonos)."
            ),
        ]
    def get_table_metadata(self) -> Optional[msc.TableMetaData]:
        # Identifier + daily frequency + a concise description
        return msc.TableMetaData(
            identifier="banxico_1d_otr_mxn",
            data_frequency_id=msc.DataFrequency.one_d,
            description=(
                "On-the-run CETES & BONOS (MXN) daily time series from Banxico SIE with columns: "
                "days_to_maturity, clean_price, dirty_price, current_coupon."
            ),
        )

    def update(self) -> pd.DataFrame:
        us: UpdateStatistics = self.update_statistics

        # --- 0) Token from env (no params by design)
        token = os.getenv("BANXICO_TOKEN")
        if not token:
            raise RuntimeError("BANXICO_TOKEN environment variable is required for Banxico SIE access.")

        # --- 1) Compute update window (yesterday 00:00 UTC end). Start = min(last+1d)
        yday = dt.datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0) - dt.timedelta(days=1)
        starts: List[dt.datetime] = []
        for asset in us.asset_list:
            last = us.get_last_update_index_2d(uid=asset.unique_identifier)
            if last is not None:
                starts.append((last + dt.timedelta(days=1)).astimezone(UTC).replace(
                    hour=0, minute=0, second=0, microsecond=0
                ))
        start_dt = min(starts) if starts else dt.datetime(2010, 1, 1, tzinfo=UTC)
        if start_dt > yday:
            return pd.DataFrame()

        start_date = start_dt.date().isoformat()
        end_date = yday.date().isoformat()

        # --- 2) Build the series universe: CETES + BONOS (map to EN metric names)
        metric_by_sid: Dict[str, str] = {}

        # CETES: include 3 core metrics; add current_coupon if available in mapping
        for t, m in CETES_SERIES.items():
            for sk in ("plazo", "precio_limpio", "precio_sucio", "cupon_vigente"):
                sid = m.get(sk)
                if sid:
                    metric_by_sid[sid] = self.SPANISH_TO_EN[sk]

        # BONOS: all four metrics
        for t, m in BONOS_SERIES.items():
            for sk, sid in m.items():
                en = self.SPANISH_TO_EN.get(sk)
                if en:
                    metric_by_sid[sid] = en

        all_sids = list(metric_by_sid.keys())
        if not all_sids:
            return pd.DataFrame()

        # --- 3) Pull once from Banxico + normalize (metric column already in EN via metric_by_sid)
        raw = _fetch_banxico_series_batched(all_sids, start_date=start_date, end_date=end_date, token=token)
        long_df = _to_long(raw, metric_by_sid)  # produces columns: date, series_id, metric(EN), value
        if long_df.empty:
            return pd.DataFrame()

        # --- 4) Prepare pivoted frames per (family, tenor), columns in EN
        frames: Dict[tuple, pd.DataFrame] = {}

        # CETES pivots
        for tenor, mapping in CETES_SERIES.items():
            # include coupon if present
            sids = {mapping.get(k) for k in ("plazo", "precio_limpio", "precio_sucio", "cupon_vigente") if
                    mapping.get(k)}
            sub = long_df[long_df["series_id"].isin(sids)]
            if sub.empty:
                continue
            wide = (
                sub.pivot_table(index="date", columns="metric", values="value", aggfunc="last")
                .rename_axis(None, axis="columns")
            )
            # ensure all target cols exist (EN)
            for col in self.TARGET_COLS:
                if col not in wide.columns:
                    wide[col] = pd.NA
            wide.index = pd.to_datetime(wide.index, utc=True)
            wide.index.name = "time_index"
            frames[("Cetes", f"{tenor}_OTR")] = wide[list(self.TARGET_COLS)]

        # BONOS pivots
        for tenor, mapping in BONOS_SERIES.items():
            sids = set(mapping.values())
            sub = long_df[long_df["series_id"].isin(sids)]
            if sub.empty:
                continue
            wide = (
                sub.pivot_table(index="date", columns="metric", values="value", aggfunc="last")
                .rename_axis(None, axis="columns")
            )
            for col in self.TARGET_COLS:
                if col not in wide.columns:
                    wide[col] = pd.NA
            wide.index = pd.to_datetime(wide.index, utc=True)
            wide.index.name = "time_index"
            frames[("Bonos", f"{tenor}_OTR")] = wide[list(self.TARGET_COLS)]

        if not frames:
            return pd.DataFrame()

        # --- 5) Attach each asset to its (family, tenor) frame
        out_parts: List[pd.DataFrame] = []
        assets = us.asset_list or self.get_asset_list()
        for a in assets:
            tkr = (a.ticker or "")
            if tkr.startswith("MCET_"):
                family, tenor = "Cetes", tkr.split("MCET_", 1)[1]
                type="zero_coupon"
            elif tkr.startswith("MBONO_"):
                family, tenor = "Bonos", tkr.split("MBONO_", 1)[1]
                type = "fixed_bond"
            else:
                continue

            key = (family, tenor)
            if key not in frames:
                continue

            df = frames[key].copy()
            uid = getattr(a, "unique_identifier", None) or tkr
            df["unique_identifier"] = uid
            df["type"]=type
            out_parts.append(df.set_index("unique_identifier", append=True))

        if not out_parts:
            return pd.DataFrame()

        out = pd.concat(out_parts).sort_index()
        out = out.replace(np.nan, None)
        return out