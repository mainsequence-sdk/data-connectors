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
from .settings import *
from .utils import *
import numpy as np
import os
import time
UTC = pytz.utc


# -----------------------------
# HTTP helpers
# -----------------------------






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
        raw = fetch_banxico_series_batched(all_sids, start_date=start_date, end_date=end_date, token=token)
        long_df = to_long(raw, metric_by_sid)  # produces columns: date, series_id, metric(EN), value
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