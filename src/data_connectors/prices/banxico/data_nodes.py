from __future__ import annotations

import pytz

import datetime as dt

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

    # BONDES_182_TENORS: Tuple[str, ...] = tuple(BONDES_182_SERIES.keys())  # ("182d",)
    BONDES_D_TENORS: Tuple[str, ...] = tuple(BONDES_D_SERIES.keys())  # ("1y","2y","3y","5y")
    BONDES_F_TENORS: Tuple[str, ...] = tuple(BONDES_F_SERIES.keys())  # ("1y","2y","3y","5y","7y")
    BONDES_G_TENORS: Tuple[str, ...] = tuple(BONDES_G_SERIES.keys())  # ("2y","4y","6y","8y","10y")

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

        bondes_d_tickers = [f"BONDES_D_{t}_OTR" for t in self.BONDES_D_TENORS]
        bondes_f_tickers = [f"BONDES_F_{t}_OTR" for t in self.BONDES_F_TENORS]
        bondes_g_tickers = [f"BONDES_G_{t}_OTR" for t in self.BONDES_G_TENORS]

        wanted = (
                cetes_tickers
                + bonos_tickers
                +  bondes_d_tickers + bondes_f_tickers + bondes_g_tickers
                + [BANXICO_TARGET_RATE]
        )

        assets_payload=[]
        for identifier in wanted:
            snapshot = {
                "name": identifier,
                "ticker": identifier,
                "exchange_code": "MEXICO",
            }
            payload_item = {
                "unique_identifier": identifier,
                "snapshot": snapshot,
                "security_market_sector" : msc.MARKETS_CONSTANTS.FIGI_MARKET_SECTOR_GOVT,
            "security_type" : msc.MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_DOMESTIC,
            "security_type_2" : msc.MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_2_GOVT,
            }
            assets_payload.append(payload_item)


        try:
            assets = msc.Asset.batch_get_or_register_custom_assets(assets_payload)

        except Exception as e:
            self.logger.error(f"Failed to process asset batch: {e}")
            raise




        return assets

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
            identifier=ON_THE_RUN_DATA_NODE_TABLE_NAME,
            data_frequency_id=msc.DataFrequency.one_d,
            description=(
                "On-the-run CETES, BONOS & BONDES (182/D/F/G) daily time series from Banxico SIE with columns: "
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

        def add_family(series_map: Dict[str, Dict[str, str]]):
            for _tenor, m in series_map.items():
                for sk, sid in m.items():
                    en = self.SPANISH_TO_EN.get(sk)
                    if en and sid:
                        metric_by_sid[sid] = en

        add_family(CETES_SERIES)
        add_family(BONOS_SERIES)
        add_family(BONDES_D_SERIES)
        add_family(BONDES_F_SERIES)
        add_family(BONDES_G_SERIES)

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

        def pivot_family(family_name: str, series_map: Dict[str, Dict[str, str]]):
            for tenor, mapping in series_map.items():
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
                frames[(family_name, f"{tenor}_OTR")] = wide[list(self.TARGET_COLS)]
        # CETES / BONOS
        pivot_family("Cetes", CETES_SERIES)
        pivot_family("Bonos", BONOS_SERIES)

        # NEW: BONDES (182 / D / F / G)
        pivot_family("Bondes_D", BONDES_D_SERIES)
        pivot_family("Bondes_F", BONDES_F_SERIES)
        pivot_family("Bondes_G", BONDES_G_SERIES)

        if not frames:
            return pd.DataFrame()

        # --- 5) Attach each asset to its (family, tenor) frame
        out_parts: List[pd.DataFrame] = []
        assets = us.asset_list or self.get_asset_list()
        for a in assets:
            tkr = (a.ticker or "")

            if tkr.startswith("MCET_"):
                family, tenor = "Cetes", tkr.split("MCET_", 1)[1]
                sec_type = "zero_coupon"

            elif tkr.startswith("MBONO_"):
                family, tenor = "Bonos", tkr.split("MBONO_", 1)[1]
                sec_type = "fixed_bond"

            #  Bondes families

            elif tkr.startswith("BONDES_D_"):
                family, tenor = "Bondes_D", tkr.split("BONDES_D_", 1)[1]
                sec_type = "floating_bondes_d"

            elif tkr.startswith("BONDES_F_"):
                family, tenor = "Bondes_F", tkr.split("BONDES_F_", 1)[1]
                sec_type = "floating_bondes_f"

            elif tkr.startswith("BONDES_G_"):
                family, tenor = "Bondes_G", tkr.split("BONDES_G_", 1)[1]
                sec_type = "floating_bondes_g"

            else:
                continue

            key = (family, tenor)
            if key not in frames:
                continue

            df = frames[key].copy()
            uid = getattr(a, "unique_identifier", None) or tkr
            df["unique_identifier"] = uid
            df["type"] = sec_type
            out_parts.append(df.set_index("unique_identifier", append=True))

        if not out_parts:
            return pd.DataFrame()


        #get Funding rate


        raw = fetch_banxico_series_batched([MONEY_MARKET_RATES[BANXICO_TARGET_RATE]], start_date=start_date, end_date=end_date, token=token)
        long_df = to_long(raw, {BANXICO_TARGET_RATE:"dato"})  # produces columns: date, series_id, metric(EN), value
        long_df["days_to_maturity"]=1
        long_df["type"]="overnight_rate"
        long_df["date"]=pd.to_datetime(long_df["date"],utc=True)
        long_df=long_df.rename(columns={"date":"time_index","value":"dirty_price"})
        inverse={v:k for k,v in MONEY_MARKET_RATES.items()}
        long_df["unique_identifier"]=long_df["series_id"].map(inverse)
        long_df=long_df.set_index(["time_index","unique_identifier"])
        long_df["clean_price"]=None
        long_df["dirty_price"] = long_df["dirty_price"]/100
        long_df["current_coupon"] = None
        out_parts.append(long_df[out_parts[0].columns])
        out = pd.concat(out_parts).sort_index()
        out = out.replace(np.nan, None)

        out=out[out.days_to_maturity>0] #some banxico series have errors

        return out