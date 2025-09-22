
from dotenv import load_dotenv
from pathlib import Path
import numpy as np

# Load environment variables from .env.example
project_root = Path(__file__).resolve().parents[1]
env_file_path = project_root / ".env"

load_dotenv(dotenv_path=env_file_path)
from mainsequence.tdag.data_nodes import DataNode,  APIDataNode
import mainsequence.client as ms_client
class TestTimeSerie(DataNode):
    def __init__(self, data_source_id: str, local_hash_id: str, *args, **kwargs):
        self.bars = APITimeSerie(local_hash_id=local_hash_id, data_source_id=data_source_id)
        super().__init__(*args, **kwargs)

    def update(self, latest_value, *args, **kwargs):
        full_data = self.bars.get_df_between_dates(
            start_date=latest_value,
            end_date=None,
            great_or_equal=False,
        )
        return full_data

def test_valmer():
    from data_connectors.prices.valmer.time_series import ImportValmer
    BUCKET_NAME = "Vector de precios"


    for i in range(360//5):
        ts_all_files = ImportValmer(
            bucket_name=BUCKET_NAME,
        )

        ts_all_files.run(
            debug_mode=True,
            force_update=True,
        )

def test_floating_portfolio_valmer():
    import mainsequence.client as msc
    import re
    import pandas as pd
    from mainsequence.virtualfundbuilder.data_nodes import PortfolioFromDF, All_PORTFOLIO_COLUMNS, WEIGHTS_TO_PORTFOLIO_COLUMNS
    from mainsequence.virtualfundbuilder.portfolio_interface import PortfolioInterface

    import json


    class TestPortfolio(PortfolioFromDF):
        def get_portfolio_df(self):
            BUCKET_NAME = "Vector de precios"
            artifacts = msc.Artifact.filter(bucket__name=BUCKET_NAME)
            sorted_artifacts = sorted(artifacts, key=lambda artifact: artifact.name)
            # --- Conditional processing based on process_all_files flag ---
            df = pd.DataFrame()
            for artifact in sorted_artifacts:
                match = re.search(r'(\d{4}-\d{2}-\d{2})', artifact.name)
                if match:
                    df = pd.read_excel(artifact.content, engine="xlrd")
                    continue
            if df.empty:
                return pd.DataFrame()
            import numpy as np
            random_assets=np.random.randint(100,size=10)

            df_tiie = df[df["SUBYACENTE"].astype(str).str.contains("TIIE", na=False)].iloc[random_assets]
            df_cete=df[df["SUBYACENTE"].astype(str).str.contains("CETE", na=False)].iloc[:10]
            df=pd.concat([df_tiie, df_cete],axis=0)

            df = df
            df["FECHA"]=pd.to_datetime(df["FECHA"], format='%Y%m%d', utc=True)
            df["unique_identifier"] = (
                df["TIPO VALOR"].astype("string")
                .str.cat(df["EMISORA"].astype("string"), sep="_")
                .str.cat(df["SERIE"].astype("string"), sep="_")
            )

            unique_identifiers = df['unique_identifier'].unique().tolist()
            existing_assets = msc.Asset.query(unique_identifier__in=unique_identifiers, per_page=1000)
            existing_assets = [a for a in existing_assets if a.current_pricing_detail is not None]
            if len(existing_assets) == 0:
                return pd.DataFrame()

            # ----- build dict-valued columns -----
            keys = [a.unique_identifier for a in existing_assets]
            n = len(keys)

            # random weights that sum to 1
            import numpy as np
            w = np.random.rand(n)
            w = w / w.sum()
            weights_dict = json.dumps({k: float(v) for k, v in zip(keys, w)})

            # everything else set to 1 per asset
            ones_dict = json.dumps({k: 1 for k in keys})

            time_idx = df["FECHA"].iloc[0]

            # Map logical fields to actual DF columns
            col_weights_current = "rebalance_weights" # "weights_current"
            col_price_current ="rebalance_price"  # "price_current"
            col_vol_current ="volume"  # "volume_current"
            col_weights_before = "weights_at_last_rebalance" # "weights_before"
            col_price_before ="price_at_last_rebalance" # "price_before"
            col_vol_before ="volume_at_last_rebalance"  # "volume_before"

            row = {
                "time_index": time_idx,
                "close": 1,
                "return": 0,
                "last_rebalance_date": time_idx.timestamp(),
                col_weights_current: weights_dict,
                col_weights_before: weights_dict,  # same as current
                col_price_current: ones_dict,
                col_price_before: ones_dict,
                col_vol_current: ones_dict,
                col_vol_before: ones_dict,
            }

            # one-row DataFrame
            portoflio_df = pd.DataFrame([row])
            portoflio_df=portoflio_df.set_index("time_index")
            if self.update_statistics.max_time_index_value is not None:
                portoflio_df=portoflio_df[portoflio_df.index>self.update_statistics.max_time_index_value]
            return portoflio_df

    node=TestPortfolio(portfolio_name="TestPortfolio",calendar_name="24/7",target_portfolio_about="Test with Vector")

    PortfolioInterface.build_and_run_portfolio_from_df(portfolio_node=node,
                                                       add_portfolio_to_markets_backend=True)

    node = TestPortfolio(portfolio_name="TestPortfolio2", calendar_name="24/7",
                         target_portfolio_about="Test with Vector2")

    PortfolioInterface.build_and_run_portfolio_from_df(portfolio_node=node,
                                                       add_portfolio_to_markets_backend=True)





def test_discount_curves():
    from data_connectors.interest_rates.nodes import (DiscountCurves,CurveConfig,
                                                      TIIE_28_ZERO_CURVE,)
    from data_connectors.prices.banxico import (ON_THE_RUN_DATA_NODE_UID,
                                                      M_BONOS_ZERO_OTR_CURVE_UID)
    # config=CurveConfig(unique_identifier=TIIE_28_ZERO_CURVE,
    #                    name="Discount Curve TIIE 28 Mexder Valmer",
    #                    )
    # node=DiscountCurves(curve_config=config)
    # node.run(debug_mode=True,force_update=True)

    config = CurveConfig(unique_identifier=M_BONOS_ZERO_OTR_CURVE_UID,
                         name="Discount Curve M Bonos Banxico Boostrapped",
    curve_points_dependecy_data_node_uid=ON_THE_RUN_DATA_NODE_UID
                         )
    node = DiscountCurves(curve_config=config)
    node.run(debug_mode=True, force_update=True)


def test_binance_bars_from_trades(bar_type="time"):
    from data_connectors.prices.binance.time_series import BinanceBarsFromTrades,TimeBarConfig,ImbalanceBarConfig
    from mainsequence.client import AssetFutureUSDM, AssetCurrencyPair

    future_assets = AssetFutureUSDM.filter(ticker__in=["BTC-USDT", "ETH-USDT", "1000SHIB-USDT"])
    spot_assets = AssetCurrencyPair.filter(ticker__in=["BTC-USDT", "ETH-USDT", "SHIB-USDT"])
    if bar_type== "time":
        bar_configuration=TimeBarConfig(frequency_id="1m")
    else:
        bar_configuration=ImbalanceBarConfig()
    ts = BinanceBarsFromTrades(asset_list=future_assets,bar_configuration=bar_configuration)
    ts.run(debug_mode=True,force_update=True)

def test_binance_daily_bars():
    from data_connectors.prices.binance.time_series import BinanceHistoricalBars,TimeBarConfig
    from mainsequence.client import AssetFutureUSDM, AssetCurrencyPair
    future_assets = AssetFutureUSDM.filter(ticker__in=["BTC-USDT", "ETHU-SDT", "1000SHIB-USDT"])
    spot_assets = AssetCurrencyPair.filter(ticker__in=["BTC-USDT", "ETH-USDT", "SHIB-USDT"])

    # ts = BinanceHistoricalBars(asset_list=spot_assets,  bar_configuration=TimeBarConfig(frequency_id="1d"))
    # ts.run(debug_mode=True,force_update=True)



    ts = BinanceHistoricalBars(asset_list=None,asset_category_identifier="top_10_crypto_market_cap",  bar_configuration=TimeBarConfig(frequency_id="1d"))
    ts.run(debug_mode=True,force_update=True)


def test_alpaca_bars():
    from data_connectors.prices.alpaca.time_series import AlpacaEquityBars
    from data_connectors.helpers import update_calendar_holes
    from mainsequence.client import  Asset
    ts = AlpacaEquityBars( asset_list=None,frequency_id="1d", adjustment="all")
    update_calendar_holes(data_node=ts,start_date=ts.OFFSET_START,frequency="1d")
    ts.run(debug_mode=True,

           force_update=True)

def test_alpaca_bars_small():
    from data_connectors.prices.alpaca.time_series import AlpacaEquityBars
    from mainsequence.client import AssetCategory
    asset_cat = AssetCategory.get(unique_identifier="magnificent_7")
    assets = asset_cat.get_assets()[:2]
    ts = AlpacaEquityBars(asset_list=assets, frequency_id="1d", adjustment="all")
    ts.run(debug_mode=True, force_update=True)

def test_databento_bars():
    from data_connectors.prices.databento.time_series import DatabentoHistoricalBars
    ts = DatabentoHistoricalBars(
        asset_list=None,
        frequency_id="1d",
        dataset="XNAS.ITCH"
    )
    ts.run(debug_mode=True, force_update=True)


def test_databento_bars_small():
    from data_connectors.prices.databento.time_series import DatabentoHistoricalBars
    from mainsequence.client import AssetCategory
    asset_cat = AssetCategory.get(unique_identifier="magnificent_7")
    assets = asset_cat.get_assets()  # [:2]
    ts = DatabentoHistoricalBars(
        asset_list=assets,
        frequency_id="1d",
        dataset="XNAS.ITCH"
    )
    ts.run(debug_mode=True, force_update=True)

def test_databento_market_cap_small():
    from data_connectors.fundamentals.databento.market_cap import DatabentoMarketCap
    from mainsequence.client import AssetCategory

    asset_cat = AssetCategory.get(unique_identifier="magnificent_7")
    assets = asset_cat.get_assets()

    dataset = "XNAS.ITCH"
    prices_ts_identifier = f"databento_xnas_itch_1d_bars"

    ts = DatabentoMarketCap(
        asset_list=assets,
        prices_time_serie_unique_identifier=prices_ts_identifier,
        dataset=dataset
    )
    ts.run(debug_mode=True, force_update=True)

def test_api_time_series():
    from data_connectors.prices.binance.time_series import BinanceBarsFromTrades
    from mainsequence.client import AssetFutureUSDM, AssetCurrencyPair

    future_assets = AssetFutureUSDM.filter(ticker__in=["BTC-USDT", "ETH-USDT", "1000SHIB-USDT"])
    spot_assets = AssetCurrencyPair.filter(ticker__in=["BTC-USDT", "ETH-USDT", "SHIB-USDT"])

    ts = BinanceBarsFromTrades(asset_list=[spot_assets[0]] + [future_assets[0]])

    new_ts = TestTimeSerie(local_hash_id=ts.local_hash_id,data_source_id=ts.metadata["data_source"]["id"])
    new_ts.run_in_debug_scheduler()

def test_equity_market_cap():
    from data_connectors.fundamentals.polygon.equity_fundamentals import PolygonDailyMarketCap

    ts = PolygonDailyMarketCap(asset_list=None)
    ts.run(debug_mode=True, force_update=True)

def test_crypto_market_cap():
    from data_connectors.fundamentals.coingecko.crypto_fundamentals import CoinGeckoMarketCap
    from mainsequence.client import AssetFutureUSDM, AssetCurrencyPair

    spot_assets = AssetCurrencyPair.filter(ticker__in=["BTC-USDT", "ETH-USDT", "SHIB-USDT"])
    spot_assets=[a.base_asset for a in spot_assets]
    ts = CoinGeckoMarketCap(asset_list=spot_assets)
    ts.run(debug_mode=True, force_update=True)

def test_equity_fundamentals():
    from data_connectors.fundamentals.polygon.equity_fundamentals import PolygonQFundamentals
    asset_list=ms_client.Asset.filter(ticker__in=["AIG","OXY"])
    ts = PolygonQFundamentals(asset_list=asset_list)
    ts.run(debug_mode=True, force_update=True)

def test_banxico_mbonos():
    from data_connectors.prices.banxico.data_nodes import BanxicoMXNOTR
    import os

    ts=BanxicoMXNOTR()
    ts.run(debug_mode=True, force_update=True)

def test_banxico_tiie():
    from data_connectors.interest_rates.nodes import FixingRatesNode,FixingRateConfig,RateConfig
    from data_connectors.prices.banxico.settings import (TIIE_OVERNIGHT_UID,
                                                         TIIE_28_UID,
                                                         TIIE_91_UID,
                                                         TIIE_182_UID, )

    fixing_config = FixingRateConfig(rates_config_list=[
        # RateConfig(unique_identifier=TIIE_OVERNIGHT_UID,
        #            name=f"Interbank Equilibrium Interest Rate (TIIE) {TIIE_OVERNIGHT_UID}"),
        # RateConfig(unique_identifier=TIIE_28_UID, name=f"Interbank Equilibrium Interest Rate (TIIE) {TIIE_28_UID}"),
        # RateConfig(unique_identifier=TIIE_91_UID, name=f"Interbank Equilibrium Interest Rate (TIIE) {TIIE_91_UID}"),
        # RateConfig(unique_identifier=TIIE_182_UID, name=f"Interbank Equilibrium Interest Rate (TIIE) {TIIE_182_UID}"),
        RateConfig(unique_identifier=CETE_28, name=f"CETE 28 days {CETE_28}"),

    ]

    )

    ts=FixingRatesNode(rates_config=fixing_config)
    ts.run(debug_mode=True, force_update=True)

# test_api_time_series()
# test_binance_bars_from_trades()
# test_crypto_market_cap()
# test_equity_market_cap()
# test_binance_daily_bars()
# test_alpaca_bars()
# test_valmer()

# test_alpaca_bars_small()
# test_banxico_mbonos()

# test_databento_bars_small()
# test_databento_market_cap_small()
# test_databento_bars()
# test_equity_fundamentals()


# test_banxico_tiie()
# test_banxico_mbonos()
# test_discount_curves()
test_floating_portfolio_valmer()
