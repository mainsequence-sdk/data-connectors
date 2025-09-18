
from dotenv import load_dotenv
from pathlib import Path

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

def test_discount_curves():
    from data_connectors.interest_rates.nodes import DiscountCurves,CurveConfig,TIIE_28_ZERO_CURVE
    config=CurveConfig(unique_identifier=TIIE_28_ZERO_CURVE,
                       name="Discount Curve TIIE 28 Mexder Valmer",
                       )
    node=DiscountCurves(curve_config=config)
    node.run(debug_mode=True,force_update=True)

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
        RateConfig(unique_identifier=TIIE_OVERNIGHT_UID,
                   name=f"Interbank Equilibrium Interest Rate (TIIE) {TIIE_OVERNIGHT_UID}"),
        RateConfig(unique_identifier=TIIE_28_UID, name=f"Interbank Equilibrium Interest Rate (TIIE) {TIIE_28_UID}"),
        RateConfig(unique_identifier=TIIE_91_UID, name=f"Interbank Equilibrium Interest Rate (TIIE) {TIIE_91_UID}"),
        RateConfig(unique_identifier=TIIE_182_UID, name=f"Interbank Equilibrium Interest Rate (TIIE) {TIIE_182_UID}"),

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
test_banxico_mbonos()

# test_databento_bars_small()
# test_databento_market_cap_small()
# test_databento_bars()
# test_equity_fundamentals()


# test_banxico_tiie()
# test_discount_curves()