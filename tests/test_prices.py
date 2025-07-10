
from dotenv import load_dotenv
from pathlib import Path

import os
# Load environment variables from .env.example
project_root = Path(__file__).resolve().parents[1]
env_file_path = project_root / ".env"

load_dotenv(dotenv_path=env_file_path)
from mainsequence.tdag.time_series import TimeSerie, ModelList, APITimeSerie

class TestTimeSerie(TimeSerie):
    @TimeSerie._post_init_routines()
    def __init__(self, data_source_id: str, local_hash_id: str, *args, **kwargs):
        self.bars = APITimeSerie(local_hash_id=local_hash_id, data_source_id=data_source_id)
        super().__init__(*args, **kwargs)

    def update(self, latest_value, *args, **kwargs):
        full_data = self.bars.get_df_between_dates(
            start_date=latest_value,
            end_date=None,
            great_or_equal=False,
                                       )

        a=5

        return full_data


def test_binance_bars_from_trades():
    from data_connectors.prices.binance.time_series import BinanceBarsFromTrades
    from mainsequence.client import AssetFutureUSDM, AssetCurrencyPair

    future_assets = AssetFutureUSDM.filter(symbol__in=["BTCUSDT", "ETHUSDT", "1000SHIBUSDT"])
    spot_assets = AssetCurrencyPair.filter(symbol__in=["BTCUSDT", "ETHUSDT", "SHIBUSDT"])

    # ts = BinanceBarsFromTrades(asset_list=[spot_assets[0]]+[future_assets[0]],
    # )
    ts = BinanceBarsFromTrades(asset_list=None)
    ts.run(debug_mode=True,force_update=True)

def test_binance_daily_bars():
    from data_connectors.prices.binance.time_series import BinanceHistoricalBars
    ts = BinanceHistoricalBars(asset_list=None, frequency_id="1d")
    ts.run(debug_mode=True,force_update=True)


def test_alpaca_bars():
    from data_connectors.prices.alpaca.time_series import AlpacaEquityBars
    from mainsequence.client import  Asset
    from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

    ts = AlpacaEquityBars(asset_list=None, frequency_id="1d", adjustment="all")
    ts.run(debug_mode=True, force_update=True)

def test_databento_bars_small():
    from data_connectors.prices.databento.time_series import DatabentoHistoricalBars
    from mainsequence.client import AssetCategory
    asset_cat = AssetCategory.get(unique_identifier="magnificent_7")
    assets = asset_cat.get_assets()[:2]
    ts = DatabentoHistoricalBars(
        asset_list=assets,
        frequency_id="1d",
        dataset="XNAS.ITCH"
    )
    ts.run(debug_mode=True, force_update=True)

def test_alpaca_bars_small():
    from data_connectors.prices.alpaca.time_series import AlpacaEquityBars
    from mainsequence.client import AssetCategory
    asset_cat = AssetCategory.get(unique_identifier="magnificent_7")
    assets = asset_cat.get_assets()[:2]
    ts = AlpacaEquityBars(asset_list=assets, frequency_id="1d", adjustment="all")
    ts.run(debug_mode=True, force_update=True)

def test_api_time_series():
    from data_connectors.prices.binance.time_series import BinanceBarsFromTrades
    from mainsequence.client import AssetFutureUSDM, AssetCurrencyPair

    future_assets = AssetFutureUSDM.filter(symbol__in=["BTCUSDT", "ETHUSDT", "1000SHIBUSDT"])
    spot_assets = AssetCurrencyPair.filter(symbol__in=["BTCUSDT", "ETHUSDT", "SHIBUSDT"])

    ts = BinanceBarsFromTrades(asset_list=[spot_assets[0]] + [future_assets[0]])

    new_ts = TestTimeSerie(local_hash_id=ts.local_hash_id,data_source_id=ts.metadata["data_source"]["id"])
    new_ts.run_in_debug_scheduler()

def test_equity_market_cap():
    from data_connectors.fundamentals.equity_fundamentals import PolygonDailyMarketCap
    ts = PolygonDailyMarketCap(asset_list=None)
    ts.run(debug_mode=True, force_update=True)

def test_crypto_market_cap():
    from data_connectors.fundamentals.crypto_fundamentals import CoinGeckoMarketCap
    ts = CoinGeckoMarketCap(asset_list=None)
    ts.run(debug_mode=True, force_update=True)

def test_equity_fundamentals():
    from data_connectors.fundamentals.equity_fundamentals import PolygonQFundamentals
    ts = PolygonQFundamentals(asset_list=None)
    ts.run(debug_mode=True, force_update=True)



# test_api_time_series()
# test_binance_bars_from_trades()
# test_binance_daily_bars()
# test_alpaca_bars()
# test_crypto_market_cap()
# test_equity_market_cap()
# test_alpaca_bars_small()
test_databento_bars_small()
# test_equity_fundamentals()