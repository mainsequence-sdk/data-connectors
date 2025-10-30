from src.data_connectors.prices.alpaca.time_series import AlpacaEquityBars
from src.data_connectors.fundamentals.coingecko.crypto_fundamentals import CoinGeckoMarketCap
from src.data_connectors.fundamentals.polygon.equity_fundamentals import PolygonQFundamentals
from src.data_connectors.prices.binance.time_series import BinanceHistoricalBars, TimeBarConfig
from src.data_connectors.fundamentals.polygon.equity_fundamentals import PolygonDailyMarketCap
from src.data_connectors.scripts.create_translation_tables import create_asset_translation_table

ts = PolygonDailyMarketCap(asset_list=None)
ts.run(debug_mode=True, force_update=True)

ts = CoinGeckoMarketCap(asset_list=None)
ts.run(debug_mode=True, force_update=True)

ts = PolygonQFundamentals(asset_list=None)
ts.run(debug_mode=True, force_update=True)

ts = AlpacaEquityBars(asset_list=None, frequency_id="1d", adjustment="all")
ts.run(debug_mode=True, update_tree=True, force_update=True)

ts = BinanceHistoricalBars(asset_category_identifier="top_50_crypto_market_cap", asset_list=None, bar_configuration=TimeBarConfig(frequency_id="1d"))
ts.run(debug_mode=True, update_tree=True, force_update=True)


create_asset_translation_table()