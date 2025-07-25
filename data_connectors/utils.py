import logging
import os

from mainsequence.client import MARKETS_CONSTANTS, Asset, AssetFutureUSDM, AssetCurrencyPair, AssetTranslationTable
from mainsequence.client import Asset, DataFrequency, AssetCategory

import psutil
import pandas as pd
from mainsequence.client.utils import DoesNotExist
from mainsequence.client.models_helpers import  MarketsTimeSeriesDetails
from typing import List

NAME_CRYPTO_MARKET_CAP_TOP10 = f"Top {10} Crypto Market Cap"
NAME_CRYPTO_MARKET_CAP_TOP50 = f"Top {50} Crypto Market Cap"
NAME_CRYPTO_MARKET_CAP_TOP100 = f"Top {100} Crypto Market Cap"
NAME_CRYPTO_MARKET_CAP = {10: NAME_CRYPTO_MARKET_CAP_TOP10, 50: NAME_CRYPTO_MARKET_CAP_TOP50, 100: NAME_CRYPTO_MARKET_CAP_TOP100}

NAME_US_EQUITY_MARKET_CAP_TOP10 = f"Top {10} US Equity Market Cap"
NAME_US_EQUITY_MARKET_CAP_TOP50 = f"Top {50} US Equity Market Cap"
NAME_US_EQUITY_MARKET_CAP_TOP100 = f"Top {100} US Equity Market Cap"

NAME_ALPACA_MARKET_CAP = {
    10: NAME_US_EQUITY_MARKET_CAP_TOP10,
    50: NAME_US_EQUITY_MARKET_CAP_TOP50,
    100: NAME_US_EQUITY_MARKET_CAP_TOP100
}

SP500_CATEGORY_NAME = "S&P500 Constitutents"
MAG_7_CATEGORY_NAME = "MAGNIFICENT 7"
MAG_7_CATEGORY_SYMBOLS = ['AAPL', 'MSFT', 'AMZN', 'GOOGL', 'META', 'NVDA', 'TSLA']


ETFS_MAIN_TICKERS = [
    "IVV",   # S&P 500 - US large-cap equity
    "SPY",
    "QQQ",   # Nasdaq 100 - US tech/innovation
    "IWM",   # Russell 2000 - US small-cap equities
    "EFA",   # Developed international markets (ex-US & Canada)
    "EEM",   # Emerging markets equities
    "BIL",   # 1-3 Month T-Bills - proxy for overnight rate / Fed policy
    "SHY",   # 1-3 Year US Treasury Bonds - short-term rates
    "IEF",   # 7-10 Year US Treasury Bonds - intermediate rates
    "TLT",   # 20+ Year US Treasury Bonds - long-term rates
    "LQD",   # Investment Grade Corporate Bonds - credit risk premium
    "HYG",   # High Yield Corporate Bonds - junk bond risk sentiment
    "AGG",   # US Aggregate Bond Market - broad fixed income exposure
    "IAU",   # Gold - inflation hedge, safe haven
    "USO",   # Crude Oil - commodity/inflation proxy
    "DBC",   # Broad Commodities Basket - inflation & global growth proxy
    "VNQ",   # US Real Estate - real asset / rates sensitivity
    "XLF",   # Financial Sector - banking, credit conditions
    "XLY",   # Consumer Discretionary - consumer spending & sentiment
    "XLP",   # Consumer Staples - defensive consumer demand
    "XLU",   # Utilities - interest rate sensitivity, defensive play
    "XLE",   # Energy Sector - oil prices and energy demand proxy

    # Style factor proxies (added below as requested)
    "MTUM",  # Momentum (12-month minus 1-month)
    "QUAL",  # Profitability / Quality factor (ROE, ROA)
    "VLUE",  # Value (book-to-price, earnings yield)
    "USMV",  # Low Volatility / Residual Volatility
    "SPLV",  # Another low-volatility ETF; high exposure to ResidualVol
    "VBR",  # Small-cap value; strong size and value factor exposure
    "VBK",  # Small-cap growth; size and growth exposure
    "RFG",  # Mid-cap growth; Growth and Momentum exposure
    "PRF",  # Fundamental Index; indirect exposure to Value, Earnings Yield
    "DVY",  # Dividend Yield; stable, high-dividend stocks
    "VTV",  # Large-cap value; value exposure
    "VUG",  # Large-cap growth; growth exposure
    "HDV",  # High Dividend Yield; dividend and quality/profitability tilt

]


def has_sufficient_memory(threshold_fraction=0.6) -> bool:
    mem = psutil.virtual_memory()
    # Return True if we are below threshold_fraction of total memory
    return (mem.used / mem.total) < threshold_fraction

def get_stock_assets():
    unique_identifier = SP500_CATEGORY_NAME.lower().replace(" ", "_")
    sp500_cat = AssetCategory.get_or_none(unique_identifier=unique_identifier)

    mag_7_cat = AssetCategory.get_or_none(unique_identifier=MAG_7_CATEGORY_NAME.lower().replace(" ", "_"))

    if sp500_cat is None:
        sp500_cat = AssetCategory.create(
            display_name=SP500_CATEGORY_NAME,
            source="datahub.io",
            description="Constitutens of S&P500",
            unique_identifier=unique_identifier,
        )
    if mag_7_cat is None:
        mag_7_cat = AssetCategory.create(
            display_name=MAG_7_CATEGORY_NAME,
            source="mainsequence",
            description="Constitutens of MAG7",
            unique_identifier=MAG_7_CATEGORY_NAME.lower().replace(" ", "_"),
        )

    mag_7_assets = Asset.filter(
        ticker__in=MAG_7_CATEGORY_SYMBOLS,
        execution_venue__symbol=MARKETS_CONSTANTS.MAIN_SEQUENCE_EV,
        security_type=MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_COMMON_STOCK,
        security_market_sector=MARKETS_CONSTANTS.FIGI_MARKET_SECTOR_EQUITY,
        real_figi=False,
    )

    mag_7_cat.patch(assets=[a.id for a in mag_7_assets])

    # get assets
    url = "https://datahub.io/core/s-and-p-500-companies/_r/-/data/constituents.csv"
    df = pd.read_csv(url)

    assets = Asset.filter(
        ticker__in=df["Symbol"].to_list(),
        execution_venue__symbol=MARKETS_CONSTANTS.MAIN_SEQUENCE_EV,
        security_type=MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_COMMON_STOCK,
        security_market_sector=MARKETS_CONSTANTS.FIGI_MARKET_SECTOR_EQUITY,
        real_figi=False,
    )

    assets_etfs=Asset.filter(
        ticker__in=ETFS_MAIN_TICKERS,
        execution_venue__symbol=MARKETS_CONSTANTS.MAIN_SEQUENCE_EV,
        security_type=MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_ETP,
        security_market_sector=MARKETS_CONSTANTS.FIGI_MARKET_SECTOR_EQUITY,
        real_figi=False,
    )


    sp500_cat.patch(assets=[a.id for a in assets])
    return assets+assets_etfs


def register_mts_in_backed(
        unique_identifier,
        time_serie,
        data_frequency_id,
        asset_list: List[Asset],
        description="",
):
    try:
        bar_source = MarketsTimeSeriesDetails.get(
            unique_identifier=unique_identifier,
        )

        if time_serie.use_vam_assets and bar_source.source_table.id != time_serie.metadata.id:
            bar_source = bar_source.patch(source_table=time_serie.metadata.id)

    except DoesNotExist:
        if time_serie.use_vam_assets:
            # if run for the first time save this as reference in VAM
            bar_source = MarketsTimeSeriesDetails.update_or_create(
                unique_identifier=unique_identifier,
                source_table__id=time_serie.metadata.id,
                data_frequency_id=data_frequency_id,
                description=description,
            )

    if bar_source is None:
        raise ValueError("No historical bars source found")

    bar_source.append_asset_list_source(asset_list=asset_list)

    return bar_source


def register_rules(
    translation_table_identifier,
    rules,
):
    translation_table = AssetTranslationTable.get_or_none(unique_identifier=translation_table_identifier)
    rules_serialized = [r.model_dump() for r in rules]

    if translation_table is None:
        translation_table = AssetTranslationTable.create(
            unique_identifier=translation_table_identifier,
            rules=rules_serialized,
        )
    else:
        translation_table.add_rules(rules)
