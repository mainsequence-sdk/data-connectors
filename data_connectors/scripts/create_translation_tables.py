from mainsequence.client import AssetTranslationRule, AssetFilter, MARKETS_CONSTANTS

from data_connectors.utils import register_rules


def create_asset_translation_table():

    # --- ALPACA ---
    translation_table_identifier = f"prices_translation_table_1d"
    markets_time_series_identifier = f"alpaca_1d_bars"
    rules = [
        AssetTranslationRule(
            asset_filter=AssetFilter(
                security_type=MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_COMMON_STOCK,
            ),
            markets_time_serie_unique_identifier=markets_time_series_identifier,
        ),
        AssetTranslationRule(
            asset_filter=AssetFilter(
                security_type=MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_ETP,
            ),
            markets_time_serie_unique_identifier=markets_time_series_identifier,
        )
    ]
    register_rules(
        translation_table_identifier,
        rules,
    )


    # --- BINANCE ---
    translation_table_identifier = f"prices_translation_table_1d"

    rules = [
        AssetTranslationRule(
            asset_filter=AssetFilter(
                security_type=MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_CRYPTO,
            ),
            markets_time_serie_unique_identifier="binance_1d_bars",
        ),
    ]

    register_rules(
        translation_table_identifier,
        rules,
    )


    # --- MARKET CAP ---
    translation_table_identifier = "marketcap_translation_table"

    # Binance
    markets_time_series_identifier = f"coingecko_market_cap"
    rules = [
        AssetTranslationRule(
            asset_filter=AssetFilter(
                security_type=MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_CRYPTO,
            ),
            markets_time_serie_unique_identifier=markets_time_series_identifier,
        ),
    ]
    register_rules(
        translation_table_identifier,
        rules,
    )

    # Polygon
    markets_time_series_identifier = f"polygon_historical_marketcap"
    rules = [
        AssetTranslationRule(
            asset_filter=AssetFilter(
                security_type=MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_COMMON_STOCK,
            ),
            markets_time_serie_unique_identifier=markets_time_series_identifier,
        ),
    ]
    register_rules(
        translation_table_identifier,
        rules,
    )


if __name__ == "__main__":
    create_asset_translation_table()