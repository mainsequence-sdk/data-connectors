from mainsequence.client import AssetTranslationRule, AssetFilter, MARKETS_CONSTANTS

from data_connectors.utils import register_rules

if __name__ == "__main__":

    # --- ALPACA ---
    translation_table_identifier = f"alpaca_prices"
    markets_time_series_identifier = f"alpaca_1d_bars"
    rules = [
        AssetTranslationRule(
            asset_filter=AssetFilter(
                execution_venue_symbol=MARKETS_CONSTANTS.MAIN_SEQUENCE_EV,
                security_type=MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_COMMON_STOCK,
            ),
            markets_time_serie_unique_identifier=markets_time_series_identifier,
            target_execution_venue_symbol=MARKETS_CONSTANTS.ALPACA_EV_SYMBOL,
        ),
        AssetTranslationRule(
            asset_filter=AssetFilter(
                execution_venue_symbol=MARKETS_CONSTANTS.ALPACA_EV_SYMBOL,
                security_type=MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_COMMON_STOCK,
            ),
            markets_time_serie_unique_identifier=markets_time_series_identifier,
            target_execution_venue_symbol=MARKETS_CONSTANTS.ALPACA_EV_SYMBOL,
        ),
    ]
    register_rules(
        translation_table_identifier,
        rules,
    )


    # --- DATABENTO ---
    # keeps the assets in the same venue
    translation_table_identifier = f"databento_prices"
    markets_time_series_identifier = f"databento_xnas_itch_1d_bars"
    rules = [
        AssetTranslationRule(
            asset_filter=AssetFilter(
                execution_venue_symbol=MARKETS_CONSTANTS.MAIN_SEQUENCE_EV,
                security_type=MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_COMMON_STOCK,
            ),
            markets_time_serie_unique_identifier=markets_time_series_identifier,
            target_execution_venue_symbol=MARKETS_CONSTANTS.MAIN_SEQUENCE_EV,
        ),
        AssetTranslationRule(
            asset_filter=AssetFilter(
                execution_venue_symbol=MARKETS_CONSTANTS.MAIN_SEQUENCE_EV,
                security_type=MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_COMMON_STOCK,
            ),
            markets_time_serie_unique_identifier=markets_time_series_identifier,
            target_execution_venue_symbol=MARKETS_CONSTANTS.MAIN_SEQUENCE_EV,
        ),
    ]
    register_rules(
        translation_table_identifier,
        rules,
    )


    # --- BINANCE (UNUSED FOR NOW) ---
    # translation_table_identifier = f"binance_prices"
    #
    # rules = [
    #     AssetTranslationRule(
    #         asset_filter=AssetFilter(
    #             execution_venue_symbol=MARKETS_CONSTANTS.MAIN_SEQUENCE_EV,
    #             security_type=MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_CRYPTO,
    #         ),
    #         markets_time_serie_unique_identifier="binance_1d_bars",
    #         target_execution_venue_symbol=MARKETS_CONSTANTS.BINANCE_EV_SYMBOL,
    #     ),
    #     # From binance crypto assign binance
    #     AssetTranslationRule(
    #         asset_filter=AssetFilter(
    #             execution_venue_symbol=MARKETS_CONSTANTS.BINANCE_EV_SYMBOL,
    #             security_type=MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_CRYPTO,
    #         ),
    #         markets_time_serie_unique_identifier="binance_1d_bars",
    #         target_execution_venue_symbol=MARKETS_CONSTANTS.BINANCE_EV_SYMBOL,
    #     ),
    # ]
    #
    # register_rules(
    #     translation_table_identifier,
    #     rules,
    # )


    # --- MARKET CAP ---
    translation_table_identifier = "marketcap_translation_table"

    # Binance
    markets_time_series_identifier = f"coingecko_market_cap"
    rules = [
        AssetTranslationRule(
            asset_filter=AssetFilter(
                execution_venue_symbol=MARKETS_CONSTANTS.MAIN_SEQUENCE_EV,
                security_type=MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_CRYPTO,
            ),
            markets_time_serie_unique_identifier=markets_time_series_identifier,
            target_execution_venue_symbol=MARKETS_CONSTANTS.MAIN_SEQUENCE_EV,
        ),
        # From binance crypto assign binance
        AssetTranslationRule(
            asset_filter=AssetFilter(
                execution_venue_symbol=MARKETS_CONSTANTS.BINANCE_EV_SYMBOL,
                security_type=MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_CRYPTO,
            ),
            markets_time_serie_unique_identifier=markets_time_series_identifier,
            target_execution_venue_symbol=MARKETS_CONSTANTS.MAIN_SEQUENCE_EV,
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
                execution_venue_symbol=MARKETS_CONSTANTS.MAIN_SEQUENCE_EV,
                security_type=MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_COMMON_STOCK,
            ),
            markets_time_serie_unique_identifier=markets_time_series_identifier,
            target_execution_venue_symbol=MARKETS_CONSTANTS.MAIN_SEQUENCE_EV,
        ),
        AssetTranslationRule(
            asset_filter=AssetFilter(
                execution_venue_symbol=MARKETS_CONSTANTS.ALPACA_EV_SYMBOL,
                security_type=MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_COMMON_STOCK,
            ),
            markets_time_serie_unique_identifier=markets_time_series_identifier,
            target_execution_venue_symbol=MARKETS_CONSTANTS.MAIN_SEQUENCE_EV,
        ),
    ]
    register_rules(
        translation_table_identifier,
        rules,
    )
