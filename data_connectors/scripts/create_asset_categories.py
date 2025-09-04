import os

import pandas as pd
import requests
import mainsequence.client as msc
from mainsequence.tdag import APIDataNode


def create_crypto_categories():


    MARKET_CAP_ID="coingecko_market_cap"
    data_node=APIDataNode.build_from_identifier(identifier=MARKET_CAP_ID)
    update_statistic=data_node.get_update_statistics()
    range_descriptor=update_statistic.get_update_range_map_great_or_equal()

    last_df=data_node.get_ranged_data_per_asset(range_descriptor=range_descriptor)

    max_limit=last_df.sort_values("market_cap",ascending=False)
    max_limit=max_limit.iloc[:250]

    base_assets=msc.Asset.filter(unique_identifier__in=max_limit.index.get_level_values("unique_identifier").unique().tolist())
    uid_to_id_map={a.unique_identifier:a.id for a in base_assets}
    max_limit["id"]=max_limit.reset_index()["unique_identifier"].map(uid_to_id_map).values
    max_limit.dropna(inplace=True)
    quote_asset = msc.Asset.get(ticker="USDT",
                                **{'security_type': 'Crypto',
                                   'security_type_2': 'CRYPTO',
                                   'security_market_sector': 'Curncy', }
                                )
    for i in [25, 50, 100, 250]:
        # Get the top i coin IDs.
        group_coins = max_limit.iloc[:i]

        category_name = f"Binance Spot MarketCap Top {i} USDT"
        spot_in_category = msc.AssetCurrencyPair.filter(base_asset__id__in=group_coins["id"].to_list(),
                                                        quote_asset__id=quote_asset.id,
                                                        current_snapshot__exchange_code="BNCE" #only binance
                                                        )
        spot_in_category=[a.id for a in spot_in_category]
        msc.AssetCategory.get_or_create(display_name=category_name, source="binance/coingecko",
                                        assets=spot_in_category)


        future_category_name=f"Binance Futures MarketCap Top {i} USDT"

        future_in_category = msc.AssetFutureUSDM.filter(currency_pair__id__in=spot_in_category,
                                                        current_snapshot__exchange_code="BNF8"  # only binance
                                                        )
        future_in_category = [a.id for a in future_in_category]
        msc.AssetCategory.get_or_create(display_name=future_category_name, source="binance/coingecko",
                                        assets=future_in_category)


create_crypto_categories()