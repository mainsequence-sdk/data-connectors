import os

import pandas as pd
import requests
from mainsequence.client import Asset, AssetCategory, MARKETS_CONSTANTS, User, ExecutionVenue

user = User.get_authenticated_user_details()
org_uid = user.organization.uid  #

def create_crypto_categories():
    headers = {
        "X-Cg-Pro-Api-Key": os.getenv("COINGECKO_API_KEY"),
    }

    def fetch_coin_tickers(coin_id, market="binance"):
        """
        /coins/{coin_id} with tickers=true returns exchange listings (incl. Binance)
        """
        url = f"https://pro-api.coingecko.com/api/v3/coins/{coin_id}"
        params = {
            "localization": "false",
            "tickers": "true",
            "market_data": "false",
            "community_data": "false",
            "developer_data": "false",
            "sparkline": "false",
        }
        r = requests.get(url, params=params, headers=headers)
        r.raise_for_status()
        data = r.json()
        if coin_id == "tether":
            return {coin_id: "USDT"}
        data = {
            coin_id: c["base"] for c in data["tickers"]
            if (c["market"]["identifier"] == market) and (c["target"] == "USDT")
        }

        if not data:
            print("No data")

        return data

    # 1 build category top market cap crypto
    def get_all_coins():
        url = "https://pro-api.coingecko.com/api/v3/coins/markets"
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 250,
            "page": 1
        }

        all_coins = []

        while True:
            response = requests.get(url, params=params, headers=headers)
            data = response.json()

            if not data:
                break  # Stop if no more data

            all_coins.extend(data)
            params["page"] += 1  # Move to the next page
            if params["page"] >= 1:
                break

        return all_coins

    binance_ev = ExecutionVenue.get(symbol=MARKETS_CONSTANTS.BINANCE_EV_SYMBOL)
    at = MARKETS_CONSTANTS.ASSET_TYPE_CRYPTO_SPOT

    all_assets = Asset.filter(
        execution_venue__symbol=binance_ev.symbol,
        asset_type=at,
    )

    asset_requested = []
    coins_df = get_all_coins()
    coins_df = pd.DataFrame(coins_df).sort_values("market_cap", ascending=False)

    coins_df["symbol"] = coins_df["id"].iloc[:10].apply(lambda coin_id: fetch_coin_tickers(coin_id, market="binance")[coin_id])

    # remove wrapped
    coins_df[~coins_df.id.apply(lambda x: "wrapped" in x)]
    for i in [25, 50, 100, 250]:
        # Get the top i coin IDs.
        group_coins = coins_df.iloc[:i]

        category_name = f"Crypto MarketCap Top {i}"
        category = AssetCategory.get_or_none(organization_owner_uid=org_uid, name=category_name)
        if category is not None:
            print(f"Category {category_name} already exists")
            continue


        # TODO set venue_specific_properties with coingecko id
        # TODO finish!!!
        group_assets = [a for a in all_assets if a.ticker in group_coins["symbol"]]

        AssetCategory.create(display_name=category_name, source="mainsequence", assets=crypto_asset_ids)


create_crypto_categories()