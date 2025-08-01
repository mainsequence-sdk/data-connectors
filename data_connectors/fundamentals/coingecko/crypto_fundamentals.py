


import pandas as pd
from mainsequence.tdag.time_series import TimeSerie
from mainsequence.client import (UpdateStatistics, MARKETS_CONSTANTS, Asset, AssetCurrencyPair)
from datetime import datetime, timedelta
import pytz
from typing import List, Optional
from tqdm import tqdm
from data_connectors.fundamentals.utils import approximate_market_cap_with_polygon,get_polygon_financials,get_latest_market_cap_coingecko
import json
from data_connectors.utils import NAME_CRYPTO_MARKET_CAP


class CoinGeckoMarketCap(TimeSerie):
    """
    Gets latest MarketCap data. If no Market Data is available, gets the data since 1.1.2017.
    """

    def __init__(
            self,
            asset_list: Optional[List] = None,
            local_kwargs_to_ignore: List[str] = ["asset_universe"],
            *args,
            **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.asset_list = asset_list
        # If no asset_list is passed, we'll rely on the subclass to define _get_default_asset_list
        self.use_vam_assets = False
        self.create_categories=False
        if asset_list is None:
            self.use_vam_assets = True
            self.create_categories=True
        else:
            # Optional check: ensure all assets have the same venue
            assert all(
                [
                    a.execution_venue.symbol == MARKETS_CONSTANTS.FIGI_COMPOSITE_EV
                    for a in asset_list
                ]
            ), f"Execution Venue in all assets should be {MARKETS_CONSTANTS.FIGI_COMPOSITE_EV}"

    def dependencies(self):
        return {}

    def get_asset_list(self):
        if self.asset_list is not None:
            return self.asset_list
        else:
            currency_assets = AssetCurrencyPair.filter(
                execution_venue__symbol=MARKETS_CONSTANTS.MAIN_SEQUENCE_EV,
                security_type=MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_CRYPTO,
                security_market_sector=MARKETS_CONSTANTS.FIGI_MARKET_SECTOR_CURNCY,
                quote_asset__ticker="USDT",
            )
            asset_list = [a.base_asset for a in currency_assets]
            self.logger.info(
                f"{self.local_hash_id} is updating {len(asset_list)} assets"
            )
            return asset_list

    def update(self, update_statistics: "UpdateStatistics"):
        """
        Generic update that loops over assets, figures out from_date, calls
        the specialized _get_provider_data(...) and returns a DataFrame.
        """
        last_available_update = (
                datetime.now()
                .replace(tzinfo=pytz.utc, hour=0, minute=0, second=0, microsecond=0)
                - timedelta(days=1)
        )
        provider_data_list = []

        for asset in tqdm(update_statistics.asset_list):
            from_date = update_statistics.update_statistics[asset.unique_identifier]

            if from_date >= last_available_update:
                continue

            # Call the subclass-specific method to retrieve data
            provider_data = get_latest_market_cap_coingecko(
                from_date=from_date, symbol=asset.ticker, to_date=last_available_update
            )

            # Filter any data older than 'from_date'
            provider_data = provider_data[provider_data.index > from_date]
            if provider_data.shape[0] == 0:
                continue

            provider_data.loc[:, "unique_identifier"] = asset.unique_identifier
            provider_data_list.append(provider_data)

        if len(provider_data_list) == 0:
            return pd.DataFrame()

        provider_data = pd.concat(provider_data_list, axis=0)
        if len(provider_data) > 0:
            provider_data = provider_data.set_index(["unique_identifier"], append=True)

        return provider_data



    def _run_post_update_routines(self, error_on_last_update,update_statistics:UpdateStatistics):
        """
        Common post-update steps plus a call to subclass's `_register_in_backend`.
        """
        if error_on_last_update:
            self.logger.warning("Do not register data source due to error during run")
            return

        if self.metadata is not None:
            if not self.metadata.protect_from_deletion:
                self.local_persist_manager.protect_from_deletion()

        from mainsequence.client.models_vam import  AssetCategory

        #updater category from last_obsevation
        last_observation = self.get_last_observation()

        if last_observation is not None:
            last_date = last_observation.index[0][0]
            last_observation = last_observation["market_cap"].sort_values(ascending=False)
            last_observation = last_observation.iloc[:100]
            all_assets = Asset.filter(unique_identifier__in=last_observation.index.get_level_values("unique_identifier").to_list())
            asset_map = {a.unique_identifier: a.id for a in all_assets}
            last_observation = last_observation.reset_index()
            last_observation["asset_id"] = last_observation["unique_identifier"].map(asset_map).astype(int)
            for i in [10, 50, 100]:
                subset = last_observation.iloc[:i]

                name = NAME_CRYPTO_MARKET_CAP[i]
                crypto_category = AssetCategory.filter(display_name=name)
                if len(crypto_category) == 0:
                    crypto_category = AssetCategory.create(
                        display_name=name,
                        source="coingecko",
                        description=f"This category contains the top {i} cryptos by market cap as of {last_date}",
                        unique_id=name.replace(" ","_").lower(),
                    )
                    print(f"Created Categories: Crypto: {crypto_category}")
                else:
                    crypto_category = crypto_category[0]
                crypto_category.patch(assets=subset["asset_id"].to_list())
