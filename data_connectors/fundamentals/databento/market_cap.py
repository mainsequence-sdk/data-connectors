import datetime
import os
from typing import List, Optional

import databento as db
import pandas as pd
import pytz
from mainsequence.client import (
    UpdateStatistics, Asset, MarketsTimeSeriesDetails, DataFrequency,
    AssetTranslationRule, AssetFilter, MARKETS_CONSTANTS
)
from mainsequence.tdag.data_nodes import DataNode, APITimeSerie

from ...utils import get_stock_assets, register_rules

DATABENTO_API_KEY = os.environ.get('DATABENTO_API_KEY')


class DatabentoMarketCap(DataNode):
    """
    Calculates daily market cap using Databento prices and reference data.
    It reuses an existing Databento prices DataNode.
    """
    OFFSET_START = datetime.datetime(2019, 1, 1, tzinfo=pytz.utc)

    def __init__(self,
                 asset_list: Optional[List],
                 prices_time_serie_unique_identifier: str,
                 dataset: str,
                 local_kwargs_to_ignore: List[str] = ["asset_list"],
                 *args, **kwargs):
        super().__init__(*args, **kwargs)

        raise NotImplementedError("This time series is not yet fully supported ")

        if not DATABENTO_API_KEY:
            raise ValueError("DATABENTO_API_KEY environment variable not set.")

        self.asset_list = asset_list
        self.prices_time_serie_unique_identifier = prices_time_serie_unique_identifier
        self.dataset = dataset

        self.use_vam_assets = self.asset_list is None

        self.ref_client = db.Reference(key=DATABENTO_API_KEY)

        self.prices_ts = None
        try:
            mts_details = MarketsTimeSeriesDetails.get(unique_identifier=self.prices_time_serie_unique_identifier)
            if mts_details and mts_details.source_table:
                self.prices_ts = APITimeSerie(
                    data_source_id=mts_details.source_table.data_source,
                    source_table_hash_id=mts_details.source_table.hash_id
                )
        except Exception as e:
            self.logger.error(f"Could not instantiate APITimeSerie for prices using identifier '{self.prices_time_serie_unique_identifier}': {e}")

    def dependencies(self):
        return {}

    def get_asset_list(self) -> List[Asset]:
        if self.asset_list is None:
            self.logger.info(f"{self.local_hash_id} is using default stock assets.")
            self.asset_list = get_stock_assets()
        return self.asset_list

    def update(self,):
        """
        Calculates market cap on a per-asset basis. For each asset, it fetches new price data
        and then queries for shares outstanding only over the time period for which new prices were found.
        """
        if self.prices_ts is None:
            self.logger.error("Prices APITimeSerie is not available, skipping update.")
            return pd.DataFrame()

        all_market_caps = []

        for asset in self.update_statistics.asset_list:
            unique_identifier = asset.unique_identifier
            from_date = self.update_statistics[unique_identifier]

            to_date = datetime.datetime.now(pytz.utc)

            df_bars = self.prices_ts.get_df_between_dates(start_date=from_date, end_date=to_date,
                                                          unique_identifier_list=[asset.unique_identifier])

            if df_bars.empty:
                continue

            if isinstance(df_bars.index, pd.MultiIndex):
                df_bars = df_bars.reset_index(level='unique_identifier', drop=True)

            prices_start_date = df_bars.index.min()
            prices_end_date = df_bars.index.max()

            # 2. Get shares outstanding for the asset for the new price date range
            try:
                df_security_master = self.ref_client.security_master.get_range(
                    symbols=[asset.ticker],
                    start=prices_start_date.strftime("%Y-%m-%d"),
                    end=(prices_end_date + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
                ).to_df()
            except Exception as e:
                self.logger.error(f"Failed to get security master data from Databento for {asset.ticker}: {e}")
                continue

            if df_security_master.empty:
                self.logger.warning(f"No security master data from Databento for {asset.ticker} in range {prices_start_date} - {prices_end_date}.")
                continue

            df_security_master["shares_outstanding_date"] = pd.to_datetime(
                df_security_master["shares_outstanding_date"],
                utc=True,
            )

            df_shares = df_security_master.set_index("shares_outstanding_date")[['shares_outstanding']]
            df_shares = df_shares[~df_shares.index.duplicated(keep='first')]

            full_date_range = pd.date_range(start=prices_start_date.normalize(), end=prices_end_date.normalize(), freq='D', tz='UTC')
            df_shares = df_shares.reindex(full_date_range, method='ffill')

            # 3. Calculate market cap
            df_bars.index = df_bars.index.normalize()

            merged = df_bars.join(df_shares, how='left')
            merged['shares_outstanding'] = merged['shares_outstanding'].ffill()
            merged.dropna(subset=['shares_outstanding', 'close'], inplace=True)

            if merged.empty:
                continue

            merged['market_cap'] = merged['close'] * merged['shares_outstanding']
            merged = merged[['market_cap']].dropna()

            if merged.empty:
                continue

            merged['unique_identifier'] = unique_identifier
            all_market_caps.append(merged.reset_index().set_index(['time_index', 'unique_identifier']))

        if not all_market_caps:
            return pd.DataFrame()

        final_df = pd.concat(all_market_caps)
        final_df = final_df[~final_df.index.duplicated(keep='first')]

        return final_df

    def _run_post_update_routines(self, error_on_last_update: bool):
        if error_on_last_update:
            self.logger.warning("Do not register data source due to error during run")
            return

        if self.metadata is not None:
            if not self.metadata.protect_from_deletion:
                self.local_persist_manager.protect_from_deletion()

        if self.use_vam_assets:
            markets_time_series_identifier = f"databento_{self.dataset.lower().replace('.', '_')}_market_cap"

            translation_table_identifier = "marketcap_translation_table"
            rules = [
                AssetTranslationRule(
                    asset_filter=AssetFilter(
                        execution_venue_symbol=MARKETS_CONSTANTS.MAIN_SEQUENCE_EV,
                        security_type=MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_COMMON_STOCK,
                    ),
                    markets_time_serie_unique_identifier=markets_time_series_identifier,
                    target_execution_venue_symbol=MARKETS_CONSTANTS.MAIN_SEQUENCE_EV,
                )
            ]
            register_rules(translation_table_identifier, rules)