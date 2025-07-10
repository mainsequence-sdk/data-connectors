import numpy as np
import pandas as pd
from mainsequence.tdag.time_series import TimeSerie, ModelList
from mainsequence.client import (DataUpdates, MARKETS_CONSTANTS,Asset)
from datetime import datetime, timedelta
import pytz
from typing import List, Optional
from tqdm import tqdm
from .utils import approximate_market_cap_with_polygon,get_polygon_financials
import json
import json
from ..utils import NAME_ALPACA_MARKET_CAP, get_stock_assets, register_mts_in_backed, register_rules
from mainsequence.client import MarketsTimeSeriesDetails, DataFrequency, AssetCategory, AssetTranslationTable, AssetTranslationRule, AssetFilter


class PolygonBaseTimeSeries(TimeSerie):
    """
    Base class that contains the common logic for both daily market cap and fundamentals.

    Subclasses must implement:
    - _get_provider_data(self, from_date, symbol, to_date) -> pd.DataFrame
      (i.e. which 'approximate_with_polygon' style function to call)
    - _register_in_backend(self)  -> None
      (what to do in post-update to register data source)

    Optionally:
    - _get_default_asset_list(self) -> ModelList
      (if the default set of assets to filter is different in each child class)
    """

    @TimeSerie._post_init_routines()
    def __init__(
        self,
        asset_list: Optional[ModelList] = None,
        local_kwargs_to_ignore: List[str] = ["asset_universe"],
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.asset_list = asset_list

        # If no asset_list is passed, we'll rely on the subclass to define _get_default_asset_list
        self.use_vam_assets = False
        if asset_list is None:
            self.use_vam_assets = True
        else:
            # Optional check: ensure all assets have the same venue
            assert all(
                [
                    a.execution_venue.symbol == MARKETS_CONSTANTS.FIGI_COMPOSITE_EV
                    for a in asset_list
                ]
            ), f"Execution Venue in all assets should be {MARKETS_CONSTANTS.FIGI_COMPOSITE_EV}"

    def _get_default_asset_list(self) -> ModelList:
        """
        If no asset_list is provided, this method should be overridden
        in child classes to return the default set of assets.
        """
        raise NotImplementedError("Subclass must implement its own default asset list.")

    def set_update_statistics(self, update_statistics: "DataUpdates"):
        """
        Overwrites the main method to include custom assets in the update if none provided.
        """
        # If we didn't get an explicit list, ask the subclass for the default
        if self.asset_list is None:
            self.asset_list = self._get_default_asset_list()
            self.logger.info(
                f"{self.local_hash_id} is updating {len(self.asset_list)} assets"
            )

        return super().set_update_statistics(update_statistics=update_statistics)

    def _get_provider_data(self, from_date: datetime, symbol: str, to_date: datetime) -> pd.DataFrame:
        """
        Subclasses must implement the actual fetch logic here
        (i.e., whichever 'approximate_with_polygon_*' function is needed).
        """
        raise NotImplementedError("Subclass must implement data‐fetching logic.")

    def update(self, update_statistics: "DataUpdates"):
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

        for asset in tqdm(self.asset_list):
            from_date = update_statistics.update_statistics[asset.unique_identifier]

            if from_date >= last_available_update:
                continue

            # Call the subclass-specific method to retrieve data
            provider_data = self._get_provider_data(
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

        provider_data = provider_data[~provider_data.index.duplicated(keep='first')]
        return provider_data

    def _register_in_backend(self):
        """
        Each subclass can implement how to register itself in the backend
        (unique_identifier, description, etc.).
        """
        raise NotImplementedError("Subclass must implement how to register in backend.")

    def  _run_post_update_routines(self, error_on_last_update,update_statistics:DataUpdates):
        """
        Common post-update steps plus a call to subclass's `_register_in_backend`.
        """
        if error_on_last_update:
            self.logger.warning("Do not register data source due to error during run")
            return

        if hasattr(self, "metadata"):
            if not self.metadata.protect_from_deletion:
                self.local_persist_manager.protect_from_deletion()

        # Let each subclass handle its own backend registration
        self._register_in_backend(update_statistics)


class PolygonDailyMarketCap(PolygonBaseTimeSeries):
    """
    Gets latest MarketCap data. If no Market Data is available,
    gets the data since 1.1.2017 (or any default you like).

    Uses 'approximate_with_polygon' internally.
    """

    def _get_default_asset_list(self):
        """
        Example default set of assets for daily market cap.
        """
        assets = get_stock_assets()
        return assets

    def _get_provider_data(self, from_date: datetime, symbol: str, to_date: datetime) -> pd.DataFrame:
        """
        Actually call approximate_with_polygon for Daily MarketCap data.
        """

        return approximate_market_cap_with_polygon(
            from_date=from_date,
            symbol=symbol,
            to_date=to_date,
        )

    def _register_in_backend(self, update_statistics):
        """
        Register 'Daily Market Cap Data' with the specific unique_identifier
        for historical market cap.
        """
        register_mts_in_backed(
            unique_identifier="polygon_historical_marketcap",
            time_serie=self,
            data_frequency_id=DataFrequency.one_d,
            description="Daily Market Cap Data",
            asset_list=update_statistics.asset_list
        )

        # add rule to asset translation table
        translation_table_identifier = "marketcap_translation_table"
        rules = [
            AssetTranslationRule(
                asset_filter=AssetFilter(
                    execution_venue_symbol=MARKETS_CONSTANTS.MAIN_SEQUENCE_EV,
                    security_type="Common Stock",
                ),
                markets_time_serie_unique_identifier="polygon_historical_marketcap",
                target_execution_venue_symbol=MARKETS_CONSTANTS.MAIN_SEQUENCE_EV,
            ),
            AssetTranslationRule(
                asset_filter=AssetFilter(
                    execution_venue_symbol=MARKETS_CONSTANTS.ALPACA_EV_SYMBOL,
                    security_type="Common Stock",
                ),
                markets_time_serie_unique_identifier="polygon_historical_marketcap",
                target_execution_venue_symbol=MARKETS_CONSTANTS.MAIN_SEQUENCE_EV,
            ),
        ]

        register_rules(
            translation_table_identifier,
            rules,
        )

        # updater category from last_obsevation
        last_observation = self.get_last_observation()

        if last_observation is not None:
            last_date = last_observation.index[0][0]
            last_observation = last_observation["market_cap"].sort_values(ascending=False)
            last_observation = last_observation.iloc[:100]
            all_assets = Asset.filter(
                unique_identifier__in=last_observation.index.get_level_values("unique_identifier").to_list(),
                real_figi=False
            )
            asset_map = {a.unique_identifier: a.id for a in all_assets}
            last_observation = last_observation.reset_index()
            last_observation = last_observation[last_observation["unique_identifier"].isin(asset_map.keys())]
            last_observation["asset_id"] = last_observation["unique_identifier"].map(asset_map).astype(int)
            for i in [10, 50, 100]:
                subset = last_observation.iloc[:i]

                name = NAME_ALPACA_MARKET_CAP[i]
                asset_category = AssetCategory.filter(display_name=name)
                if len(asset_category) == 0:
                    asset_category = AssetCategory.create(
                        display_name=name,
                        source="polygon",
                        description=f"This category contains the top {i} US Equities by market cap as of {last_date}",
                        unique_id=name.replace(" ", "_").lower(),
                    )
                    print(f"Created Categories: US Equity: {asset_category}")
                else:
                    asset_category = asset_category[0]
                asset_category.patch(assets=subset["asset_id"].to_list())


class PolygonQFundamentals(PolygonBaseTimeSeries):
    """
    Similar to PolygonDailyMarketCap but calls a different function to retrieve data
    (e.g. approximate_with_polygon_fundamentals) and registers itself differently.
    """

    def _get_default_asset_list(self):
        # Example only; pick whichever set of assets applies to 'fundamentals'
        assets = get_stock_assets()
        return assets

    def _get_provider_data(self, from_date: datetime, symbol: str, to_date: datetime) -> pd.DataFrame:
        """
        Replace with your fundamentals retrieval function
        """

        results=get_polygon_financials(
            from_date=from_date,
            ticker=symbol,
            to_date=to_date, timeframe="quarterly"
        )

        rows = []

        column_meta_data={}

        for result in results:
            # Create a dictionary to hold row data
            row = {}

            # 1) time_index (filing_date) and end_date
            filing_date = result.get("filing_date")
            end_date = result.get("end_date")
            row["filing_date"] = filing_date
            row["end_date"] = end_date

            # 2) Pull out balance_sheet items
            balance_sheet = result.get("financials", {}).get("balance_sheet", {})
            for key, item_dict in balance_sheet.items():
                # Column name like 'balance_sheet.assets' or 'balance_sheet.liabilities'
                col_name = f"bs_{key}"[:60]
                # We assume the "value" key holds the actual numeric value
                # (some items may not have a 'value')
                row[col_name] = item_dict.get("value")
                column_meta_data[col_name] = dict(label= item_dict.get("label"),
                                                  unit=item_dict.get("unit"),
                                                  description=item_dict.get("label"),
                                                  )

            # 3) Pull out income_statement items
            income_statement = result.get("financials", {}).get("income_statement", {})
            for key, item_dict in income_statement.items():
                col_name = f"is_{key}"[:60]
                row[col_name] = item_dict.get("value")
                column_meta_data[col_name] = dict(label=item_dict.get("label"),
                                                  unit=item_dict.get("unit"),
                                                  description=item_dict.get("label"),
                                                  )

            # 4) Store the entire financial response (for each row) in 'json'
            # row["JSONB.full_result"] = json.dumps(result.get("financials", {}))

            # Add the row to our container
            rows.append(row)

        # Create a DataFrame
        df = pd.DataFrame(rows)

        # Set the index to 'filing_date' and rename it 'time_index'
        if not df.empty and "end_date" in df.columns:
            df.set_index("end_date", inplace=True)
            df.index.rename("time_index", inplace=True)
            df.index = pd.to_datetime(df.index)
            # df.index = df.index.tz_localize("America/New_York")
            df.index = df.index.tz_localize("UTC")

            df.index = df.index.normalize() + pd.Timedelta(hours=23, minutes=59, seconds=59)
            df.index = df.index.tz_convert("UTC")
            df["filing_date"] = df["filing_date"].astype(str)
            df["quarter"] = df.index.year.astype(str) + "Q" + df.index.quarter.astype(str)

        self.column_meta_data = column_meta_data

        if df.index.duplicated().sum() > 0:
            df = df.loc[~df.index.duplicated(keep='last')]

        return df

    def _register_in_backend(self,update_statistics):
        from mainsequence.client import MarketsTimeSeriesDetails, DataFrequency
        register_mts_in_backed(  unique_identifier="polygon_historical_fundamentals",
            time_serie=self,
            data_frequency_id=DataFrequency.one_quarter,
            description="Quarterly Fundamentals Data provided by Polygon",
            asset_list=update_statistics.asset_list )

        if self.metadata.sourcetableconfiguration is not None:
            self.metadata.sourcetableconfiguration.patch_column_metadata(column_metadata=self.column_meta_data)


