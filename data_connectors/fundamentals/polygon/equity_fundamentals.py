import numpy as np
import pandas as pd
from mainsequence.tdag.time_series import DataNode
from mainsequence.client import (UpdateStatistics, MARKETS_CONSTANTS,Asset)
from datetime import datetime, timedelta
import pytz
from typing import List, Optional
from tqdm import tqdm
from data_connectors.fundamentals.utils import approximate_market_cap_with_polygon,get_polygon_financials
import json
import json
from data_connectors.utils import NAME_ALPACA_MARKET_CAP, get_stock_assets
from mainsequence.client import  DataFrequency, AssetCategory, AssetTranslationTable, AssetTranslationRule, AssetFilter
import mainsequence.client as ms_client

class PolygonBaseTimeSeries(DataNode):
    """
    Base class that contains the common logic for both daily market cap and fundamentals.

    Subclasses must implement:
    - _get_provider_data(self, from_date, symbol, to_date) -> pd.DataFrame
      (i.e. which 'approximate_with_polygon' style function to call)
    - _register_in_backend(self)  -> None
      (what to do in post-update to register data source)

    Optionally:
    - _get_default_asset_list(self) -> List
      (if the default set of assets to filter is different in each child class)
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


        # else:
        #     # Optional check: ensure all assets have the same venue
        #     assert all(
        #         [
        #             a.current_snapshot.exchange_code ==None
        #             for a in asset_list
        #         ]
        #     ), f"Execution Venue in all assets should be None Execution VEnue"

    def dependencies(self):
        return {}

    def get_asset_list(self) -> List:
        """
        If no asset_list is provided, this method should be overridden
        in child classes to return the default set of assets.
        """
        raise NotImplementedError("Subclass must implement its own default asset list.")

    def _get_provider_data(self, from_date: datetime, symbol: str, to_date: datetime) -> pd.DataFrame:
        """
        Subclasses must implement the actual fetch logic here
        (i.e., whichever 'approximate_with_polygon_*' function is needed).
        """
        raise NotImplementedError("Subclass must implement data‐fetching logic.")

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
            from_date = update_statistics.get_last_update_index_2d(asset.unique_identifier)

            if from_date >= last_available_update:
                continue

            # Call the subclass-specific method to retrieve data
            provider_data = self._get_provider_data(
                from_date=from_date, symbol=asset.current_snapshot.ticker, to_date=last_available_update
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





    def  run_post_update_routines(self, error_on_last_update,update_statistics:UpdateStatistics):
        """
        Common post-update steps plus a call to subclass's `_register_in_backend`.
        """
        if error_on_last_update:
            self.logger.warning("Do not register data source due to error during run")
            return

        if self.metadata is not None:
            if not self.metadata.protect_from_deletion:
                self.local_persist_manager.protect_from_deletion()




class PolygonDailyMarketCap(PolygonBaseTimeSeries):
    """
    Gets latest MarketCap data. If no Market Data is available,
    gets the data since 1.1.2017 (or any default you like).

    Uses 'approximate_with_polygon' internally.
    """

    def get_asset_list(self):
        """
        Example default set of assets for daily market cap.
        """
        if self.asset_list is None:
            self.asset_list = get_stock_assets(inlcude_etfs=False)
        return self.asset_list

    def _get_provider_data(self, from_date: datetime, symbol: str, to_date: datetime) -> pd.DataFrame:
        """
        Actually call approximate_with_polygon for Daily MarketCap data.
        """

        return approximate_market_cap_with_polygon(
            from_date=from_date,
            symbol=symbol,
            to_date=to_date,
        )


    def get_table_metadata(self, update_statistics) -> Optional[ms_client.TableMetaData]:
        # Logic from original code for automatic VAM creation

        identifier = f"polygon_historical_marketcap"
        return ms_client.TableMetaData(
            identifier=identifier,
            description="Daily Market Cap Data From Polygon",
            data_frequency_id=DataFrequency.one_d,
        )





class PolygonQFundamentals(PolygonBaseTimeSeries):
    """
    Similar to PolygonDailyMarketCap but calls a different function to retrieve data
    (e.g. approximate_with_polygon_fundamentals) and registers itself differently.
    """

    def get_asset_list(self):
        # Example only; pick whichever set of assets applies to 'fundamentals'
        self.asset_list = get_stock_assets()
        return self.asset_list

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



