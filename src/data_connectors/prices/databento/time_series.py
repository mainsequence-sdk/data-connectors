import datetime
import os
from typing import List, Optional

import numpy as np
import pandas_market_calendars as mcal

import databento as db
import pandas as pd
import pytz
from mainsequence.client import UpdateStatistics, Asset
from mainsequence.tdag.data_nodes import DataNode, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

from ...utils import get_stock_assets

DATABENTO_API_KEY = os.environ.get('DATABENTO_API_KEY')


class DatabentoHistoricalBars(DataNode):
    """
    Integration for Databento Data Series.
    Fetches historical bar data from Databento.
    """
    SCHEMA_MAP = {
        "1d": "ohlcv-1d",
        "1h": "ohlcv-1h",
        "1m": "ohlcv-1m",
        "1s": "ohlcv-1s",
    }
    # Databento misses earlier data
    OFFSET_START = datetime.datetime(2019, 1, 1, tzinfo=pytz.utc)
    _ARGS_IGNORE_IN_STORAGE_HASH=["asset_list"]
    def __init__(self, asset_list: Optional[List], frequency_id: str,
                 dataset: str,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)

        if frequency_id not in self.SCHEMA_MAP:
            raise ValueError(f"Unsupported frequency_id for Databento: {frequency_id}")

        if not DATABENTO_API_KEY:
            raise ValueError("DATABENTO_API_KEY environment variable not set.")

        self.asset_list = asset_list
        self.frequency_id = frequency_id
        self.dataset = dataset
        self.schema = self.SCHEMA_MAP[frequency_id]

        self.use_vam_assets = False
        if self.asset_list is None:
            self.use_vam_assets = True

        self.client = db.Historical(key=DATABENTO_API_KEY)

    def dependencies(self):
        return {}

    def _get_asset_list(self) -> List[Asset]:
        """
        Returns the default asset list if none is provided.
        """
        if self.asset_list is None:
            self.logger.info(f"{self.local_hash_id} is using default stock assets.")
            self.asset_list = get_stock_assets()

        self.asset_calendar_map = {a.unique_identifier: a.get_calendar().name for a in self.asset_list}
        return self.asset_list

    def _process_asset_request(self, asset: Asset, from_date: pd.Timestamp, to_date: pd.Timestamp) -> Optional[pd.DataFrame]:
        """
        Fetches and processes data for a single asset in a thread-safe manner.
        """
        try:
            data = self.client.timeseries.get_range(
                dataset=self.dataset,
                symbols=[asset.ticker],
                schema=self.schema,
                start=from_date,
                end=to_date,
            ).to_df()

            if data.empty:
                return None

            # calcualte vwap
            pvt = data[["open", "high", "close"]].mean(axis=1) * data["volume"]
            data["vwap"] = pvt.cumsum() / data["volume"].cumsum()

            data.index.name = "time_index"
            data['unique_identifier'] = asset.unique_identifier
            data["calendar"] = self.asset_calendar_map[asset.unique_identifier]
            data = data.set_index(['unique_identifier'], append=True)
            return data

        except Exception as e:
            self.logger.error(f"Error fetching data for {asset.ticker} from Databento: {e}")
            return None

    def update(self):
        """
        Fetches updates for all assets in parallel.
        """
        calendars = {str(cal): mcal.get_calendar(cal.replace("ARCA", "XNYS").replace("AMEX","XNYS")) for cal in np.unique(list(self.asset_calendar_map.values()))}

        last_available_value = datetime.datetime.now(pytz.utc).replace(hour=0, minute=0, second=0,
                                                                       microsecond=0) - datetime.timedelta(minutes=1)

        # check which assets need to be updated
        if self.update_statistics.is_empty():
            calendar_max_closes = {}
            for cal_name, cal in calendars.items():
                full_schedule = cal.schedule(last_available_value-datetime.timedelta(days=7), last_available_value) #cal.schedule(*min_max)
                calendar_max_closes[cal_name] = full_schedule.iloc[-1]["market_close"]

            min_max = [self.update_statistics.get_min_latest_value(), datetime.datetime.now(pytz.utc).replace(hour=0, minute=0, second=0,)]
            for cal_name, cal in calendars.items():
                full_schedule = cal.schedule(*min_max)
                calendar_max_closes[cal_name] = full_schedule.iloc[-1]["market_close"]

            asset_identifier_to_update = []
            for a in self.update_statistics.asset_list:
                if self.update_statistics[a.unique_identifier] < calendar_max_closes[self.asset_calendar_map[a.unique_identifier]]:
                    asset_identifier_to_update.append(a.unique_identifier)
                    continue

            if len(asset_identifier_to_update) == 0:
                self.logger.info(f"Nothing to update, prices not yet available in calendars {calendar_max_closes.keys()}")
                return pd.DataFrame()

            update_statistics = self.update_statistics.update_assets(self.update_statistics.asset_list)

        max_skip = update_statistics.get_max_latest_value(init_fallback_date=self.OFFSET_START) + datetime.timedelta(days=360)
        if max_skip < datetime.datetime.now(pytz.utc):
            last_available_value = max_skip


        # Perform update
        bars_request_df = []
        workers = 1
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [
                executor.submit(
                    self._process_asset_request,
                    **dict(
                        asset=asset,
                        from_date=update_statistics[asset.unique_identifier],
                        to_date=last_available_value,
                    )
                )
                for asset in tqdm(update_statistics.asset_list, desc="Submitting Assets to Thread Pool")
            ]

            for future in tqdm(as_completed(futures), total=len(futures), desc="Fetching Data"):
                try:
                    bar_set_requests = future.result()
                    if bar_set_requests is None:
                        continue
                    if len(bar_set_requests) == 0:
                        continue  # Skip if no data was fetched
                    # Filter the results based on the timestamp

                    # Append the result to bars_request_df
                    bars_request_df.append(bar_set_requests)

                except Exception as e:
                    self.logger.exception(f"An error occurred while processing results: {e}")

        # Postprocessing and duplicate checks
        if len(bars_request_df) == 0:
            return pd.DataFrame()

        bars_request_df = pd.concat(bars_request_df, axis=0)
        bars_request_df = bars_request_df.reset_index()
        bars_request_df = bars_request_df.rename(columns={"time_index": "open_time"})
        bars_request_df['calendar'] = bars_request_df['unique_identifier'].map(self.asset_calendar_map)

        new_bars = []
        if self.frequency_id == "1d":
            for calendar_name in bars_request_df['calendar'].unique():
                calendar = calendars[calendar_name]
                asset_bars_for_calendar = bars_request_df[bars_request_df['calendar'] == calendar_name].copy()

                if asset_bars_for_calendar.empty:
                    continue

                min_date, max_date = asset_bars_for_calendar["open_time"].min(), asset_bars_for_calendar[
                    "open_time"].max()
                full_schedule = calendar.schedule(start_date=min_date.date(), end_date=max_date.date())

                schedule_for_mapping = full_schedule.copy()
                schedule_for_mapping.index = schedule_for_mapping.index.tz_localize('UTC')
                market_close_map = schedule_for_mapping['market_close']

                asset_bars_for_calendar["day_key"] = asset_bars_for_calendar["open_time"].dt.normalize()
                asset_bars_for_calendar["time_index"] = asset_bars_for_calendar["day_key"].map(market_close_map)
                asset_bars_for_calendar.dropna(subset=['time_index'], inplace=True)
                new_bars.append(asset_bars_for_calendar)

            if not new_bars:
                return pd.DataFrame()

            bars_request_df = pd.concat(new_bars, axis=0, ignore_index=True)
            bars_request_df = bars_request_df.set_index(["unique_identifier", "time_index"])

        else:
            raise NotImplementedError(f"Frequency '{self.frequency_id}' not implemented for market close alignment.")

        bars_request_df = bars_request_df.swaplevel()
        bars_request_df = bars_request_df.drop(columns=["day_key", "rtype"])
        bars_request_df["open_time"] = bars_request_df["open_time"].apply(lambda dt: dt.timestamp())

        # filter out duplicates for assets
        for unique_identifier, last_update in update_statistics.items():
            bars_request_df = bars_request_df[
                (
                        (bars_request_df.index.get_level_values("unique_identifier") == unique_identifier) &
                        (bars_request_df.index.get_level_values("time_index") > last_update)
                )
                |
                (bars_request_df.index.get_level_values("unique_identifier") != unique_identifier)
            ]

        return bars_request_df

    def _run_post_update_routines(self, error_on_last_update: bool, update_statistics: "UpdateStatistics"):
        """
        Registers the time series in the backend after a successful update.
        """
        if error_on_last_update:
            self.logger.warning("Do not register data source due to error during run")
            return

        if self.metadata is not None:
            if not self.metadata.protect_from_deletion:
                self.local_persist_manager.protect_from_deletion()

        if self.use_vam_assets:
            markets_time_series_identifier = f"databento_{self.dataset}_{self.frequency_id}_bars".lower().replace(".",
                                                                                                                  "_")
