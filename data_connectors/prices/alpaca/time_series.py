from mainsequence.tdag.data_nodes import DataNode, WrapperDataNode
from mainsequence.client import UpdateStatistics, AssetCurrencyPair, Asset, DataFrequency, MARKETS_CONSTANTS

import mainsequence.client as ms_client

from alpaca.data.requests import CryptoBarsRequest
from alpaca.data.historical import CryptoHistoricalDataClient


from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestQuoteRequest, StockBarsRequest
from alpaca.data.enums import Adjustment
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
import numpy as np
from typing import List, Optional, Tuple, Dict
import pandas_market_calendars as mcal
import os
import pytz
import datetime
import pandas as pd
import gc

from mainsequence.tdag import DataNode
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback
from typing import Union
from ...utils import NAME_US_EQUITY_MARKET_CAP_TOP100, get_stock_assets

ALPACA_API_KEY = os.environ.get('ALPACA_API_KEY', None)
ALPACA_SECRET_KEY = os.environ.get('ALPACA_SECRET_KEY', None)
EQUITIES_TYPE = [
    MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_COMMON_STOCK,
    MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_ETP,
    MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_REIT,
]


import requests

# url = f"https://data.alpaca.markets/v1/corporate-actions?symbols={symbol}&limit=1000&sort=asc"
# headers = {
#     "accept": "application/json",
#     "APCA-API-KEY-ID": ALPACA_API_KEY,
#     "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
#     "start": valid_start_time.strftime('%Y-%m-%d'),
#     "end": valid_end_time.strftime('%Y-%m-%d')
# }
# response = requests.get(url, headers=headers)

class AlpacaEquityBars(DataNode):
    """
  Fetches historical market data (bars) for stocks and cryptocurrencies from the Alpaca API.

    This class can be configured to fetch data for a specific list of assets or, if no list
    is provided, it will automatically discover all compatible assets available through Alpaca.
    It handles different data frequencies (e.g., minute, hour, day) and price adjustments.

    Attributes:
        frequency_id (str): The data frequency (e.g., '1m', '15m', '1d').
        asset_list (List | None): A specific list of assets to track, or None to track all.
        timeframe (TimeFrame): A parsed representation of the frequency.
        adjustment (Adjustment): The type of price adjustment to apply (e.g., splits, dividends).
    """
    FREQ_TO_TIMEFRAME_UNIT = {
        "m": TimeFrameUnit.Minute,
        "h": TimeFrameUnit.Hour,
        "d": TimeFrameUnit.Day,
        "w": TimeFrameUnit.Week,
        "mo": TimeFrameUnit.Month,
    }
    _ARGS_IGNORE_IN_STORAGE_HASH = ["asset_list"]
    def __init__(self, asset_list: Optional[List], frequency_id: str,
                 adjustment: str, *args, **kwargs):
        """

        Args:
        asset_list: A list of Asset models to fetch data for. If set to None, the time series
                    will fetch the asset according to the override get_asset_list_method
        frequency_id: The frequency of the data bars, in the format 'AmountUnit'
                      (e.g., '1m', '15m', '1h', '1d').
        adjustment: The type of price adjustment for equity bars. Defaults to "raw".
                    See Alpaca's documentation for options like 'split', 'dividend'.

        """
        if frequency_id not in {freq.value for freq in DataFrequency}:
            raise AssertionError(f"Invalid frequency_id: {frequency_id}")

        self.frequency_id = frequency_id
        self.asset_list = asset_list
        self.use_vam_assets = False

        self.timeframe = self._get_timeframe()
        self.adjustment = Adjustment(adjustment)

        super().__init__(*args, **kwargs)

    def dependencies(self):
        return {}

    def _get_timeframe(self):
        """
        Parses the frequency_id to extract the frequency amount and unit.

        Returns:
            tuple: (frequency_amount, frequency_unit)
        """
        import re

        # Regex to match a number followed by a unit
        match = re.match(r"(\d+)([a-z]+)", self.frequency_id, re.IGNORECASE)

        if not match:
            raise ValueError(f"Invalid frequency_id format: {self.frequency_id}")

        frequency_amount = int(match.group(1))
        frequency_unit = match.group(2).lower()  # Normalize to lowercase

        # Validate and map the unit using the predefined mapping
        if frequency_unit not in self.FREQ_TO_TIMEFRAME_UNIT:
            raise ValueError(f"Unsupported frequency unit: {frequency_unit}")

        return TimeFrame(amount=frequency_amount, unit=self.FREQ_TO_TIMEFRAME_UNIT[frequency_unit])

    def _process_asset_request(self, asset, last_update:datetime.datetime, last_available_value, client, calendars):
        if client is None:
            return None

        max_skip = last_update + datetime.timedelta(
            days=360)
        if max_skip < datetime.datetime.now(pytz.utc):
            last_available_value = max_skip


        symbol = asset.current_snapshot.ticker
        if last_update >= last_available_value:
            return None

        start_year = last_update.year
        end_year = last_available_value.year
        years = range(start_year, end_year + 1)

        asset_calendar = calendars[self.asset_calendar_map[asset.unique_identifier]]

        # Prepare the requests to send later in the thread pool
        requests = []
        for year in tqdm(years, desc=f"Processing from {last_update} to {last_available_value} for {symbol}",
                         leave=False):
            current_start = max(last_update, datetime.datetime(year, 1, 1, tzinfo=pytz.utc))
            current_end = min(last_available_value,
                              datetime.datetime(year, 12, 31, 23, 59, 59, 999999, tzinfo=pytz.utc))

            full_schedule = asset_calendar.schedule(current_start.date(), current_end.date())
            full_schedule = full_schedule[
                (full_schedule.market_open >= current_start) & (full_schedule.market_close <= current_end)]
            if full_schedule.shape[0] == 0:
                return None
            trading_days = asset_calendar.valid_days(start_date=current_start.date(), end_date=current_end.date())
            if len(trading_days) == 0:
                return None
            trading_days = trading_days.to_list()
            trading_days.extend([current_start, current_end])

            valid_start_time, valid_end_time = np.min(trading_days), np.max(trading_days)

            if current_start > current_end:
                continue

            if asset.security_type in EQUITIES_TYPE:
                # Prepare the request parameters
                request_params = StockBarsRequest(
                    symbol_or_symbols=symbol,
                    timeframe=self.timeframe,
                    start=valid_start_time,
                    end=valid_end_time,
                    adjustment=self.adjustment
                )
                bar_set = client.get_stock_bars(request_params)
            elif asset.security_type == MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_CRYPTO:
                crypto_symbol = Asset.get_properties_from_unique_symbol(symbol)["symbol"]
                request_params = CryptoBarsRequest(
                    symbol_or_symbols=crypto_symbol,
                    timeframe=self.timeframe,
                    start=valid_start_time,
                    end=valid_end_time,
                )

                bar_set = client.get_crypto_bars(request_params)
            else:
                raise NotImplementedError(f"Asset type {asset.security_type} not implemented")

            tmp_f = bar_set.df
            tmp_f = tmp_f[~tmp_f.index.duplicated(keep='first')]  #
            if len(tmp_f) == 0:
                self.logger.warning(f"Retrieved prices for symbol {symbol} from {valid_start_time} to {valid_end_time} are empty!")
            else:
                tmp_f = tmp_f[
                    (tmp_f.index.get_level_values("timestamp") > current_start) &
                    (tmp_f.index.get_level_values("timestamp") <= current_end)
                    ]
                tmp_f["calendar"] = self.asset_calendar_map[asset.unique_identifier]
                tmp_f["unique_identifier"] = asset.unique_identifier
                requests.append(tmp_f)

        return requests

    def get_client_for_asset(self, asset):


        if asset.security_type in EQUITIES_TYPE:
            client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
        elif asset.security_type  == MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_CRYPTO:
            client = CryptoHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
        else:
            self.logger.error(f"Security Type {asset.security_type} for asset {asset} is not implemented - asset will not be updated")
            return None
        return client

    def get_asset_list(self) -> Union[None, list]:
        if self.asset_list is None:
            assets = get_stock_assets()
            self.asset_list = assets

        self.asset_calendar_map = {a.unique_identifier: a.get_calendar().name for a in self.asset_list}
        return self.asset_list

    def _fetch_data_concurrently(self, update_statistics:UpdateStatistics,calendars) -> pd.DataFrame:
        """
        Uses a thread pool to fetch data for all specified assets from the Alpaca API.
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        bars_request_df = []  # Using the original variable name
        workers = int(os.environ.get("ALPACA_MAX_WORKERS", 5))
        workers=1
        # Calculate the lookback limit
        last_available_value = datetime.datetime.now(pytz.utc).replace(hour=0, minute=0, second=0) - datetime.timedelta(
            minutes=1)


        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [
                executor.submit(
                    self._process_asset_request,
                    asset=asset,
                    last_update=update_statistics.get_last_update_index_2d(asset.unique_identifier),
                    last_available_value=last_available_value,
                    client=self.get_client_for_asset(asset),
                    calendars=calendars
                )
                for asset in tqdm(update_statistics.asset_list, desc="Submitting Assets to Thread Pool")
            ]

            for future in tqdm(as_completed(futures), total=len(futures), desc="Fetching Data"):
                try:
                    bar_set_requests = future.result()
                    if bar_set_requests:
                        bars_request_df.extend(bar_set_requests)
                except Exception:
                    self.logger.error("A request failed in the thread pool.", exc_info=True)

        if not bars_request_df:
            return pd.DataFrame()

        return pd.concat(bars_request_df, axis=0)

    def _align_timestamps(self, bars_request_df: pd.DataFrame,
                          calendars: dict) -> pd.DataFrame:
        """
        Processes the raw DataFrame to align timestamps, set the correct index,
        and filter out any duplicate data points.
        """
        TIMEDELTA_MAP = {
            TimeFrameUnit.Minute.value: "minutes",
            TimeFrameUnit.Hour.value: "hours",
            TimeFrameUnit.Day.value: "days",
        }

        # Rename timestamp column and handle alignment based on frequency
        bars_request_df = bars_request_df.reset_index("timestamp").rename(columns={"timestamp": "open_time"})

        new_bars = []
        if self.timeframe.unit == TimeFrameUnit.Day:
            for calendar_name, group in bars_request_df.groupby('calendar'):
                calendar = calendars.get(calendar_name)
                if calendar is None: continue

                schedule = calendar.schedule(start_date=group["open_time"].min().date(),
                                             end_date=group["open_time"].max().date())
                market_close_map = schedule.set_index(schedule.index.date)['market_close'].to_dict()

                group["time_index"] = group["open_time"].dt.date.map(market_close_map)
                new_bars.append(group)

            bars_request_df = pd.concat(new_bars, axis=0, ignore_index=True).drop(columns=["day"], errors='ignore')

        elif self.timeframe.unit in [TimeFrameUnit.Minute, TimeFrameUnit.Hour]:
            td_kwargs = {TIMEDELTA_MAP[self.timeframe.unit.value]: self.timeframe.amount}
            bars_request_df["time_index"] = bars_request_df["open_time"] + datetime.timedelta(**td_kwargs)
        else:
            raise NotImplementedError(f"Timestamp alignment for {self.timeframe.unit} not implemented.")

        # Clean up and set final index
        bars_request_df = bars_request_df.drop(columns=["calendar", "symbol"], errors='ignore').set_index(
            ["time_index", "unique_identifier"])
        if "open_time" in bars_request_df.columns:
            bars_request_df["open_time"] = bars_request_df["open_time"].astype(np.int64)

        return bars_request_df

    def update(self):
        """
           [Core Logic] Fetches new bar data from the Alpaca API.

           This method orchestrates the entire update process:
           1. Determines the precise date range needed for each asset based on the last update.
           2. Uses a thread pool to fetch data for all required assets concurrently.
           3. For daily bars, it aligns the bar's timestamp to the official market close time
              for the respective exchange calendar.
           4. For minute/hour bars, it calculates the closing timestamp based on the bar's open time.
           5. Formats the combined data into a final DataFrame ready for persistence.
        """
        calendars = {str(cal): mcal.get_calendar(cal.replace("ARCA", "XNYS").replace("AMEX", "XNYS")) for cal in
                     np.unique(list(self.asset_calendar_map.values()))}
        bars_request_df = self._fetch_data_concurrently(self.update_statistics, calendars=calendars)
        if bars_request_df.empty:
            self.logger.info("No new bars were returned from the API.")
            return pd.DataFrame()

        # Step 3: Align timestamps and finalize the DataFrame.
        bars_request_df = self._align_timestamps(bars_request_df,calendars=calendars)
        return bars_request_df


    def get_table_metadata(self)->ms_client.TableMetaData:
        TS_ID = f"alpaca_{self.frequency_id}_bars"
        meta = ms_client.TableMetaData(
            identifier=TS_ID,
            description=f"Alpaca {self.frequency_id} Bars",
            data_frequency_id=self.frequency_id,
        )

        return meta

    def run_post_update_routines(self, error_on_last_update):
        super().run_after_post_init_routines()

        if self.metadata is None:
            return None

        if not self.metadata.protect_from_deletion:
            self.local_persist_manager.protect_from_deletion()

        if error_on_last_update:
            self.logger.warning("Do not register data source due to error during run")
            return

