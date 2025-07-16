from mainsequence.tdag.time_series import TimeSerie, WrapperTimeSerie, ModelList
from mainsequence.client import DataUpdates, AssetCurrencyPair, Asset, DataFrequency, MARKETS_CONSTANTS, AssetCategory
from alpaca.data.requests import CryptoBarsRequest
from alpaca.data.historical import CryptoHistoricalDataClient


from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestQuoteRequest, StockBarsRequest
from alpaca.data.enums import Adjustment
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
import numpy as np
from typing import List
import pandas_market_calendars as mcal
import os
import pytz
import datetime
import pandas as pd
import gc
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback
from typing import Union
from ...utils import NAME_US_EQUITY_MARKET_CAP_TOP100, get_stock_assets, register_mts_in_backed

ALPACA_API_KEY = os.environ.get('ALPACA_API_KEY', None)
ALPACA_SECRET_KEY = os.environ.get('ALPACA_SECRET_KEY', None)
EQUITIES_TYPE=[ MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_COMMON_STOCK,
                   MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_ETP
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

class AlpacaEquityBars(TimeSerie):
    """
    Integration for Alpaca Data Series
    """
    FREQ_TO_TIMEFRAME_UNIT = {
        "m": TimeFrameUnit.Minute,
        "h": TimeFrameUnit.Hour,
        "d": TimeFrameUnit.Day,
        "w": TimeFrameUnit.Week,
        "mo": TimeFrameUnit.Month,
    }

    def __init__(self, asset_list: Union[ModelList,None], frequency_id: str,
                 adjustment: str, local_kwargs_to_ignore: List[str] = ["asset_list"], *args, **kwargs):
        """

        Args:
            asset:
            frequency_id:
            *args:
            **kwargs:
        """
        if frequency_id not in {freq.value for freq in DataFrequency}:
            raise AssertionError(f"Invalid frequency_id: {frequency_id}")

        self.frequency_id = frequency_id
        self.asset_list = asset_list
        self.use_vam_assets = False
        if self.asset_list is None:
            self.use_vam_assets = True
        self.timeframe = self._get_timeframe()
        self.adjustment = Adjustment(adjustment)

        super().__init__(*args, **kwargs)

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

    def _process_asset_request(self, asset, max_per_asset_symbol, last_available_value, client, calendars):
        if client is None:
            return None

        symbol = asset.ticker
        symbol_latest_value = max_per_asset_symbol
        if symbol_latest_value >= last_available_value:
            return None

        start_year = symbol_latest_value.year
        end_year = last_available_value.year
        years = range(start_year, end_year + 1)

        asset_calendar = calendars[self.asset_calendar_map[asset.unique_identifier]]

        # Prepare the requests to send later in the thread pool
        requests = []
        for year in tqdm(years, desc=f"Processing from {symbol_latest_value} to {last_available_value} for {symbol}",
                         leave=False):
            current_start = max(symbol_latest_value, datetime.datetime(year, 1, 1, tzinfo=pytz.utc))
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

    def _get_asset_list(self) -> Union[None, list]:
        if self.asset_list is None:
            assets = get_stock_assets()

            alpaca_assets = Asset.filter(
                main_sequence_share_class__in=[
                    a.main_sequence_share_class for a in assets
                ],
                execution_venue__symbol=MARKETS_CONSTANTS.ALPACA_EV_SYMBOL,
            )
            self.asset_list = alpaca_assets

        self.asset_calendar_map = {a.unique_identifier: a.get_calendar().name for a in self.asset_list}
        return self.asset_list

    def update(self, update_statistics: DataUpdates, **class_arguments):
        TIMEDELTA_MAP = {
            TimeFrameUnit.Minute.value: "minutes",
            TimeFrameUnit.Hour.value: "hours",
            TimeFrameUnit.Day.value: "days",
        }
        calendars = {str(cal): mcal.get_calendar(cal.replace("ARCA", "XNYS").replace("AMEX","XNYS")) for cal in np.unique(list(self.asset_calendar_map.values()))}

        last_available_value = datetime.datetime.now(pytz.utc).replace(hour=0, minute=0, second=0,
                                                                       microsecond=0) - datetime.timedelta(minutes=1)

        # check which assets need to be updated
        if update_statistics.is_empty():
            calendar_max_closes = {}
            for cal_name, cal in calendars.items():
                full_schedule = cal.schedule(last_available_value-datetime.timedelta(days=7), last_available_value) #cal.schedule(*min_max)
                full_schedule.iloc[-1]["market_close"]
                calendar_max_closes[cal_name] = full_schedule.iloc[-1]["market_close"]

            min_max = [update_statistics.get_min_latest_value(), datetime.datetime.now(pytz.utc).replace(hour=0, minute=0, second=0,)]
            for cal_name, cal in calendars.items():
                full_schedule = cal.schedule(*min_max)
                full_schedule.iloc[-1]["market_close"]
                calendar_max_closes[cal_name] = full_schedule.iloc[-1]["market_close"]

            asset_identifier_to_update = []
            for a in update_statistics.asset_list:
                if update_statistics[a.unique_identifier] < calendar_max_closes[self.asset_calendar_map[a.unique_identifier]]:
                    asset_identifier_to_update.append(a.unique_identifier)
                    continue

            if len(asset_identifier_to_update) == 0:
                self.logger.info(f"Nothing to update, prices not yet available in calendars {calendar_max_closes.keys()}")
                return pd.DataFrame()

            update_statistics = update_statistics.update_assets(update_statistics.asset_list)

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
                        max_per_asset_symbol=update_statistics[asset.unique_identifier],
                        last_available_value=last_available_value,
                        client=self.get_client_for_asset(asset),
                        calendars=calendars
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
                        continue  # Skip if no data was fetche
                    # Filter the results based on the timestamp

                    # Append the result to bars_request_df
                    bars_request_df.extend(bar_set_requests)

                except Exception as e:
                    self.logger.error(f"An error occurred while processing results: {e}")
                    self.logger.error("Stack Trace: %s", traceback.format_exc())

        # Postprocessing and duplicate checks
        if len(bars_request_df) == 0:
            return pd.DataFrame()

        bars_request_df = pd.concat(bars_request_df, axis=0)
        bars_request_df = bars_request_df.reset_index("timestamp")
        bars_request_df = bars_request_df.rename(columns={"timestamp": "open_time"})

        new_bars = []
        if self.timeframe.unit == TimeFrameUnit.Day:
            for calendar_name in bars_request_df.calendar.unique():
                calendar = calendars[calendar_name]
                full_schedule = calendar.schedule(
                    bars_request_df["open_time"].min(),
                    bars_request_df["open_time"].max()
                )

                # Merge full_schedule with bars_request_df based on the open_time to align the times
                asset_bars = bars_request_df[
                    bars_request_df.calendar == calendar_name].reset_index()  # Extract the bars for the asset
                try:
                    asset_bars["day"] = asset_bars["open_time"].dt.date
                except Exception as e:
                    raise e
                # Merge the calendar schedule with the actual bars data on open_time
                asset_bars["time_index"] = asset_bars["day"].map(full_schedule["market_close"].to_dict())
                new_bars.append(asset_bars)

            bars_request_df = pd.concat(new_bars, axis=0, ignore_index=True)
            bars_request_df = bars_request_df.drop(columns=["day"])
            del new_bars
            gc.collect()
            bars_request_df = bars_request_df.set_index("unique_identifier")
        elif self.timeframe.unit == TimeFrameUnit.Minute:
            td_kwargs = {TIMEDELTA_MAP[self.timeframe.unit.value]: self.timeframe.amount}
            bars_request_df["time_index"] = bars_request_df["open_time"] + datetime.timedelta(**td_kwargs)
        else:
            raise NotImplementedError

        bars_request_df = bars_request_df.drop(columns=["calendar", "symbol"])
        bars_request_df = bars_request_df.set_index("time_index", append=True)
        bars_request_df.index.names = ["unique_identifier", "time_index"]

        if bars_request_df.shape[0] > 0:
            bars_request_df["open_time"] = bars_request_df["open_time"].astype(np.int64)

        bars_request_df = bars_request_df.swaplevel()
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

    def _run_post_update_routines(self, error_on_last_update,update_statistics:DataUpdates):
        super().run_after_post_init_routines()

        if self.metadata is None:
            return None

        if not self.metadata.protect_from_deletion:
            self.local_persist_manager.protect_from_deletion()

        if error_on_last_update:
            self.logger.warning("Do not register data source due to error during run")
            return

        if self.use_vam_assets == True:
            markets_time_series_identifier = f"alpaca_{self.frequency_id}_bars"
            markets_time_serie = register_mts_in_backed(
                unique_identifier=markets_time_series_identifier,
                time_serie=self,
                description=f"Alpaca {self.frequency_id} Bars",
                data_frequency_id=self.frequency_id,
                asset_list=update_statistics.asset_list
            )

