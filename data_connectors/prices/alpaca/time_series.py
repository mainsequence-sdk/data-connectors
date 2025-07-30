from mainsequence.tdag.time_series import TimeSerie, WrapperTimeSerie
from mainsequence.client import UpdateStatistics, AssetCurrencyPair, Asset, DataFrequency, MARKETS_CONSTANTS, AssetCategory
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
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback
from typing import Union
from ...utils import NAME_US_EQUITY_MARKET_CAP_TOP100, get_stock_assets, register_mts_in_backed
import mainsequence.client as ms_client

ALPACA_API_KEY = os.environ.get('ALPACA_API_KEY', None)
ALPACA_SECRET_KEY = os.environ.get('ALPACA_SECRET_KEY', None)
EQUITIES_TYPE = [
    MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_COMMON_STOCK,
    MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_ETP
]

class AlpacaEquityBars(TimeSerie):
    """
    Fetches historical market data (bars) for stocks and cryptocurrencies from the Alpaca API.
    """
    BATCH_UPDATE_DAYS = 365 * 2  # Fetch data in chunks of up to 2 years
    LAST_AVAILABLE_DAYS = 1      # Data is available up to the previous day

    FREQ_TO_TIMEFRAME_UNIT = {
        "m": TimeFrameUnit.Minute,
        "h": TimeFrameUnit.Hour,
        "d": TimeFrameUnit.Day,
        "w": TimeFrameUnit.Week,
        "mo": TimeFrameUnit.Month,
    }

    def __init__(self, asset_list: Optional[List], frequency_id: str,
                 adjustment: str, local_kwargs_to_ignore: List[str] = ["asset_list"], *args, **kwargs):
        if frequency_id not in {freq.value for freq in DataFrequency}:
            raise AssertionError(f"Invalid frequency_id: {frequency_id}")

        self.frequency_id = frequency_id
        self.asset_list = asset_list
        self.use_vam_assets = self.asset_list is None
        self.timeframe = self._get_timeframe()
        self.adjustment = Adjustment(adjustment)

        super().__init__(*args, **kwargs)

    def dependencies(self):
        return {}

    def _get_timeframe(self):
        import re
        match = re.match(r"(\d+)([a-z]+)", self.frequency_id, re.IGNORECASE)
        if not match:
            raise ValueError(f"Invalid frequency_id format: {self.frequency_id}")

        frequency_amount = int(match.group(1))
        frequency_unit = match.group(2).lower()

        if frequency_unit not in self.FREQ_TO_TIMEFRAME_UNIT:
            raise ValueError(f"Unsupported frequency unit: {frequency_unit}")

        return TimeFrame(amount=frequency_amount, unit=self.FREQ_TO_TIMEFRAME_UNIT[frequency_unit])

    def get_asset_list(self) -> Union[None, list]:
        if self.asset_list is None:
            assets = get_stock_assets()
            self.asset_list = Asset.filter(
                main_sequence_share_class__in=[a.main_sequence_share_class for a in assets],
                execution_venue__symbol=MARKETS_CONSTANTS.ALPACA_EV_SYMBOL,
            )

        self.asset_calendar_map = {a.unique_identifier: a.get_calendar().name for a in self.asset_list}
        return self.asset_list

    def get_client_for_asset(self, asset):
        if asset.security_type in EQUITIES_TYPE:
            return StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
        elif asset.security_type == MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_CRYPTO:
            return CryptoHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
        else:
            self.logger.error(f"Security Type {asset.security_type} for asset {asset} is not implemented - asset will not be updated")
            return None

    def _calculate_update_ranges(self, update_statistics: UpdateStatistics) -> Dict[str, Tuple[datetime.datetime, datetime.datetime]]:
        """Determines the date range to fetch for each asset."""
        update_ranges = {}
        last_available_date = (datetime.datetime.now(pytz.utc).date() - datetime.timedelta(days=self.LAST_AVAILABLE_DAYS))

        for asset in update_statistics.asset_list:
            uid = asset.unique_identifier
            last_update = update_statistics.get_last_update_index_2d(uid)
            start_date = last_update if last_update else self.OFFSET_START

            end_date = min(
                start_date + datetime.timedelta(days=self.BATCH_UPDATE_DAYS),
                datetime.datetime.combine(last_available_date, datetime.datetime.max.time(), tzinfo=pytz.utc)
            )

            if end_date > start_date:
                update_ranges[uid] = (start_date, end_date)

        return update_ranges

    def _process_asset_request(self, asset: Asset, start_date: datetime.datetime, end_date: datetime.datetime, client) -> Optional[pd.DataFrame]:
        """Fetches bar data for a single asset over a specified date range."""
        if client is None:
            return None

        if start_date >= end_date:
            return None

        symbol = asset.ticker
        if asset.security_type in EQUITIES_TYPE:
            request_params = StockBarsRequest(
                symbol_or_symbols=symbol,
                timeframe=self.timeframe,
                start=start_date,
                end=end_date,
                adjustment=self.adjustment
            )
            bar_set = client.get_stock_bars(request_params)
        elif asset.security_type == MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_CRYPTO:
            crypto_symbol = Asset.get_properties_from_unique_symbol(symbol)["symbol"]
            request_params = CryptoBarsRequest(
                symbol_or_symbols=crypto_symbol,
                timeframe=self.timeframe,
                start=start_date,
                end=end_date,
            )
            bar_set = client.get_crypto_bars(request_params)
        else:
            raise NotImplementedError(f"Asset type {asset.security_type} not implemented")

        result_df = bar_set.df
        if result_df.empty:
            return None

        result_df["calendar"] = self.asset_calendar_map[asset.unique_identifier]
        result_df["unique_identifier"] = asset.unique_identifier
        return result_df

    def _fetch_data_concurrently(self, update_ranges: Dict[str, Tuple[datetime.datetime, datetime.datetime]]) -> pd.DataFrame:
        """Uses a thread pool to fetch data for all specified assets based on date ranges."""
        all_bars = []
        workers = int(os.environ.get("ALPACA_MAX_WORKERS", 5))
        asset_map = {a.unique_identifier: a for a in self.asset_list}

        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_uid = {
                executor.submit(
                    self._process_asset_request,
                    asset=asset_map.get(uid),
                    start_date=start,
                    end_date=end,
                    client=self.get_client_for_asset(asset_map.get(uid))
                ): uid
                for uid, (start, end) in update_ranges.items() if asset_map.get(uid)
            }

            for future in tqdm(as_completed(future_to_uid), total=len(future_to_uid), desc="Fetching Data"):
                uid = future_to_uid[future]
                try:
                    asset_bars_df = future.result()
                    if asset_bars_df is not None and not asset_bars_df.empty:
                        all_bars.append(asset_bars_df)
                except Exception:
                    self.logger.error(f"A request for asset {uid} failed in the thread pool.", exc_info=True)

        return pd.concat(all_bars, axis=0) if all_bars else pd.DataFrame()

    def _align_timestamps(self, bars_df: pd.DataFrame, update_statistics: UpdateStatistics, calendars: dict) -> pd.DataFrame:
        """Processes the raw DataFrame to align timestamps, set index, and filter duplicates."""
        TIMEDELTA_MAP = {
            TimeFrameUnit.Minute.value: "minutes",
            TimeFrameUnit.Hour.value: "hours",
            TimeFrameUnit.Day.value: "days",
        }

        bars_df = bars_df.reset_index("timestamp").rename(columns={"timestamp": "open_time"})

        new_bars = []
        if self.timeframe.unit == TimeFrameUnit.Day:
            for calendar_name, group in bars_df.groupby('calendar'):
                calendar = calendars.get(calendar_name)
                if calendar is None: continue

                schedule = calendar.schedule(start_date=group["open_time"].min().date(), end_date=group["open_time"].max().date())
                market_close_map = schedule.set_index(schedule.index.date)['market_close'].to_dict()

                group["time_index"] = group["open_time"].dt.date.map(market_close_map)
                new_bars.append(group)

            bars_df = pd.concat(new_bars, axis=0, ignore_index=True).dropna(subset=['time_index'])

        elif self.timeframe.unit in [TimeFrameUnit.Minute, TimeFrameUnit.Hour]:
            td_kwargs = {TIMEDELTA_MAP[self.timeframe.unit.value]: self.timeframe.amount}
            bars_df["time_index"] = bars_df["open_time"] + datetime.timedelta(**td_kwargs)
        else:
            raise NotImplementedError(f"Timestamp alignment for {self.timeframe.unit} not implemented.")

        bars_df = bars_df.drop(columns=["calendar", "symbol", "day"], errors='ignore').set_index(["time_index", "unique_identifier"])
        if "open_time" in bars_df.columns:
            bars_df["open_time"] = bars_df["open_time"].astype(np.int64)

        for uid, last_update in update_statistics.items():
            if last_update:
                mask = (bars_df.index.get_level_values("unique_identifier") == uid) & \
                       (bars_df.index.get_level_values("time_index") <= last_update)
                bars_df = bars_df[~mask]

        return bars_df

    def update(self, update_statistics: UpdateStatistics):
        """[Core Logic] Fetches new bar data from the Alpaca API."""
        if update_statistics.is_empty():
            update_statistics = update_statistics.update_assets(self.asset_list, init_fallback_date=self.OFFSET_START)

        update_ranges = self._calculate_update_ranges(update_statistics)
        if not update_ranges:
            self.logger.info("Nothing to update, all assets are up to date.")
            return pd.DataFrame()

        calendars = {str(cal): mcal.get_calendar(cal.replace("ARCA", "XNYS").replace("AMEX", "XNYS")) for cal in
                     np.unique(list(self.asset_calendar_map.values()))}

        bars_request_df = self._fetch_data_concurrently(update_ranges)
        if bars_request_df.empty:
            self.logger.info("No new bars were returned from the API.")
            return pd.DataFrame()

        return self._align_timestamps(bars_request_df, update_statistics, calendars)

    def get_table_metadata(self, update_statistics) -> ms_client.TableMetaData:
        if self.use_vam_assets:
            TS_ID = f"alpaca_{self.frequency_id}_bars"
            return ms_client.TableMetaData(
                identifier=TS_ID,
                description=f"Alpaca {self.frequency_id} Bars",
                data_frequency_id=self.frequency_id,
            )
        return None

    def _run_post_update_routines(self, error_on_last_update, update_statistics: UpdateStatistics):
        super().run_after_post_init_routines()
        if self.metadata is None:
            return

        if not self.metadata.protect_from_deletion:
            self.local_persist_manager.protect_from_deletion()

        if error_on_last_update:
            self.logger.warning("Do not register data source due to error during run")