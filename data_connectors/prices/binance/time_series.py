from mainsequence.tdag.time_series import TimeSerie
from .utils import (   get_bars_by_date_optimized,
                    get_information_bars
                    )
from ..utils import transform_frequency_to_seconds

from joblib import delayed, Parallel
from tqdm import tqdm
import os
import numpy as npA
import pandas as pd
import datetime
import pytz
from typing import Union, List, Optional, Dict, Any, Tuple
import requests
import time

from mainsequence.client import (MARKETS_CONSTANTS,
                                 DoesNotExist, Asset, AssetCurrencyPair, AssetFutureUSDM, DataFrequency,
                                AssetCategory
                                 )
from abc import ABC, abstractmethod

import mainsequence.client as ms_client
import copy
import logging
from pydantic import BaseModel
from ...utils import has_sufficient_memory
import dataclasses
import functools
import zipfile
import io


# Binance API credentials
BINANCE_API_KEY = os.environ.get('BINANCE_API_KEY', None)
BINANCE_API_SECRET = os.environ.get('BINANCE_API_SECRET', None)


@functools.lru_cache(maxsize=None)
def fetch_spot_symbols_dict():
    """
    Fetches the entire spot exchangeInfo once and returns a dict:
        {symbol_name: status, ...}
    """
    url = "https://api.binance.com/api/v3/exchangeInfo"
    resp = requests.get(url)
    resp.raise_for_status()  # raise an HTTPError if bad status
    data = resp.json()

    # Build a lookup dict of symbol -> status
    return {s["symbol"]: s["status"] for s in data.get("symbols", [])}

@functools.lru_cache(maxsize=None)
def fetch_futures_symbols_dict():
    """
    Fetches the entire USDM futures exchangeInfo once and returns a dict:
        {symbol_name: status, ...}
    """
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()

    # Build a lookup dict of symbol -> status
    return {s["symbol"]: s["status"] for s in data.get("symbols", [])}

def timestamp_to_utc_datetime(x):
    return datetime.datetime.utcfromtimestamp(x * 1e-3).replace(tzinfo=pytz.utc)

def binance_time_stamp_to_datetime(series:pd.Series)->pd.Series:
    return series.apply(lambda x: datetime.datetime.utcfromtimestamp(x * 1e-3))

class NoDataInURL(Exception):
    pass

class BarConfiguration(BaseModel):
    """Abstract base class for bar construction configurations."""
    pass

class TimeBarConfig(BarConfiguration):
    """Configuration for standard time-aggregated bars."""
    frequency_id: str

class ImbalanceBarConfig(BarConfiguration):
    """
    Configuration for information-driven bars based on the tick rule.
    """
    ema_alpha: float = 0.001
    warmup_bars: int = 20
    warmup_lookahead_trades: int = 50000


@dataclasses.dataclass
class BinanceConfig:
    """Holds all configuration for Binance data sources."""
    SPOT_API_URL: str = "https://api.binance.com"
    FUTURES_API_URL: str = "https://fapi.binance.com"
    SPOT_DAY_ROOT_URL: str = "https://data.binance.vision/data/spot/daily/trades/"
    SPOT_MONTH_ROOT_URL: str = "https://data.binance.vision/data/spot/monthly/trades/"
    FUTURES_USDM_DAY_ROOT_URL: str = "https://data.binance.vision/data/futures/um/daily/trades/"
    FUTURES_USDM_MONTH_ROOT_URL: str = "https://data.binance.vision/data/futures/um/monthly/trades/"
    MEMORY_THRESHOLD: float = 0.6

CONFIG = BinanceConfig()


logger = logging.getLogger(__name__)

class NoDataInURL(Exception):
    pass

class BaseBinanceEndpoint(TimeSerie):
    """Base class for fetching historical data from Binance."""
    OFFSET_START = datetime.datetime(2017, 1, 1, tzinfo=pytz.utc)

    def __init__(
            self,
            asset_list: Optional[List[Asset]],
            bar_configuration: BarConfiguration,
            local_kwargs_to_ignore: List[str] = ["asset_list","asset_category_identifier"],
            asset_category_identifier: Optional[str] = None,
            *args,
            **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.asset_list = asset_list
        if asset_category_identifier is not None:
            assert self.asset_list is None, "asset list should be empty if using an asset category identifier"
        self.asset_category_identifier = asset_category_identifier
        self.bar_configuration = bar_configuration
        self.info_map: Dict[str, dict] = {}

        if self.asset_list is not None:
            self._init_info_map(self.asset_list)

    def dependencies(self) -> Dict:
        return {}

    def _init_info_map(self, asset_list: list[Asset]):
        """
        Creates a mapping from unique_identifier to Binance-specific info.
        This is the CORRECT implementation provided by the user.
        """
        from mainsequence.client import MARKETS_CONSTANTS as CONSTANTS

        info_map = {}
        for asset in asset_list:
            # Correctly determine the symbol based on security type
            if asset.security_type == CONSTANTS.FIGI_SECURITY_TYPE_GENERIC_CURRENCY_FUTURE:
                binance_symbol = asset.currency_pair.current_snapshot.ticker
                assert asset.maturity_code == "PERPETUAL"
            else:  # Assumes spot otherwise
                binance_symbol = f"{asset.base_asset.current_snapshot.ticker}{asset.quote_asset.current_snapshot.ticker}"

            info_map[asset.unique_identifier] = {
                "binance_symbol": binance_symbol,
                "api_source": asset.security_type  # Store the specific security type
            }
        self.info_map = info_map

    def get_asset_list(self):
        if self.asset_list is None:
            # get them through main sequence figi class and exchange
            target_category = ms_client.AssetCategory.get(unique_identifier=self.asset_category_identifier)
            asset_list = ms_client.Asset.filter(id__in=target_category.assets)
            # return binance_futures+binance_currency_pairs
            self.logger.warning("Only using currency pair for now due to timeouts in updating - add futures later")
            return asset_list

        return self.asset_list

    def _get_url_and_root_for_day(self, symbol_info: dict, day: datetime.datetime) -> tuple[str, str]:
        """Constructs the download URL and file root for a specific day's trades."""
        is_daily = (datetime.datetime.now().date() - day.date()).days <= 33
        api_source = symbol_info["api_source"]
        binance_symbol = symbol_info["binance_symbol"]

        if api_source == MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_GENERIC_CURRENCY_FUTURE:
            root_url = CONFIG.FUTURES_USDM_DAY_ROOT_URL if is_daily else CONFIG.FUTURES_USDM_MONTH_ROOT_URL
        else:
            root_url = CONFIG.SPOT_DAY_ROOT_URL if is_daily else CONFIG.SPOT_MONTH_ROOT_URL

        date_str = day.strftime("%Y-%m-%d") if is_daily else day.strftime("%Y-%m")
        file_root = f"{binance_symbol}-trades-{date_str}"
        url = f"{root_url}{binance_symbol}/{file_root}.zip"
        return url, file_root

    def update(self, update_statistics: ms_client.UpdateStatistics) -> pd.DataFrame:
        """Main update orchestrator."""
        if not self.info_map:
            self._init_info_map(update_statistics.asset_list)

        # 1. Get active assets and their historical start dates
        active_uids, start_date_map = self._get_active_assets_and_start_dates(update_statistics)
        if not active_uids:
            logger.warning("No active assets found to update.")
            return pd.DataFrame()

        # 2. Calculate the required update range for each asset
        update_ranges = self._calculate_update_ranges(update_statistics, start_date_map)

        # 3. Fetch data in parallel
        all_bars = self._run_parallel_fetch(update_ranges)
        if all_bars.empty:
            return pd.DataFrame()

        return all_bars

    def _get_active_assets_and_start_dates(self, update_statistics) -> Tuple[List[str], Dict[str, datetime.date]]:
        """Filters for tradable assets and finds their inception date on Binance."""

        futures_status = fetch_futures_symbols_dict()
        spot_status = fetch_spot_symbols_dict()

        active_uids = []
        start_date_map = {}

        for asset in tqdm(update_statistics.asset_list, desc="Checking asset status"):
            uid = asset.unique_identifier
            info = self.info_map.get(uid)
            if not info:
                continue

            # Check trading status
            is_future = info["api_source"] == MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_GENERIC_CURRENCY_FUTURE
            status_dict = futures_status if is_future else spot_status
            if status_dict.get(info["binance_symbol"]) != "TRADING":
                logger.debug(f"Skipping {uid} ({info['binance_symbol']}) - not in TRADING status.")
                continue

            # Find first trade date from Binance API to avoid 404s on historical data
            endpoint = CONFIG.FUTURES_API_URL if is_future else CONFIG.SPOT_API_URL
            api_call="fapi" if is_future else "api"
            url = f'{endpoint}/{api_call}/v1/klines?symbol={info["binance_symbol"]}&interval=1d&limit=1&startTime=0'
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                first_trade_ms = response.json()[0][0]
                start_date_map[uid] = datetime.datetime.fromtimestamp(first_trade_ms / 1000, tz=pytz.utc).date()
                active_uids.append(uid)
            except (requests.RequestException, IndexError) as e:
                logger.warning(f"Could not get start date for {uid} ({info['binance_symbol']}): {e}")

        return active_uids, start_date_map

    def _calculate_update_ranges(self, update_statistics, start_date_map) -> Dict:
        """Determines the date range to fetch for each asset."""
        update_ranges = {}
        last_available_date = (
                    datetime.datetime.now(pytz.utc) - datetime.timedelta(days=self.LAST_AVAILABLE_DAYS)).date()

        for uid, historical_start in start_date_map.items():
            last_update = update_statistics.get_last_update_index_2d(uid).date()
            start_date = max(historical_start, last_update)

            # Apply batching limit to avoid huge downloads
            end_date = min(start_date + datetime.timedelta(days=self.BATCH_UPDATE_DAYS), last_available_date)

            if end_date > start_date:
                update_ranges[uid] = pd.date_range(start_date, end_date, freq='D')

        return update_ranges

    def _run_parallel_fetch(self, update_ranges: Dict[str, pd.DatetimeIndex]) -> pd.DataFrame:
        """Orchestrates fetching data in parallel using the defined fetcher strategy."""
        if not update_ranges:
            return pd.DataFrame()

        # Simple memory check
        n_jobs = min(os.cpu_count() or 1, 5)  # Limit jobs to avoid rate limits / DB issues

        if not has_sufficient_memory(CONFIG.MEMORY_THRESHOLD):
            logger.warning("Memory usage too high, use sequential fetch.")
            n_jobs = 1

        results = Parallel(n_jobs=n_jobs)(
            delayed(self._process_single_asset)(uid, date_range,self.logger)
            for uid, date_range in update_ranges.items()
        )

        valid_results = [df for df in results if not df.empty]
        return pd.concat(valid_results, axis=0) if valid_results else pd.DataFrame()

    def _process_single_asset(self, uid: str, date_range: pd.DatetimeIndex,logger) -> pd.DataFrame:
        """
        To be implemented by subclasses. This is the core method that defines
        how data for a single asset over a date range is fetched and processed.
        """
        raise NotImplementedError

    def run_post_update_routines(self, error_on_last_update: bool, update_statistics: ms_client.UpdateStatistics):
        """Common post-update routines for all Binance TimeSeries."""
        if self.metadata is None:
            return
        if not self.metadata.protect_from_deletion:
            self.local_persist_manager.protect_from_deletion()
        if error_on_last_update:
            logger.warning("Not performing post-update tasks due to error during run.")
            return

        # Append assets to a VAM or other registry if needed
        # asset_id_list = [a.id for a in update_statistics.asset_list]
        # self.vam_bar_source.append_assets(asset_id_list=asset_id_list)
        logger.info(f"Post-update routines completed for {self.__class__.__name__}.")


# ==============================================================================
# 4. LEAN AND EXTENSIBLE TIMESERIE CLASSES
# ==============================================================================

class BinanceHistoricalBars(BaseBinanceEndpoint):
    """
    TimeSerie for fetching aggregated bars directly from the Binance API.
    """
    BATCH_UPDATE_DAYS = 30 * 12 * 8
    LAST_AVAILABLE_DAYS = 1
    def __init__(
            self,
            *args,
            **kwargs
    ):

        super().__init__(*args, **kwargs        )
        assert isinstance(self.bar_configuration, TimeBarConfig)

    def get_table_metadata(self, update_statistics) -> Optional[ms_client.TableMetaData]:
        is_dynamic = self.asset_list is None
        if is_dynamic and self.bar_configuration.frequency_id != "1m":
            identifier = f"binance_{self.bar_configuration.frequency_id}_bars"
            return ms_client.TableMetaData(
                identifier=identifier,
                description=f"Binance {self.bar_configuration.frequency_id} bars, does not include vwap",
                data_frequency_id=DataFrequency(self.bar_configuration.frequency_id),
            )
        return None

    @staticmethod
    def fetch_binance_bars_for_single_day(
            url:str,symbol:str,
            single_day: datetime,logger
    ) -> pd.DataFrame:
        """
        Combines the endpoint construction + single-day extraction logic into one function.

        Args:
            market_type (str): "spot" or "usdm" or "futures".
            is_daily (bool): True if daily endpoints should be used, otherwise monthly.
            symbol (str): e.g., "BTCUSDT".
            interval (str): e.g., "1m", "1h", "1d".
            single_day (datetime): The date to fetch; only year-month-day is used for the filename.
            logger (logging.Logger): Logger instance for warnings/errors.

        Returns:
            pd.DataFrame: Candlestick data for the given day, or an empty DataFrame if not found.
        """

        extract_filename = lambda url: url.split('/')[-1] if '/' in url else url

        df_day = pd.DataFrame()  # in case of error or no data
        max_trials = 3
        trial = 0
        try:
            while trial < max_trials:
                resp = requests.get(url)
                if resp.status_code == 200:
                    break

                logger.debug(
                    f"No data for {symbol} on {single_day.date()}. "
                    f"Status code: {resp.status_code} | URL: {url} trial{trial}/{max_trials} "
                )
                time.sleep(10)
                trial = trial + 1
                if trial >= max_trials:
                    logger.warning(
                        f"No data for {symbol} on {single_day.date()}. "
                        f"Status code: {resp.status_code} | URL: {url} trial{trial}/{max_trials} "
                    )
                    return df_day

            # The CSV is inside the ZIP, so open the ZIP in memory
            z = zipfile.ZipFile(io.BytesIO(resp.content))
            filename_csv = extract_filename(url).replace(".zip",".csv")

            with z.open(filename_csv) as csv_file:
                # Binance Klines CSV columns:
                # 0: open_time (ms)
                # 1: open
                # 2: high
                # 3: low
                # 4: close
                # 5: volume
                # 6: close_time (ms)
                # 7: quote_asset_volume
                # 8: number_of_trades
                # 9: taker_buy_base_asset_volume
                # 10: taker_buy_quote_asset_volume
                # 11: ignore
                df_day = pd.read_csv(csv_file, header=None)

                def safe_to_numeric(series):
                    """
                    Attempts to convert a Series to numeric.
                    If it fails, it returns the original unconverted Series.
                    """
                    try:
                        return pd.to_numeric(series)
                    except (ValueError, TypeError):
                        return series

                # sometimes headers are present
                if isinstance(df_day.iloc[0, 0], str):
                    df_day = df_day.iloc[1:]
                    df_day = df_day.apply(safe_to_numeric)

                df_day.columns = [
                    "open_time",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "close_time",
                    "quote_asset_volume",
                    "number_of_trades",
                    "taker_buy_base_asset_volume",
                    "taker_buy_quote_asset_volume",
                    "ignore"
                ]

            # 4) Convert millisecond timestamps to UTC datetime
            if len(df_day) > 0:
                for time_col in ["open_time", "close_time"]:
                    if df_day[time_col].iloc[0] > 2e14:  # ~2e14 = 200,000,000,000,000 => definitely microseconds
                        df_day[time_col] //= 1000

            df_day["close_time"] = pd.to_datetime(df_day["close_time"], unit="ms", utc=True)
            df_day = df_day.drop(columns=["ignore"])
            # 5) Set the 'open_time' as index (optional)

        except Exception as e:
            logger.error(f"Error downloading or reading bars for {symbol} on {single_day.date()}: {e}")
            return pd.DataFrame()

        return df_day

    @staticmethod
    def get_binance_bars_url(market_type, dump_frequency_daily, symbol, interval, date: datetime):
        """
        Constructs the full Binance .zip file URL for Kline data.

        Args:
            market_type (str): "spot" or "futures" or "usdm".
            is_daily (bool): True for daily data folder (used by Binance), else monthly.
            symbol (str): Trading pair symbol (e.g., "BTCUSDT").
            interval (str): Kline interval (e.g., "1d", "1h").
            date (datetime): Date object indicating which month to retrieve.

        Returns:
            str: Full URL to the .zip file (e.g., .../BTCUSDT-1d-2019-09.zip)
        """
        # Normalize inputs
        symbol = symbol.upper()
        interval = interval.lower()
        yyyy_mm = date.strftime("%Y-%m")

        # Define root URLs
        base_url = "https://data.binance.vision/data"
        if market_type == MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_GENERIC_CURRENCY_FUTURE:

            root = "futures/um"
        else:

            root = "spot"

        folder = "daily" if dump_frequency_daily else "monthly"
        if dump_frequency_daily:
            date_str = date.strftime("%Y-%m-%d")
            file_extension = ".zip"
        else:
            date_str = date.strftime("%Y-%m")
            file_extension = ".zip"

        # Construct file name and full URL
        file_name = f"{symbol}-{interval}-{date_str}{file_extension}"
        url = f"{base_url}/{root}/{folder}/klines/{symbol}/{interval}/{file_name}"
        return url

    def _process_single_asset(self, uid: str, date_range: pd.DatetimeIndex,logger) -> pd.DataFrame:
        """Fetches pre-aggregated klines for each day in the date range."""
        all_dfs = []
        symbol_info = self.info_map[uid]
        frequency_id = self.bar_configuration.frequency_id

        dump_frequency_daily = self.bar_configuration.frequency_id != "1d"

        if dump_frequency_daily == False:
            date_range = [d.replace(day=1) for d in date_range]
            date_range = set(date_range)

        for day in tqdm(date_range, desc=f"Fetching klines for {uid}", leave=False):
            url = self.get_binance_bars_url(
                market_type=symbol_info["api_source"],
                dump_frequency_daily=dump_frequency_daily,
                symbol=symbol_info["binance_symbol"],
                date=day,
                interval=frequency_id
            )
            try:
                daily_df = self.fetch_binance_bars_for_single_day(
                    url=url,
                    symbol=symbol_info["binance_symbol"],
                    single_day=day,logger=logger,
                )
                if not daily_df.empty:
                    daily_df = daily_df.set_index("close_time")
                    all_dfs.append(daily_df)
            except NoDataInURL:
                logger.debug(f"No kline data URL for {uid} on {day.date()}.")
            except Exception as e:
                logger.error(f"Error fetching klines for {uid} on {day.date()}: {e}")

        if not all_dfs: return pd.DataFrame()

        asset_df = pd.concat(all_dfs, axis=0)
        asset_df.index.name = "time_index"
        asset_df["unique_identifier"] = uid
        return asset_df.set_index("unique_identifier", append=True)


class BinanceBarsFromTrades(BaseBinanceEndpoint):
    """
    TimeSerie for fetching raw trades from Binance and aggregating them into 1-minute bars.
    """
    BATCH_UPDATE_DAYS = 365
    LAST_AVAILABLE_DAYS = 1
    def __init__(
            self,
            *args,
            **kwargs
    ):
        # Frequency is fixed to 1 minute for this data source
        super().__init__(*args, **kwargs     )

    def _get_url_and_root_for_chunk(self, symbol_info: dict, chunk_date: datetime.datetime) -> tuple[str, str]:
        """Constructs the download URL and file root for a specific chunk's trades."""
        is_daily = (datetime.datetime.now().date() - chunk_date.date()).days <= 33
        api_source = symbol_info["api_source"]
        binance_symbol = symbol_info["binance_symbol"]

        if api_source == MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_GENERIC_CURRENCY_FUTURE:
            root_url = CONFIG.FUTURES_USDM_DAY_ROOT_URL if is_daily else CONFIG.FUTURES_USDM_MONTH_ROOT_URL
        else:
            root_url = CONFIG.SPOT_DAY_ROOT_URL if is_daily else CONFIG.SPOT_MONTH_ROOT_URL

        date_str = chunk_date.strftime("%Y-%m-%d") if is_daily else chunk_date.strftime("%Y-%m")
        file_root = f"{binance_symbol}-trades-{date_str}"
        url = f"{root_url}{binance_symbol}/{file_root}.zip"
        return url, file_root

    def _process_single_asset(self, uid: str, date_range: pd.DatetimeIndex,logger) -> pd.DataFrame:
        """
        Orchestrates fetching raw trades and aggregating them into bars based on the bar_config.
        """
        all_dfs = []
        info_bar_state = None
        symbol_info = self.info_map[uid]

        # Intelligent Chunking Logic
        processing_chunks = []
        processed_months = set()
        for day in date_range:
            is_daily = (datetime.datetime.now().date() - day.date()).days <= 33
            if is_daily:
                processing_chunks.append(day)
            else:
                month_key = day.strftime("%Y-%m")
                if month_key not in processed_months:
                    processing_chunks.append(day)
                    processed_months.add(month_key)

        for chunk_date in tqdm(processing_chunks, desc=f"Processing {uid}", leave=False):
            url, file_root = self._get_url_and_root_for_chunk(symbol_info, chunk_date)

            if isinstance(self.bar_configuration, TimeBarConfig):

                tmp_bars = get_bars_by_date_optimized(
                    url=url, file_root=file_root, api_source=symbol_info["api_source"],
                    bars_frequency=self.bar_configuration.frequency_id,logger=logger
                )
                COLUMNS = ["open", "high", "low", "volume", "close", "vwap", "open_time",
                           ]
                tmp_bars=tmp_bars[COLUMNS]
                if not tmp_bars.empty: all_dfs.append(tmp_bars)

            elif isinstance(self.bar_configuration, ImbalanceBarConfig):
                completed_bars, new_state = get_information_bars(
                    url=url, file_root=file_root, api_source=symbol_info["api_source"],
                    ema_alpha=self.bar_configuration.ema_alpha, warmup_bars=self.bar_configuration.warmup_bars,
                    warmup_lookahead_trades=self.bar_configuration.warmup_lookahead_trades,
                    previous_day_state=info_bar_state,logger=logger
                )
                COLUMNS = {
                    'open': "The price of the very first trade that occurred within the bar.",
                    'high': "The highest trade price observed during the formation of the bar.",
                    'low': "The lowest trade price observed during the formation of the bar.",
                    'close': "The price of the final trade that caused the bar to be completed.",
                    'volume': "The total sum of the quantity of all trades that make up the bar.",
                    'vwap': "The Volume-Weighted Average Price of all trades within the bar.",
                    'open_time': "The timestamp of the first trade in the bar. Note that the DataFrame's index is the bar's closing time.",
                    'imbalance_at_close': "The final value of the cumulative signed volume (buy volume - sell volume) at the exact moment the bar was formed. This is the value that met or exceeded the dynamic threshold, triggering the bar's creation."
                }
                completed_bars=completed_bars[list(COLUMNS.keys())]
                if not completed_bars.empty: all_dfs.append(completed_bars)
                info_bar_state = new_state

        if not all_dfs: return pd.DataFrame()

        asset_df = pd.concat(all_dfs, axis=0)

        asset_df["unique_identifier"] = uid
        return asset_df.set_index("unique_identifier", append=True)