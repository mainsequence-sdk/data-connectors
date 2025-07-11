from mainsequence.tdag.time_series import TimeSerie, ModelList
from .utils import  get_bars_by_date, set_ohlc,fetch_binance_bars_for_single_day
from ..utils import transform_frequency_to_seconds

from joblib import delayed, Parallel
from tqdm import tqdm
import os
import numpy as npA
import pandas as pd
import datetime
import pytz
from typing import Union, List
import requests
import calendar
import json

from mainsequence.client import (MARKETS_CONSTANTS,
                                 DoesNotExist, Asset, AssetCurrencyPair, AssetFutureUSDM, DataFrequency,
                                AssetCategory
                                 )
import copy
import logging

from ...utils import has_sufficient_memory, NAME_CRYPTO_MARKET_CAP_TOP100, register_mts_in_backed, \
    NAME_CRYPTO_MARKET_CAP_TOP50, NAME_CRYPTO_MARKET_CAP_TOP10
from .utils import fetch_futures_symbols_dict, fetch_spot_symbols_dict

# Binance API credentials
BINANCE_API_KEY = os.environ.get('BINANCE_API_KEY', None)
BINANCE_API_SECRET = os.environ.get('BINANCE_API_SECRET', None)

def get_date_format(date: datetime.datetime) -> str:
    return date.strftime("%d %b, %Y")

class NoDataInURL(Exception):
    pass

SPOT_API_URL = "https://api.binance.com"  # Spot endpoint
FUTURES_API_URL = "https://fapi.binance.com"  # Futures USD-M endpoint
# Root URLs for Binance data
SPOT_DAY_ROOT_URL = "https://data.binance.vision/data/spot/daily/trades/"
SPOT_MONTH_ROOT_URL = "https://data.binance.vision/data/spot/monthly/trades/"
FUTURES_USDM_DAY_ROOT_URL = "https://data.binance.vision/data/futures/um/daily/trades/"
FUTURES_USDM_MONTH_ROOT_URL = "https://data.binance.vision/data/futures/um/monthly/trades/"

EV_FRE_URL_MAP =  {
            "daily": {
                MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_CRYPTO: SPOT_DAY_ROOT_URL,
                MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_GENERIC_CURRENCY_FUTURE: FUTURES_USDM_DAY_ROOT_URL
            },
            "monthly": {
                MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_CRYPTO: SPOT_MONTH_ROOT_URL,
                MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_GENERIC_CURRENCY_FUTURE: FUTURES_USDM_MONTH_ROOT_URL
            }
        }

logger = logging.getLogger(__name__)

def get_url( is_daily: bool, api_source: str) -> str:
    return EV_FRE_URL_MAP["daily"][api_source] if is_daily else EV_FRE_URL_MAP["monthly"][api_source]

def get_file_root(binance_symbol: str, date: datetime.datetime, is_daily: bool) -> str:
    date_str = date.strftime("%Y-%m-%d") if is_daily else date.strftime("%Y-%m")
    return f"{binance_symbol}-trades-{date_str}"

def get_target_url_by_day(date, is_daily, api_source, binance_symbol):
    root = get_url(is_daily=is_daily, api_source=api_source)
    return root + binance_symbol + "/" + get_file_root(binance_symbol,date, is_daily) + ".zip"

def _parallel_bar_update(info_map, frequency_to_seconds, frequency_id,
                         is_daily, time_range, latest_value,
                         unique_identifier:str,bars_source):
    global logger
    api_source = info_map[unique_identifier]["api_source"]
    binance_symbol = info_map[unique_identifier]["binance_symbol"]

    if bars_source == "binance_bars":

        all_dfs = [fetch_binance_bars_for_single_day(
            market_type=api_source,
            is_daily=is_daily,
            symbol=binance_symbol,
            interval=frequency_id,
            single_day=day,

            ) for day in tqdm(time_range,desc=f"{unique_identifier}: {time_range[0]}-{time_range[-1]}")]
    elif bars_source == "binance_trades":
        all_dfs = [get_bars_by_date(
            url=get_target_url_by_day(date=day, is_daily=is_daily, api_source=api_source,
                                      binance_symbol=binance_symbol),
            file_root=get_file_root(date=day, is_daily=is_daily, binance_symbol=binance_symbol),
            api_source=api_source,
            frequency_to_seconds=frequency_to_seconds,
            bars_frequency=frequency_id
        ) for day in tqdm(time_range,desc=f"{unique_identifier}: {time_range[0]}-{time_range[-1]}")]
    else:
        raise NotImplementedError
    try:
        all_dfs = pd.concat(all_dfs, axis=0, ignore_index=True)
    except Exception as e:
        raise e

    if len(all_dfs) == 0:
        return pd.DataFrame()

    if  bars_source == "binance_trades":
        raise Exception("next minute forward not implemented")
        all_dfs = set_ohlc(ohlc_df=all_dfs)
        if all_dfs.shape[0] == 0:
            logger.warning(f"{unique_identifier} No more data after {time_range[0]}")
    else:
        all_dfs["close_time"]=all_dfs["close_time"].dt.floor(freq="1min")+datetime.timedelta(seconds=60)
        all_dfs = all_dfs.set_index("close_time")

    return all_dfs

def _process_symbol_update(
    unique_identifier,
    start_date_for_update,
    is_daily,
    last_date_available,
    LAST_AVAILABLE_DAYS,
    bars_source: str,
    logger_context: str,
    info_map: dict,
    frequency_to_seconds: int,
    frequency_id: str,
    batch_update_days: int,
):
    global logger
    """Process one (symbol, execution_venue_symbol) pair and return bars DataFrame."""

    if not has_sufficient_memory(0.6):
        logger.warning(f"Memory usage too high, stopping the update for {unique_identifier}.")
        return pd.DataFrame()

    update_prices = False

    if (datetime.datetime.now(tz=pytz.utc).date() - start_date_for_update).days > LAST_AVAILABLE_DAYS:
        update_prices = True
        assert LAST_AVAILABLE_DAYS >= 1

    all_dfs = pd.DataFrame()
    if update_prices:
        freq = "1d" if is_daily else "1ME"  # or "1ME" if not is_daily else "1d"
        last_date_available_monthly = last_date_available
        date_seq = calendar._nextmonth(last_date_available_monthly.year, last_date_available_monthly.month)
        date_seq = datetime.date(date_seq[0], date_seq[1], 1)
        decrease_td= datetime.timedelta(days=LAST_AVAILABLE_DAYS) if is_daily else datetime.timedelta(days=0)
        date_seq = min(
            last_date_available -
            decrease_td,
            date_seq
        )
        time_range = pd.date_range(start_date_for_update, date_seq, freq=freq)

        if len(time_range) > 0:
            try:
                all_dfs = _parallel_bar_update(
                    time_range=time_range,
                    is_daily=is_daily,
                    latest_value=start_date_for_update,
                    unique_identifier=unique_identifier,
                    info_map=info_map,
                    frequency_to_seconds=frequency_to_seconds,
                    frequency_id=frequency_id,
                    bars_source=bars_source
                )
                last_update = all_dfs.index.max()
            except NoDataInURL:
                logger.exception(f"Not updating {unique_identifier} due to URL not found")
                return pd.DataFrame()

        # If we used monthly freq, but there's still daily data available that extends beyond last_update
        if freq == "1m" \
           and (last_update.date() < last_date_available) \
           and (last_date_available > last_date_available_monthly):
            logger.debug("Concatenating daily request")
            daily_time_range = pd.date_range(last_update.date(), last_date_available, freq="1d")
            daily_time_range = [c for c in daily_time_range if c < datetime.datetime.utcnow()]
            daily_dfs = _parallel_bar_update(
                time_range=daily_time_range,
                is_daily=True,    # daily
                unique_identifier=unique_identifier,
                latest_value=last_update
            )
            all_dfs = pd.concat([all_dfs, daily_dfs], axis=0)

        if all_dfs.shape[0] == 0:
            return pd.DataFrame()

        # Localize datetime columns
        for col in ["first_trade_time", "last_trade_time", "open_time"]:
            if col in all_dfs.columns:
                if all_dfs.dtypes[col] !="int64":
                    all_dfs[col] = all_dfs[col].dt.tz_localize(pytz.utc).astype(np.int64)

        # Deduplicate
        all_dfs = all_dfs[~all_dfs.index.duplicated(keep='last')]

        # Reindex into multi-index
        all_dfs = pd.concat([all_dfs], axis=0, keys=[unique_identifier])
        all_dfs.index.names = [ "unique_identifier", "time_index"]
        all_dfs = all_dfs.reorder_levels(["time_index", "unique_identifier"])

    return all_dfs


class BaseBinanceEndpoint:
    def __init__(
            self,
            asset_list: Union[ModelList,None],
            frequency_id: str,
            local_kwargs_to_ignore: List[str] = ["asset_list"],
            *args,
            **kwargs
    ):
        """
        Args:
            asset_list (ModelList): List of assets.
            frequency_id (str): ID of frequency (e.g., '1min').
            back_fill_futures_with_spot (bool): If True, futures are back-filled with spot prices.
            spot_pair (Union[str, None], optional): The spot pair. Defaults to None.
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.
        """
        super().__init__(*args, **kwargs)

        if frequency_id not in {freq.value for freq in DataFrequency}:
            raise AssertionError(f"Invalid frequency_id: {frequency_id}")

        self.asset_list = asset_list
        self.frequency_id = frequency_id
        self.frequency_to_seconds = transform_frequency_to_seconds(frequency_id=self.frequency_id)
        self.use_vam_assets = False
        if self.asset_list is None:
            self.use_vam_assets = True
        self.info_map=None
        if self.asset_list is not None:
            self._init_info_map(self.asset_list)

    def _init_info_map(self,asset_list:list[Asset]):
        from mainsequence.client import MARKETS_CONSTANTS as CONSTANTS

        info_map = {}
        for asset in asset_list:
            binance_symbol = (
                asset.currency_pair.ticker if asset.security_type == CONSTANTS.FIGI_SECURITY_TYPE_GENERIC_CURRENCY_FUTURE
                else
                f"{asset.base_asset.ticker}{asset.quote_asset.ticker}"

            )
            info_map[asset.unique_identifier] = {
                "binance_symbol": binance_symbol,
                "execution_venue_symbol": asset.execution_venue.symbol,
                "api_source": asset.security_type
            }
            if asset.security_type == CONSTANTS.FIGI_SECURITY_TYPE_GENERIC_CURRENCY_FUTURE:
                assert asset.maturity_code == "PERPETUAL"

        self.info_map = info_map


    def _override_end_value(
        self,
        start_date: datetime.datetime,
        last_date_available: datetime.datetime,
        unique_identifier: str
    ):
        override_end = None
        if (datetime.datetime.now(pytz.utc).date() - start_date).days > self.BATCH_UPDATE_DAYS:
            override_end = min(start_date + datetime.timedelta(days=self.BATCH_UPDATE_DAYS), last_date_available)
        if override_end is not None:
            self.logger.debug(f"Last possible update overridden for {unique_identifier} at {override_end}")
        return override_end

    def transform_frequency_to_seconds(self):
        return transform_frequency_to_seconds(frequency_id=self.frequency_id)

    def _get_required_cores(self,asset_list):
        if len(asset_list) > 5:
            return 5 # higher number may lead to data base connection errors
        return 1

    def run_after_post_init_routines(self):
        """
        Use post init routines to configure the time series
        """
        if hasattr(self,"metadata"):
            if not self.metadata.protect_from_deletion:
                self.local_persist_manager.protect_from_deletion()

    def get_updated_bars(self, update_statistics, LAST_AVAILABLE_DAYS):

        start_date_for_update_map = {}
        initial_start = copy.deepcopy(update_statistics.update_statistics)

        futures_status = fetch_futures_symbols_dict()
        spot_status = fetch_spot_symbols_dict()

        combos_to_pop = []
        for unique_identifier, symbol_config in tqdm(self.info_map.items(), desc="getting start dates"):
            start_date_for_update_map[unique_identifier] = update_statistics[unique_identifier].date()
            binance_symbol = symbol_config["binance_symbol"]

            if symbol_config["api_source"] == MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_GENERIC_CURRENCY_FUTURE:
                base_endpoint = f"{FUTURES_API_URL}/fapi/v1/klines"
                is_futures = True
            elif symbol_config["api_source"] == MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_CRYPTO:  # Default to spot
                base_endpoint = f"{SPOT_API_URL}/api/v1/klines"
                is_futures = False
            else:
                raise NotImplementedError

            if start_date_for_update_map[unique_identifier] > self.OFFSET_START.date():
                if binance_symbol not in futures_status and binance_symbol not in spot_status:
                    combos_to_pop.append(unique_identifier)

                if is_futures:
                    if futures_status[binance_symbol] != "TRADING":
                        combos_to_pop.append(unique_identifier)
                else:
                    if spot_status[binance_symbol] != "TRADING":
                        combos_to_pop.append(unique_identifier)
                continue

            response = requests.get(f'{base_endpoint}?symbol={binance_symbol}&interval=1m&startTime=0')
            if response.status_code != 200:
                combos_to_pop.append(unique_identifier)
                self.logger.warning(f"{unique_identifier} prices does not exist")
                continue

            first_trade = pd.DataFrame(response.json())
            start_date_for_update = datetime.datetime.utcfromtimestamp(
                first_trade[0].sort_values().iloc[0] / 1000).date()
            start_date_for_update_map[unique_identifier] = start_date_for_update

        for c in combos_to_pop:
            self.info_map.pop(c)
            start_date_for_update_map.pop(c)
        last_date_available = (datetime.datetime.now() - datetime.timedelta(days=LAST_AVAILABLE_DAYS)).date()

        override_end_value_map = {
            unique_identifier: self._override_end_value(
                start_date=start_date_for_update,
                last_date_available=last_date_available,
                unique_identifier=unique_identifier
            )
            for unique_identifier, start_date_for_update in start_date_for_update_map.items()
        }

        last_date_available_map = {
            s: min(_override_end_value, last_date_available) if _override_end_value is not None else last_date_available
            for s, _override_end_value in override_end_value_map.items()
        }

        is_daily_map = {
            symbol_combo: (datetime.datetime.now().date() - start_date_for_update_map[symbol_combo]).days <= 33
            for symbol_combo in start_date_for_update_map.keys()
        }

        extra_kwargs = dict(logger_context=self.logger._context,
                            batch_update_days=self.BATCH_UPDATE_DAYS,
                            frequency_id=self.frequency_id,
                            frequency_to_seconds=self.frequency_to_seconds,
                            info_map=self.info_map)

        n_jobs = self._get_required_cores(update_statistics.asset_list)
        # n_jobs=1
        # start_date_for_update_map={k:v for counter,(k,v) in enumerate(start_date_for_update_map.items()) if counter<1}

        all_symbol_bars = Parallel(n_jobs=n_jobs)(
            delayed(_process_symbol_update)(
                unique_identifier,
                start_date_for_update,
                is_daily_map[unique_identifier],
                last_date_available_map[unique_identifier],
                LAST_AVAILABLE_DAYS,
                bars_source=self.bars_source,
                **extra_kwargs
            )
            for unique_identifier, start_date_for_update in start_date_for_update_map.items()
        )

        # results is now a list of DataFrames; you can concat them
        all_symbol_bars = pd.concat(all_symbol_bars, axis=0) if any(
            not df.empty for df in all_symbol_bars) else pd.DataFrame()

        if all_symbol_bars.shape[0] == 0:
            return pd.DataFrame()

        all_symbol_bars = update_statistics.filter_df_by_latest_value(all_symbol_bars)
        return all_symbol_bars


    def _get_asset_list(self):
        if self.asset_list is None:
            # TODO make it work with more cryptos
            top_10_crypto = AssetCategory.get(source="coingecko",
                                               unique_identifier=NAME_CRYPTO_MARKET_CAP_TOP10.lower().replace(" ",
                                                                                                               "_"), )
            spot_assets = Asset.filter(id__in=top_10_crypto.assets)
            # get them through main sequence figi class and exchange
            binance_currency_pairs = AssetCurrencyPair.filter(
                base_asset__main_sequence_share_class__in=[a.main_sequence_share_class for a
                                                                         in spot_assets],
                execution_venue__symbol=MARKETS_CONSTANTS.BINANCE_EV_SYMBOL,
                quote_asset__ticker="USDT"
                )

            binance_futures = AssetFutureUSDM.filter(
                execution_venue__symbol=MARKETS_CONSTANTS.BINANCE_FUTURES_EV_SYMBOL,
                currency_pair__ticker__in=[a.ticker for a in binance_currency_pairs] + ["1000" + a.ticker for a in
                                                                                        binance_currency_pairs]

            )

            # return binance_futures+binance_currency_pairs
            self.logger.warning("Only using currency pair for now due to timeouts in updating - add futures later")
            return binance_currency_pairs

        return self.asset_list

    def update(self, update_statistics):
        if self.info_map is None:
            self._init_info_map(update_statistics.asset_list)

        all_symbol_bars = self.get_updated_bars(
            update_statistics=update_statistics,
            LAST_AVAILABLE_DAYS=self.LAST_AVAILABLE_DAYS,
        )

        assert all_symbol_bars.index.duplicated().sum()==0

        return all_symbol_bars

    def  _run_post_update_routines(self, error_on_last_update,update_statistics):
        asset_id_list=[a.id for a in update_statistics.asset_list]
        self.vam_bar_source.append_assets(asset_id_list=asset_id_list)

class BinanceHistoricalBars(BaseBinanceEndpoint, TimeSerie):
    BATCH_UPDATE_DAYS = 30 * 12 * 8
    LAST_AVAILABLE_DAYS = 1

    @TimeSerie._post_init_routines()
    def __init__(
            self,
            asset_list: ModelList,
            frequency_id: str,
            local_kwargs_to_ignore: List[str] = ["asset_list"],
            *args,
            **kwargs
    ):
        """
        Args:
            asset_list (ModelList): List of assets.
            frequency_id (str): ID of frequency (e.g., '1min').
            back_fill_futures_with_spot (bool): If True, futures are back-filled with spot prices.
            spot_pair (Union[str, None], optional): The spot pair. Defaults to None.
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.
        """
        super().__init__(asset_list=asset_list,
                         frequency_id=frequency_id,
                         local_kwargs_to_ignore=local_kwargs_to_ignore, *args, **kwargs)

        if self.frequency_id == "1m":
            self.logger.warning("Use BarsFromTrades for 1m data.")

        self.bars_source = "binance_bars"

    def  _run_post_update_routines(self, error_on_last_update,update_statistics):
        """
        Use post init routines to configure the time series
        """
        super().run_after_post_init_routines()

        if self.metadata is None:
            return None

        if not self.metadata.protect_from_deletion:
            self.local_persist_manager.protect_from_deletion()

        if error_on_last_update:
            self.logger.warning("Do not register data source due to error during run")
            return

        if self.use_vam_assets == True and self.frequency_id != "1m":
            markets_time_series_identifier = f"binance_{self.frequency_id}_bars"

            markets_time_serie = register_mts_in_backed(
                unique_identifier=markets_time_series_identifier,
                time_serie=self,
                description=f"Binance {self.frequency_id} bars, does not include vwap",
                asset_list=update_statistics.asset_list,
                data_frequency_id=self.frequency_id,

            )

class BinanceBarsFromTrades(BaseBinanceEndpoint, TimeSerie):
    """
    Creates Bars Aggregation in time from transactions
    """
    BATCH_UPDATE_DAYS = 365
    LAST_AVAILABLE_DAYS = 1
    @TimeSerie._post_init_routines()
    def __init__(
            self,
            asset_list: ModelList,
            local_kwargs_to_ignore: List[str] = ["asset_list"],
            *args,
            **kwargs
    ):
        """
        Args:
            asset_list (ModelList): List of assets.
            back_fill_futures_with_spot (bool): If True, futures are back-filled with spot prices.
            spot_pair (Union[str, None], optional): The spot pair. Defaults to None.
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.
        """

        frequency_id = "1m" # fixed for binance bars from trades
        super().__init__(asset_list=asset_list,
                         frequency_id=frequency_id,
                         local_kwargs_to_ignore=local_kwargs_to_ignore, *args, **kwargs)

        self.bars_source = "binance_trades"

    def _run_post_update_routines(self, error_on_last_update: bool,update_statistics ):
        """
        Use post init routines to configure the time series
        """
        super().run_after_post_init_routines()
        if error_on_last_update:
            self.logger.warning("Do not register data source due to error during run")
            return

        from mainsequence.client import HistoricalBarsSource
        raise NotImplementedError
        # register_mts_in_backed(
        #     time_serie=self,
        #     execution_venues_symbol=ASSETS_ORM_CONSTANTS.BINANCE_EV_SYMBOL,
        #     data_mode=ASSETS_ORM_CONSTANTS.DATA_MODE_BACKTEST,
        #     data_source_description=f"Historical bars from binance extracted from trades, include vwap",
        # )


    def update(self, update_statistics):
        df = super().update(update_statistics)
        if df.shape[0] > 1e7:
            self.local_persist_manager.metadata._drop_indices = True
            self.local_persist_manager.metadata._rebuild_indices = True

        return df
