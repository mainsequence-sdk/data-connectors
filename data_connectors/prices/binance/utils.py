from mainsequence.client import MARKETS_CONSTANTS
from mainsequence.client import MARKETS_CONSTANTS as CONSTANTS
from mainsequence.logconf import logger
import datetime
import pandas as pd
import pytz
from tqdm import tqdm
import os


from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen
from urllib.error import HTTPError
import gc
import time
import os
import numpy as np
from mainsequence.tdag.config import configuration
import pandas as pd
import requests
import zipfile
import io
tqdm.pandas()
BINANCE_API_KEY = os.getenv('BINANCE_API_KEY')
BINANCE_API_SECRET = os.getenv('BINANCE_API_SECRET')
BINANCE_TEST_NET_API_KEY = os.getenv('BINANCE_TEST_NET_API_KEY')
BINANCE_TEST_NET_API_SECRET = os.getenv('BINANCE_TEST_NET_API_SECRET')


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



#====BAR building=====






def get_binance_bars_endpoint(market_type, is_daily, symbol, interval):
    """
    Returns the appropriate Binance endpoint for bar data (candlesticks) based on market type and frequency.

    Args:
        market_type (str): "spot" or "futures" or "usdm".
        is_daily (bool): If True, uses daily endpoints; otherwise monthly.
        symbol (str): The trading pair symbol (e.g., "BTCUSDT").
        interval (str): The candlestick interval (e.g., "1m", "1h", "1d").

    Returns:
        str: The constructed URL endpoint.
    """
    # Define root URLs
    SPOT_DAY_ROOT_URL = "https://data.binance.vision/data/spot/daily/klines/"
    SPOT_MONTH_ROOT_URL = "https://data.binance.vision/data/spot/monthly/klines/"
    FUTURES_USDM_DAY_ROOT_URL = "https://data.binance.vision/data/futures/um/daily/klines/"
    FUTURES_USDM_MONTH_ROOT_URL = "https://data.binance.vision/data/futures/um/monthly/klines/"

    # Select the correct base URL
    if market_type == MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_GENERIC_CURRENCY_FUTURE:
        root_url = FUTURES_USDM_MONTH_ROOT_URL
        if is_daily:
            root_url = FUTURES_USDM_DAY_ROOT_URL
    else:
        root_url = SPOT_MONTH_ROOT_URL
        if is_daily:
            root_url = SPOT_DAY_ROOT_URL

    # Construct the full endpoint (ending with / )
    endpoint = f"{root_url}{symbol.upper()}/{interval}/"
    return endpoint


def fetch_binance_bars_for_single_day(
    market_type: str,
    is_daily: bool,
    symbol: str,
    interval: str,
    single_day: datetime,
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
    # 1) Construct the base endpoint
    endpoint_base = get_binance_bars_endpoint(market_type, is_daily, symbol, interval)

    # 2) Extract year, month, day for the filename
    year_str = single_day.strftime("%Y")
    month_str = single_day.strftime("%m")
    day_str = single_day.strftime("%d")

    # 3) Prepare the ZIP filename (Binance daily naming convention)
    #    e.g., BTCUSDT-1m-2023-06-01.zip
    filename_zip = f"{symbol.upper()}-{interval}-{year_str}-{month_str}"

    if is_daily:
        filename_zip = f"{filename_zip}-{day_str}.zip"
    else:
        filename_zip = f"{filename_zip}.zip"
    url = f"{endpoint_base}{filename_zip}"

    df_day = pd.DataFrame()  # in case of error or no data
    max_trials=3
    trial=0
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
            trial=trial+1
            if trial >= max_trials:
                logger.warning(
                    f"No data for {symbol} on {single_day.date()}. "
                    f"Status code: {resp.status_code} | URL: {url} trial{trial}/{max_trials} "
                )
                return df_day


        # The CSV is inside the ZIP, so open the ZIP in memory
        z = zipfile.ZipFile(io.BytesIO(resp.content))
        filename_csv = filename_zip.replace(".zip", ".csv")

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

def set_ohlc(ohlc_df):
    all_dfs = ohlc_df.set_index("bar_id")
    all_dfs = all_dfs.sort_index()
    all_dfs.index.name = "time"
    all_dfs = all_dfs.tz_localize("UTC")

    return all_dfs

BINANCE_TRADES_COLUMN_MAP={"a": "trade_id", "p": "price", "q": "qty", "T": "time", "m": "isBuyerMaker",
                                 "M": "isBestMatch"}
class FakeLogger:
    def info(self,msg):
        print(msg)
    def exception(self,msg):
        print(msg)
    def error(self,msg):
        print(msg)


def merge_partial_agg(global_agg, partial_agg):
    """
    Merges partial_agg DataFrame (grouped by bar_id) into global_agg dictionary.
    """
    for bar_id, row in partial_agg.iterrows():
        if bar_id not in global_agg:
            # Initialize a new entry
            global_agg[bar_id] = {
                'open': row['open'],
                'close': row['close'],
                'high': row['high'],
                'low': row['low'],
                'volume': row['volume'],
                'total_quote_volume': row['total_quote_volume'],
                'sum_price_qty': row['sum_price_qty'],
                'sum_buyer_maker_price_qty': row['sum_buyer_maker_price_qty'],
                'sum_buyer_maker': row['sum_buyer_maker'],
                'first_trade_time': row['first_trade_time'],
                'last_trade_time': row['last_trade_time']
            }
        else:
            # Merge with existing aggregator entry
            agg = global_agg[bar_id]

            # Price-based
            # open is the open from whichever chunk had the earliest first_trade_time
            if row['first_trade_time'] < agg['first_trade_time']:
                agg['open'] = row['open']
                agg['first_trade_time'] = row['first_trade_time']
            # close is from whichever chunk had the latest last_trade_time
            if row['last_trade_time'] > agg['last_trade_time']:
                agg['close'] = row['close']
                agg['last_trade_time'] = row['last_trade_time']

            # High/low are straightforward
            agg['high'] = max(agg['high'], row['high'])
            agg['low'] = min(agg['low'], row['low'])

            # Additive measures
            agg['volume'] += row['volume']
            agg['total_quote_volume'] += row['total_quote_volume']
            agg['sum_price_qty'] += row['sum_price_qty']
            agg['sum_buyer_maker_price_qty'] += row['sum_buyer_maker_price_qty']
            agg['sum_buyer_maker'] += row['sum_buyer_maker']


def get_bars_by_date(url, file_root, frequency_to_seconds, bars_frequency,
                     api_source: str, logger=None, data_df=None,
                     chunksize=1_000_000):
    """
    Reads data from local CSV dump or from remote, in chunks, and aggregates to bar level.
    By default, uses chunksize=200,000 for demonstration.
    """
    if logger is None:
        logger = FakeLogger()

    FUTURES_COLUMNS = ["trade_id", "price", "qty", "quoteQty", "time", "isBuyerMaker"]
    SPOT_COLUMNS = ["trade_id", "price", "qty", "quoteQty", "time", "isBuyerMaker", "isBestMatch"]



    logger.info(f"building ...{url}")
    start_time = time.time()

    # Instead of reading the entire CSV into memory, we will read in chunks.
    # We'll store partial aggregator in a dictionary keyed by bar_id
    global_agg = {}

    try:
        # 1.1) If we have a local copy, read it chunk by chunk



        # 1.2) If there's no local CSV, fetch from remote, decompress, read in chunks
        tries = 0
        MAX_TRIES, SECONDS_WAIT = 3, .5
        while True:
            try:
                resp = urlopen(url)
                break
            except HTTPError as e:
                if e.code == 404:
                    raise e
                tries += 1
                print(f"Try number {tries}, backoff {SECONDS_WAIT} seconds: {e}")
                time.sleep(SECONDS_WAIT)
                if tries >= MAX_TRIES:
                    raise e

        with ZipFile(BytesIO(resp.read())) as zipfile:
            with zipfile.open(file_root + ".csv") as foofile:
                # We read from the in-memory file object in chunks
                for chunk in tqdm(pd.read_csv(foofile, header=None, low_memory=False, chunksize=chunksize),desc="reading chunks"):
                    # Same transformations as above
                    if api_source in [CONSTANTS.ASSET_TYPE_CRYPTO_SPOT, CONSTANTS.ASSET_TYPE_CURRENCY_PAIR]:
                        chunk.columns = SPOT_COLUMNS
                        chunk.drop(columns=["trade_id", "isBestMatch"], inplace=True)
                    elif api_source == CONSTANTS.ASSET_TYPE_CRYPTO_USDM:
                        chunk.columns = FUTURES_COLUMNS
                        chunk.drop(columns=["trade_id"], inplace=True)
                    else:
                        raise NotImplementedError

                    if isinstance(chunk.iloc[0, 0], str):
                        if not chunk.iloc[0, 0].isdigit():
                            chunk = chunk.iloc[1:].copy()

                    for col in chunk.columns:
                        if col in ["price", "qty", "quoteQty"]:
                            chunk[col] = chunk[col].astype("float32", errors='ignore')
                    chunk["time"] = chunk["time"].astype(int, errors='ignore')
                    if chunk["isBuyerMaker"].dtype == 'object':
                        chunk["isBuyerMaker"] = chunk["isBuyerMaker"].astype(bool)
                    chunk["isBuyerMaker"] = chunk["isBuyerMaker"].astype(int)

                    chunk['time'] = pd.to_datetime(chunk['time'], unit='ms', errors='coerce')
                    chunk.dropna(subset=['time'], inplace=True)
                    chunk.sort_values('time', inplace=True)

                    if bars_frequency == "1m":
                        chunk["bar_id"] = chunk["time"].dt.floor("min")
                    elif bars_frequency in ["1days", "1d"]:
                        chunk["bar_id"] = chunk["time"].dt.floor("D")
                    else:
                        raise NotImplementedError(f"{bars_frequency} not supported")

                    chunk['price_qty'] = chunk['price'] * chunk['qty']
                    chunk['buyer_maker_price_qty'] = chunk['price'] * chunk['qty'] * chunk['isBuyerMaker']
                    chunk['qty_maker'] = chunk['qty'] * chunk['isBuyerMaker']

                    partial_agg = chunk.groupby('bar_id').agg(
                        open=('price', 'first'),
                        close=('price', 'last'),
                        high=('price', 'max'),
                        low=('price', 'min'),
                        volume=('qty', 'sum'),
                        total_quote_volume=('quoteQty', 'sum'),
                        sum_price_qty=('price_qty', 'sum'),
                        sum_buyer_maker_price_qty=('buyer_maker_price_qty', 'sum'),
                        sum_buyer_maker=('qty_maker', 'sum'),
                        first_trade_time=('time', 'min'),
                        last_trade_time=('time', 'max'),
                    )

                    merge_partial_agg(global_agg, partial_agg)



    except HTTPError:
        logger.error(f"{url} Not found")
        return pd.DataFrame()
    except Exception as e:
        logger.exception(str(e))
        raise e

    # -- 2) Convert global_agg (dict) into a DataFrame
    if not global_agg:
        # If somehow empty, return empty DF
        return pd.DataFrame()

    ohlc = pd.DataFrame.from_dict(global_agg, orient='index')
    ohlc.index.name = 'bar_id'
    ohlc.sort_index(inplace=True)

    # -- 3) Compute final vwap columns
    ohlc['vwap'] = ohlc['sum_price_qty'] / ohlc['volume']
    ohlc['vwap_buyer_maker'] = (
            ohlc['sum_buyer_maker_price_qty'] / ohlc['sum_buyer_maker']
    ).fillna(0)

    # -- 4) Drop helper columns
    ohlc.drop(
        ['sum_price_qty', 'sum_buyer_maker_price_qty', 'sum_buyer_maker'],
        axis=1,
        inplace=True
    )

    # -- 5) Shift bar_id by frequency_to_seconds if needed
    ohlc = ohlc.reset_index(drop=False)
    ohlc["open_time"] = ohlc["bar_id"]
    ohlc["bar_id"] = ohlc["bar_id"] + datetime.timedelta(seconds=frequency_to_seconds)

    logger.info(f"--- {url} Completed in {time.time() - start_time:.2f} seconds ---")
    return ohlc
                
            




