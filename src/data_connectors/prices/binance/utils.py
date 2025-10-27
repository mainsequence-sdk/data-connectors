
from mainsequence.logconf import logger

from tqdm import tqdm
import os


from zipfile import ZipFile,BadZipFile

import time
import os
import numpy as np
import pandas as pd
import requests
import io
from requests.exceptions import RequestException
from numba import from_dtype
import numba
import os, io, time, tempfile


tqdm.pandas()

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


bar_dtype = np.dtype([
    ('open',  np.float64), ('close',  np.float64),
    ('high',  np.float64), ('low',    np.float64),
    ('volume',               np.float64),
    ('total_quote_volume',    np.float64),
    ('sum_price_qty',         np.float64),
    ('sum_buyer_maker_price_qty', np.float64),
    ('sum_buyer_maker',       np.float64),
    ('first_trade_time', np.int64), ('last_trade_time',  np.int64),
    ('first_trade_id',  np.int64),  ('last_trade_id',   np.int64),
])
bar_agg_type = from_dtype(bar_dtype)


@numba.njit
def aggregate_trades_chunk(
        bar_ids: np.ndarray,
        prices:  np.ndarray,
        qtys:    np.ndarray,
        quote_qtys: np.ndarray,
        times:   np.ndarray,
        is_buyer_maker: np.ndarray,
        trade_ids: np.ndarray,                           #  ← NEW
        bar_id_map,                                     # numba.typed.Dict[int, int]
        global_agg_values                               # np.ndarray of bar_dtype
):
    """
    Aggregate one chunk of trades into 1‑minute or 1‑day bars,
    resolving ties (same millisecond) with `trade_id`.
    """
    for i in range(bar_ids.shape[0]):
        bar_id   = bar_ids[i]
        price    = prices[i]
        qty      = qtys[i]
        quote_qty = quote_qtys[i]
        ts       = times[i]
        is_buyer = is_buyer_maker[i]
        tid      = trade_ids[i]                         #  ← NEW

        if bar_id not in bar_id_map:
            idx = len(bar_id_map)
            bar_id_map[bar_id] = idx

            # grow array if needed
            if idx >= global_agg_values.shape[0]:
                new_size = global_agg_values.shape[0] * 2
                tmp = np.zeros(new_size, dtype=global_agg_values.dtype)
                tmp[:global_agg_values.shape[0]] = global_agg_values
                global_agg_values = tmp

            g = global_agg_values[idx]
            g.open  = g.close = g.high = g.low = price
            g.volume = qty
            g.total_quote_volume = quote_qty
            g.sum_price_qty = price * qty
            g.sum_buyer_maker = qty * is_buyer
            g.sum_buyer_maker_price_qty = price * qty * is_buyer
            g.first_trade_time = g.last_trade_time = ts
            g.first_trade_id   = g.last_trade_id   = tid       #  ← NEW
            global_agg_values[idx] = g
        else:
            idx = bar_id_map[bar_id]
            g = global_agg_values[idx]

            # ----- High / Low (order‑independent) -------------------------
            if price > g.high: g.high = price
            if price < g.low:  g.low  = price

            # ----- Open (earliest)  --------------------------------------
            if (ts < g.first_trade_time or
               (ts == g.first_trade_time and tid < g.first_trade_id)):   #  ← NEW tie‑breaker
                g.open = price
                g.first_trade_time = ts
                g.first_trade_id   = tid

            # ----- Close (latest)  ---------------------------------------
            if (ts > g.last_trade_time or
               (ts == g.last_trade_time  and tid > g.last_trade_id)):     #  ← NEW tie‑breaker
                g.close = price
                g.last_trade_time = ts
                g.last_trade_id   = tid

            # ----- Additive metrics --------------------------------------
            g.volume              += qty
            g.total_quote_volume  += quote_qty
            g.sum_price_qty       += price * qty
            g.sum_buyer_maker     += qty * is_buyer
            g.sum_buyer_maker_price_qty += price * qty * is_buyer

            global_agg_values[idx] = g

    return bar_id_map, global_agg_values


import re
_UNIT_FACTOR = {
    "s": 1,
    "ms": 10**3,
    "us": 10**6,
    "ns": 10**9,
}

def _parse_freq_seconds(freq: str) -> int:
    """'1m', '5m', '1h', '1d', '1w' (and synonyms like '1min','1day')."""
    s = str(freq).strip().lower().replace(" ", "")
    # normalize common synonyms
    s = (s.replace("minutes", "m").replace("minute", "m").replace("mins", "m").replace("min", "m")
           .replace("seconds", "s").replace("second", "s").replace("secs", "s").replace("sec", "s")
           .replace("hours", "h").replace("hour", "h").replace("hrs", "h").replace("hr", "h")
           .replace("days", "d").replace("day", "d")
           .replace("weeks", "w").replace("week", "w").replace("wks", "w").replace("wk", "w"))
    m = re.fullmatch(r"(\d+)([smhdw])", s)
    if not m:
        raise NotImplementedError(f"{freq} not supported")
    n = int(m.group(1))
    mult = {'s':1, 'm':60, 'h':3600, 'd':86400, 'w':604800}[m.group(2)]
    return n * mult

def _infer_epoch_per_second(times_arr: np.ndarray) -> int:
    """
    Detect epoch unit by magnitude. Returns units per second:
      1 (s), 1_000 (ms), 1_000_000 (µs), or 1_000_000_000 (ns).
    """
    # Use median of |values| to avoid outliers
    t = np.abs(times_arr.astype(np.int64, copy=False))
    if t.size == 0:
        return 1_000  # sensible default: ms
    med = float(np.median(t))
    if med < 1e11:        # ~seconds (1.7e9 today)
        return 1
    if med < 1e14:        # ~milliseconds (1.7e12)
        return 1_000
    if med < 1e17:        # ~microseconds (1.7e15)
        return 1_000_000
    return 1_000_000_000  # nanoseconds

_EPOCH_UNIT_STR = {1:'s', 1_000:'ms', 1_000_000:'us', 1_000_000_000:'ns'}

from src.data_connectors.helpers.s3_utils import (is_s3_uri, cached_s3_fetch, S3Config,
                                              upload_file_to_s3,delete_s3_object_if_exists,
                                              purge_invalid_zips_under_prefix)



def get_bars_by_date_optimized(url, file_root, bars_frequency,
                               api_source: str, logger=None, chunksize=1_000_000,
                               max_retries: int = 3, timeout_seconds: int = 600,
                               ):


    if logger is None:
        logger = FakeLogger()
    from mainsequence.client import MARKETS_CONSTANTS as CONSTANTS # Local import to avoid circular dependency


    FUTURES_COLUMNS = ["trade_id", "price", "qty", "quoteQty", "time", "isBuyerMaker"]
    SPOT_COLUMNS = ["trade_id", "price", "qty", "quoteQty", "time", "isBuyerMaker", "isBestMatch"]
    only_download = os.environ.get("BINANCE_ONLY_DOWNLOAD", "false").lower() == "true"
    if only_download == False:
        logger.info(f"Building (optimized)... {url}")
    start_time = time.time()


    # -- local repo ---
    source_type = "spot" if "spot" in url else "futures"
    local_path=os.environ.get("BINANCE_TEMP_FILES")
    local_path=local_path+f"/{source_type}" if local_path else local_path
    filename = url.rsplit("/", 1)[-1]  # e.g. BTCUSDT-trades-2018-07.zip
    file_root, _ = os.path.splitext(filename)  # e.g. BTCUSDT-trades-2018-07

    target_path = os.path.join(local_path, filename) if local_path else None
    if local_path and is_s3_uri(local_path):
        # os.path.join can introduce backslashes on Windows; ensure a clean S3 URI
        target_path = f"{local_path.rstrip('/')}/{filename}"

    zip_source = None
    if local_path:
        if is_s3_uri(local_path):
            # S3 cache: try to pull cached object (if exists) to a local temp cache path
            try:
                cached_local_path = cached_s3_fetch(
                    target_path,
                    cache_root=None,  # use default ~/.cache/s3_cache inside s3_utils
                    ttl_seconds=None,  # always validate remotely; if not found -> raises
                    min_free_bytes=None,
                    s3_config=S3Config.from_env(),
                    logger=logger,
                )
                zip_source = cached_local_path
                logger.info(f"S3 cached read {target_path} -> {zip_source}")
                if only_download:
                    return pd.DataFrame()
            except Exception:
                # Not present in S3 cache; we'll download via HTTP next
                zip_source = None
        else:

            import shutil
            os.makedirs(local_path, exist_ok=True)
            MIN_FREE_BYTES = 50_000_000_000  # 50 GB (decimal)
            free_bytes = shutil.disk_usage(local_path).free
            if free_bytes < MIN_FREE_BYTES:
                raise Exception("No Space to Keep Data")
            if os.path.isfile(target_path):
                # Use cached file
                zip_source = target_path
        logger.info(f"Local read {zip_source}")



    # --- Robust Network Fetching with Retries and Timeout ---
    cleanup_tmp_after_read = None  # only used if we create a temp file for S3 upload

    if zip_source is None:
        logger.info(f"Downloading File from  {url}")
        response = None
        for attempt in range(max_retries):
            try:
                response = requests.get(url, timeout=timeout_seconds, stream=True)
                response.raise_for_status()
                break
            except RequestException as e:
                logger.warning(f"Attempt {attempt + 1}/{max_retries} failed for {url}: {e}")
                if attempt + 1 == max_retries:
                    logger.warning(f"All retries failed for {url}. Aborting.")
                time.sleep(2 ** attempt)

        if response is None:
            raise Exception
        if response.status_code!=200:
            #only case when we return an empyt df to dont stop update
            return pd.DataFrame()
        if local_path:
            if is_s3_uri(local_path):
                # Write HTTP response to a local tmp, upload to S3 cache, then read from the tmp
                tmpfile = None
                try:
                    with tempfile.NamedTemporaryFile("wb", delete=False) as tf:
                        tmpfile = tf.name
                        for chunk_data in response.iter_content(chunk_size=8192):
                            if chunk_data:
                                tf.write(chunk_data)
                        tf.flush()
                        os.fsync(tf.fileno())
                    try:
                        upload_file_to_s3(
                            target_path,
                            tmpfile,
                            s3_config=S3Config.from_env(),
                            content_type="application/zip",
                        )
                        logger.info(f"S3 cached write {target_path}")
                    except Exception as e:
                        logger.warning(f"S3 cache upload failed for {target_path}: {e}")
                    zip_source = tmpfile
                    cleanup_tmp_after_read = tmpfile  # remove after processing
                except Exception:
                    # Clean up tmp if anything went wrong before we set zip_source
                    if tmpfile and os.path.exists(tmpfile):
                        try:
                            os.remove(tmpfile)
                        except OSError:
                            pass
                    raise
            else:
                # Persist to local cache atomically
                tmpfile = None
                try:
                    with tempfile.NamedTemporaryFile("wb", dir=local_path, delete=False) as tf:
                        tmpfile = tf.name
                        for chunk_data in response.iter_content(chunk_size=8192):
                            if chunk_data:
                                tf.write(chunk_data)
                        tf.flush()
                        os.fsync(tf.fileno())
                    os.replace(tmpfile, target_path)  # atomic on POSIX/Windows
                    zip_source = target_path
                finally:
                    # Clean up temp file if replace failed
                    if tmpfile and os.path.exists(tmpfile):
                        try:
                            os.remove(tmpfile)
                        except OSError:
                            pass
        else:
            # Keep in memory
            zip_in_memory = io.BytesIO()
            for chunk_data in response.iter_content(chunk_size=8192):
                if chunk_data:
                    zip_in_memory.write(chunk_data)
            zip_in_memory.seek(0)
            zip_source = zip_in_memory

    # Numba works best with typed dictionaries
    bar_id_map = numba.typed.Dict.empty(
        key_type=numba.types.int64,
        value_type=numba.types.int64,
    )
    # Pre-allocate a large array for the aggregates. We'll resize later.

    initial_size = 40000  # A full month of 1-min bars is ~43200
    global_agg_values = np.zeros(initial_size, dtype=bar_agg_type.dtype)

    if only_download:
        if cleanup_tmp_after_read and isinstance(zip_source, str) and os.path.exists(cleanup_tmp_after_read):
            try:
                os.remove(cleanup_tmp_after_read)
            except OSError:
                pass
        return pd.DataFrame()


    try:

        with ZipFile(zip_source) as zipfile_obj:
            csv_filename = file_root + ".csv"
            if csv_filename not in zipfile_obj.namelist():
                logger.error(f"CSV '{csv_filename}' not found in zip from {url}")
                return pd.DataFrame()

            with zipfile_obj.open(csv_filename) as csv_file:
                for chunk in tqdm(pd.read_csv(csv_file, header=None, low_memory=False, chunksize=chunksize),
                                  desc="Aggregating chunks", leave=False):
                    if api_source == CONSTANTS.FIGI_SECURITY_TYPE_CRYPTO:
                        chunk.columns = SPOT_COLUMNS
                        chunk.drop(columns=["isBestMatch"], inplace=True)
                    else:
                        if "price" in chunk.iloc[0].values:
                            assert chunk.iloc[0].values.tolist()==['id', 'price', 'qty', 'quote_qty', 'time', 'is_buyer_maker']
                            chunk=chunk.iloc[1:].copy()

                        chunk.columns = FUTURES_COLUMNS

                    # --- More Robust Data Cleaning - working with integers directly ---
                    # Handle potential header rows by coercing time column to numeric
                    chunk['time'] = pd.to_numeric(chunk['time'], errors='coerce')
                    try:
                        chunk["trade_id"] = pd.to_numeric(chunk["trade_id"], errors="coerce").astype(np.int64)
                    except Exception as e:
                        raise e
                    chunk.dropna(subset=['time'], inplace=True)
                    if chunk.empty: raise Exception("No data found")

                    # Ensure all relevant columns are numeric, coercing errors
                    for col in ["price", "qty", "quoteQty"]:

                        chunk[col] = pd.to_numeric(chunk[col], errors='coerce')

                    if chunk["isBuyerMaker"].dtype == 'object':
                        s = chunk["isBuyerMaker"].astype("string").str.strip().str.lower()
                        chunk["isBuyerMaker"] = s.map({"true": 1, "false": 0}).astype("Int8")

                        # Drop any rows where key data might be missing after coercion
                    chunk.dropna(subset=["price", "qty", "quoteQty", "isBuyerMaker"], inplace=True)
                    if chunk.empty: raise Exception("No data found")

                    times_arr, prices_arr, qtys_arr, quote_qtys_arr,is_buyer_maker_arr = (
                        chunk["time"].to_numpy(dtype=np.int64),
                        chunk["price"].to_numpy(dtype=np.float64),
                        chunk["qty"].to_numpy(dtype=np.float64),
                        chunk["quoteQty"].to_numpy(dtype=np.float64),
                        chunk["isBuyerMaker"].to_numpy(dtype=np.int64)

                    )
                    trade_ids_arr = chunk["trade_id"].to_numpy(dtype=np.int64)  # ← NEW

                    sec_per_bar = _parse_freq_seconds(bars_frequency)  # e.g., 5m -> 300
                    units_per_sec = _infer_epoch_per_second(times_arr)  # s/ms/µs/ns
                    step = np.int64(sec_per_bar) * np.int64(units_per_sec)  # bar length in the same unit as times_arr
                    bar_ids = (times_arr // step) * step


                    bar_id_map, global_agg_values = aggregate_trades_chunk(
                        bar_ids, prices_arr, qtys_arr, quote_qtys_arr, times_arr, is_buyer_maker_arr,
                        trade_ids_arr,
                        bar_id_map, global_agg_values
                    )
    except BadZipFile as e:
        # Specific handling for "not a zip" / corrupt archives
        logger.exception(f"Corrupt/invalid zip from {url}: {e}")

        # If we were using an S3 cache path, remove the bad object so the next run can re-fetch.
        if local_path and is_s3_uri(local_path):
            # target_path is already computed above (s3://bucket/prefix/<filename>.zip)
            delete_s3_object_if_exists(uri=target_path, logger=logger)
            purge_invalid_zips_under_prefix(
                local_path+"/",
                deep=False,  # set True to CRC-scan entries (slower)
                dry_run=True,  # preview first
                include_all=False  # only *.zip by default
            )

        # Also delete any local tmp/cached file we created so it isn't reused.
        if isinstance(zip_source, str) and os.path.exists(zip_source):
            try:
                os.remove(zip_source)
                logger.info(f"Removed local corrupt cache file: {zip_source}")
            except OSError as rm_err:
                logger.warning(f"Failed to remove local cache {zip_source}: {rm_err}")

        # Preserve existing behavior (surface the failure to the caller)
        raise
    except Exception as e:
        logger.exception(f"Failed to process zip file from {url}: {e}")
        raise e
    finally:
        # remove temp file created only to read (S3 upload case)
        if cleanup_tmp_after_read and isinstance(zip_source, str) and os.path.exists(cleanup_tmp_after_read):
            try:
                os.remove(cleanup_tmp_after_read)
            except OSError:
                pass
    if not bar_id_map: raise Exception("Error reading data")

    # --- Final DataFrame Construction ---
    final_size = len(bar_id_map)
    ohlc = pd.DataFrame(global_agg_values[:final_size])
    ohlc['bar_id_start'] = list(bar_id_map.keys())

    ohlc['vwap'] = ohlc['sum_price_qty'] / ohlc['volume']
    ohlc.drop(columns=['sum_price_qty'], inplace=True)

    # The 'open_time' of the bar is its floored start time.

    # Convert times using the same epoch unit we inferred from bar_id_start
    units_per_sec_idx = _infer_epoch_per_second(ohlc['bar_id_start'].to_numpy(dtype=np.int64))
    unit_str = _EPOCH_UNIT_STR[units_per_sec_idx]
    sec_per_bar = _parse_freq_seconds(bars_frequency)

    # Open/first/last trade times (all in correct unit + UTC tz)
    ohlc['open_time'] = pd.to_datetime(ohlc['bar_id_start'], unit=unit_str, utc=True)
    ohlc['first_trade_time'] = pd.to_datetime(ohlc['first_trade_time'], unit=unit_str, utc=True)
    ohlc['last_trade_time'] = pd.to_datetime(ohlc['last_trade_time'], unit=unit_str, utc=True)


    # The final bar timestamp (the index) is the bar's closing time.
    ohlc['time_index'] = ohlc['open_time'] + pd.to_timedelta(sec_per_bar, unit='s')
    ohlc.drop(columns=['bar_id_start'], inplace=True)
    logger.info(f"--- Completed {url} in {time.time() - start_time:.2f}s ---")
    return ohlc.set_index('time_index')


# ==============================================================================
# 4. NEW: High-Performance Aggregation for Information-Driven Bars
# ==============================================================================


information_bar_type = np.dtype([
    ('open', np.float64), ('high', np.float64), ('low', np.float64), ('close', np.float64),
    ('volume', np.float64), ('vwap', np.float64),
    ('start_time', np.int64), ('end_time', np.int64),
    ('imbalance_at_close', np.float64),
])
information_bar_type = from_dtype(information_bar_type)


@numba.njit
def aggregate_to_information_bars(
        prices, qtys, times,
        ema_alpha, warmup_bars, expected_vol_per_bar,
        # State carried over from the previous chunk
        initial_threshold, initial_imbalance,
        current_bar_state,
        warmup_imbalances_list, bars_completed_so_far,
        # Tick Rule State
        initial_last_price, initial_last_direction
):
    """
    Builds information-driven bars using the tick rule to infer trade direction.
    """
    completed_bars = []

    b_open, b_high, b_low, b_volume, b_sum_price_qty, b_start_time = current_bar_state
    cumulative_imbalance = initial_imbalance
    threshold = initial_threshold

    # Initialize tick rule state for the current chunk
    last_price = initial_last_price
    last_direction = initial_last_direction

    for i in range(len(prices)):
        price, qty, time = prices[i], qtys[i], times[i]

        if b_start_time == 0:
            b_open, b_high, b_low, b_start_time = price, price, price, time

        b_high, b_low = max(b_high, price), min(b_low, price)
        b_volume += qty
        b_sum_price_qty += price * qty

        # --- Tick Rule Implementation ---
        if price > last_price:
            direction = 1  # Buy
        elif price < last_price:
            direction = -1  # Sell
        else:
            direction = last_direction  # Use previous direction

        signed_volume = qty * direction
        cumulative_imbalance += signed_volume

        # Update state for the next trade
        if direction != 0:
            last_direction = direction
        last_price = price

        # --- Bar Trigger Logic ---
        trigger = False
        if threshold > 0:
            if abs(cumulative_imbalance) >= threshold: trigger = True
        else:
            if b_volume >= expected_vol_per_bar: trigger = True

        if trigger:
            vwap = b_sum_price_qty / b_volume if b_volume > 0 else b_open
            bar = (b_open, b_high, b_low, price, b_volume, vwap, b_start_time, time, cumulative_imbalance)
            completed_bars.append(bar)
            bars_completed_so_far += 1

            if threshold == 0:
                warmup_imbalances_list.append(abs(cumulative_imbalance))
                if bars_completed_so_far >= warmup_bars:
                    total_imbalance = 0.0
                    for imb in warmup_imbalances_list: total_imbalance += imb
                    threshold = total_imbalance / len(warmup_imbalances_list)
            else:
                threshold = (1 - ema_alpha) * threshold + ema_alpha * abs(cumulative_imbalance)

            cumulative_imbalance = 0.0
            b_open, b_high, b_low, b_start_time = 0.0, 0.0, 0.0, 0
            b_volume, b_sum_price_qty = 0.0, 0.0

    final_bar_state = (b_open, b_high, b_low, b_volume, b_sum_price_qty, b_start_time)
    final_tick_rule_state = (last_price, last_direction)

    return completed_bars, threshold, cumulative_imbalance, final_bar_state, warmup_imbalances_list, bars_completed_so_far, final_tick_rule_state


# ==============================================================================
# 5. NEW: Main Orchestrator for Information-Driven Bars
# ==============================================================================

def get_information_bars(
        url: str, file_root: str, api_source: str,
        # Control parameters
        ema_alpha: float = 0.001, warmup_bars: int = 20,
        warmup_lookahead_trades: int = 50000,
        # Optional state from previous day
        previous_day_state: dict = None,
        # Standard parameters
        chunksize: int = 1_000_000, max_retries: int = 3, timeout_seconds: int = 600
) -> tuple[pd.DataFrame, dict]:
    """
    Orchestrates the creation of information-driven bars from a remote data file.

    This function processes a single data file (e.g., a day or month of trades) and
    builds bars based on trade imbalance, as described by López de Prado. It is
    stateful, designed to be called sequentially on contiguous data blocks.

    Args:
        url (str): The URL to the remote zip file containing trade data.
        file_root (str): The base name of the CSV file inside the zip (e.g., "BTCUSDT-trades-2022-01-01").
        api_source (str): The source of the data, used to determine column names.
        ema_alpha (float): The learning rate for the EMA that updates the imbalance threshold.
                           A small value creates a stable, long-memory threshold.
        warmup_bars (int): The number of initial bars to create using a fixed-volume
                           approach before switching to the dynamic imbalance threshold.
        warmup_lookahead_trades (int): The number of trades to read from the start of the
                                       file to estimate the initial volume-per-bar for the
                                       warm-up phase. This is only used if `previous_day_state` is None.
        previous_day_state (dict, optional): A dictionary containing the final state from the
                                             previous run. If provided, the warm-up phase is
                                             skipped. Expected keys are 'last_threshold',
                                             'last_imbalance', and 'incomplete_bar'.
        chunksize (int): The number of rows to process in each chunk.
        max_retries (int): The number of times to retry downloading the data file.
        timeout_seconds (int): The timeout for the network request.

    Returns:
        tuple[pd.DataFrame, dict]:
            - pd.DataFrame: A DataFrame of the completed information-driven bars, indexed by the bar's end time.
            - dict: A dictionary containing the final state to be passed as `previous_day_state`
                    to the next run. Includes the last learned threshold, the imbalance of the
                    final incomplete bar, and the state of that incomplete bar.
    """
    from mainsequence.client import MARKETS_CONSTANTS as CONSTANTS

    FUTURES_COLUMNS = ["trade_id", "price", "qty", "quoteQty", "time", "isBuyerMaker"]
    SPOT_COLUMNS = ["trade_id", "price", "qty", "quoteQty", "time", "isBuyerMaker", "isBestMatch"]

    logger.info(f"Building Information Bars from: {url}")

    # --- Local File Caching ---
    CACHE_DIR = ".cache/binance_data"
    os.makedirs(CACHE_DIR, exist_ok=True)
    url_filename = url.split("/")[-1]
    local_zip_path = os.path.join(CACHE_DIR, url_filename)

    zip_content = None
    if os.path.exists(local_zip_path):
        logger.info(f"Loading from local cache: {local_zip_path}")
        with open(local_zip_path, 'rb') as f:
            zip_content = f.read()
    else:
        logger.info("File not in cache, downloading...")
        for attempt in range(max_retries):
            try:
                response = requests.get(url, timeout=timeout_seconds, stream=True)
                response.raise_for_status()
                zip_content = response.content
                with open(local_zip_path, 'wb') as f:
                    f.write(zip_content)
                logger.info(f"Saved to cache: {local_zip_path}")
                break
            except RequestException as e:
                logger.warning(f"Attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt + 1 == max_retries: return pd.DataFrame(), {}
                time.sleep(2 ** attempt)

    if zip_content is None: return pd.DataFrame(), {}

    # --- Initialize State for Aggregation ---
    all_completed_bars = []

    if previous_day_state:
        stateful_threshold = previous_day_state.get('last_threshold', 0.0)
        stateful_imbalance = previous_day_state.get('last_imbalance', 0.0)
        stateful_bar_state = previous_day_state.get('incomplete_bar', (0.0, 0.0, 0.0, 0.0, 0.0, 0))
        stateful_tick_rule_state = previous_day_state.get('tick_rule_state', (0.0, 1)) # (price, direction)

        stateful_warmup_imbalances = numba.typed.List.empty_list(numba.types.float64)
        stateful_bars_completed = warmup_bars
        expected_vol_per_bar = 0
    else:
        logger.info(f"No previous state. Using lookahead of {warmup_lookahead_trades} trades for warm-up.")
        try:
            with ZipFile(io.BytesIO(zip_content)) as z:
                csv_filename = file_root + ".csv"
                with z.open(csv_filename) as f:
                    warmup_df = pd.read_csv(f, header=None, nrows=warmup_lookahead_trades)

            warmup_df.columns = FUTURES_COLUMNS if api_source != CONSTANTS.FIGI_SECURITY_TYPE_CRYPTO else SPOT_COLUMNS
            warmup_df['time'] = pd.to_numeric(warmup_df['time'], errors='coerce')
            warmup_df['qty'] = pd.to_numeric(warmup_df['qty'], errors='coerce')
            warmup_df.dropna(subset=['time', 'qty'], inplace=True)

            time_span_minutes = (warmup_df['time'].max() - warmup_df['time'].min()) / 60000
            total_volume = warmup_df['qty'].sum()

            if time_span_minutes > 0 and total_volume > 0:
                avg_vol_per_minute = total_volume / time_span_minutes
                expected_vol_per_bar = avg_vol_per_minute
            else:
                expected_vol_per_bar = warmup_df['qty'].mean() * 25

            logger.info(f"Calculated warm-up volume per bar: {expected_vol_per_bar:.4f}")

        except Exception as e:
            logger.error(f"Failed during warm-up lookahead: {e}. Aborting.")
            return pd.DataFrame(), {}

        stateful_threshold = 0.0
        stateful_imbalance = 0.0
        stateful_bar_state = (0.0, 0.0, 0.0, 0.0, 0.0, 0)
        stateful_tick_rule_state = (0.0, 1) # Start with a buy assumption

        # CORRECTED: Initialize as a typed list for Numba, even when empty
        stateful_warmup_imbalances = numba.typed.List.empty_list(numba.types.float64)
        stateful_bars_completed = 0

    try:
        zip_in_memory = io.BytesIO(zip_content)
        with ZipFile(zip_in_memory) as zipfile_obj:
            csv_filename = file_root + ".csv"
            with zipfile_obj.open(csv_filename) as csv_file:
                for chunk in tqdm(pd.read_csv(csv_file, header=None, low_memory=False, chunksize=chunksize),
                                  desc="Building Info Bars"):
                    if api_source == CONSTANTS.FIGI_SECURITY_TYPE_CRYPTO:
                        chunk.columns = SPOT_COLUMNS
                        chunk.drop(columns=["trade_id", "isBestMatch", "quoteQty"], inplace=True)
                    else:
                        chunk.columns = FUTURES_COLUMNS
                        chunk.drop(columns=["trade_id", "quoteQty"], inplace=True)

                    if not chunk.empty and isinstance(chunk.iloc[0, 0], str):
                        if not chunk.iloc[0, 0].isdigit(): chunk = chunk.iloc[1:].copy()

                    for col in ["price", "qty"]: chunk[col] = pd.to_numeric(chunk[col], errors='coerce')
                    chunk["time"] = pd.to_numeric(chunk["time"], errors='coerce')
                    if chunk["isBuyerMaker"].dtype == 'object': chunk["isBuyerMaker"] = chunk["isBuyerMaker"].astype(
                        bool)

                    chunk.dropna(inplace=True)
                    chunk.sort_values('time', inplace=True)
                    if chunk.empty: continue

                    prices_arr, qtys_arr, times_arr, is_buyer_maker_arr = (
                        chunk["price"].to_numpy(dtype=np.float64),
                        chunk["qty"].to_numpy(dtype=np.float64),
                        chunk["time"].to_numpy(dtype=np.int64),
                        chunk["isBuyerMaker"].to_numpy(dtype=np.int64)
                    )

                    completed_bars, new_thresh, new_imbalance, new_bar_state, new_warmup_list, new_bars_completed, new_tick_rule_state = aggregate_to_information_bars(
                        prices_arr, qtys_arr, times_arr,
                        ema_alpha, warmup_bars, expected_vol_per_bar,
                        stateful_threshold, stateful_imbalance,
                        stateful_bar_state,
                        stateful_warmup_imbalances,
                        stateful_bars_completed,
                        *stateful_tick_rule_state
                    )
                    if completed_bars: all_completed_bars.extend(completed_bars)
                    stateful_threshold = new_thresh
                    stateful_imbalance = new_imbalance
                    stateful_bar_state = new_bar_state
                    stateful_warmup_imbalances = numba.typed.List(new_warmup_list)
                    stateful_bars_completed = new_bars_completed
                    stateful_tick_rule_state = new_tick_rule_state

    except Exception as e:
        logger.exception(f"Failed to process info bars for {url}: {e}")
        return pd.DataFrame(), {}

    # --- Final DataFrame and State Construction ---
    final_state = {
        'last_threshold': stateful_threshold,
        'last_imbalance': stateful_imbalance,
        'incomplete_bar': stateful_bar_state
    }

    if not all_completed_bars: return pd.DataFrame(), final_state

    df = pd.DataFrame(all_completed_bars, columns=[
        'open', 'high', 'low', 'close', 'volume', 'vwap', 'start_time', 'end_time', 'imbalance_at_close'
    ])
    df['end_time'] = pd.to_datetime(df['end_time'], unit='ms', utc=True)
    df=df.rename(columns={"start_time":"open_time","end_time":"time_index"})
    return df.set_index('time_index'), final_state