# Your imports, unchanged as requested.
from mainsequence.client import DynamicTableMetaData

from data_connectors.websockets.producers.alpaca import AlpacaTradesProducer
from data_connectors.websockets.producers.binance import BinanceTradesProducer
from data_connectors.websockets.time_series import BarConfiguration,LiveBarsTimeSeries


import logging
import argparse
import time
# --- Imports for Multi-Processing ---
import multiprocessing as mp
import os

import mainsequence.client as ms_client


# --- Logger Setup ---
def setup_logger():
    """
    Sets up a logger that writes to a file, making it accessible across all processes.
    """
    logger = logging.getLogger("Crawler")
    # Prevent adding handlers multiple times in child processes
    if not logger.handlers:
        logger.setLevel(logging.INFO)

        # Create a file handler to write logs to a file
        # All processes will append to this single file.
        file_handler = logging.FileHandler("crawler.log")
        file_handler.setLevel(logging.INFO)

        # Create a console handler to see logs from the main process
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        # Include processName in the formatter to distinguish logs
        formatter = logging.Formatter('%(asctime)s - %(processName)s - %(threadName)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)  # You'll see main process logs in terminal
    return logger


# --- Centralized Configurations ---
CONFIGS = {
    "binance_futures": {
        "producer_class": BinanceTradesProducer,
        "tickers": ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'XRPUSDT', 'DOGEUSDT'],  # Example with more symbols
        "ticker_execution_venue":ms_client.MARKETS_CONSTANTS.BINANCE_FUTURES_EV_SYMBOL,
        "bar_interval_minutes": 1,
    },
    "alpaca": {
        "producer_class": AlpacaTradesProducer,
        "symbols": ['SPY', 'AAPL', 'TSLA', 'NVDA', 'MSFT', 'GOOG'],  # Example with more symbols
        "bar_interval_minutes": 5,
    }
}


# --- Main Application Logic ---

def start_crawler(price_source:str,config: dict):
    """
    This function starts the scalable, multi-process pipeline.
    """



    asset_list=ms_client.AssetCurrencyPair.filter(ticker__in=config["tickers"],
                                              execution_venue__symbol=config["ticker_execution_venue"]       )

    bar_configuration=BarConfiguration(bar_interval_minutes=config["bar_interval_minutes"])
    ts=LiveBarsTimeSeries(prices_source=price_source,
                          asset_list=asset_list,
                          bar_configuration=bar_configuration,
                          )

    #force the creation of the TimeSerie
    ts.verify_and_build_remote_objects()
    #set table metadata
    if ts.metadata.sourcetableconfiguration is None:
        from data_connectors.websockets.bar import Bar
        dtypes=Bar.get_dtypes()

        index_names=["time_index","unique_identifier"]
        column_index_names=[c for c in list(dtypes.keys()) if c not in index_names]
        stc = ms_client.SourceTableConfiguration.create(
            column_dtypes_map=dtypes,
            index_names=index_names,
            time_index_name="time_index",
            column_index_names=column_index_names,
            metadata_id=ts.metadata.id
        )
        #reset metadata
        metadata=ms_client.DynamicTableMetaData.get(id=stc.related_table)
        ts.local_persist_manager._metadata_cached=metadata

    ts.local_persist_manager.set_table_metadata(
        table_metadata=ts.get_table_metadata(update_statistics=None))

    ts.set_producer_class(config["producer_class"])
    ts.live_update()







def main():
    """
    Parses command-line arguments and starts the selected crawler pipeline.
    """
    parser = argparse.ArgumentParser(description="Run a data crawler and bar aggregator for a specific exchange.")
    parser.add_argument(
        "exchange",
        type=str,
        choices=CONFIGS.keys(),
        help=f"The exchange to run. Choices: {list(CONFIGS.keys())}"
    )
    args = parser.parse_args()
    config_to_run = CONFIGS.get(args.exchange)
    if config_to_run:
        start_crawler(price_source=args.exchange,config=config_to_run)
    else:
        print(f"Invalid exchange specified. No configuration found for '{args.exchange}'.")


if __name__ == "__main__":
    # This block is required for multiprocessing on some platforms (Windows, macOS)
    # It ensures that child processes don't re-import and re-run the main script's code.
    mp.set_start_method("fork")  # 'fork' is generally faster, use 'spawn' if needed.
    main()