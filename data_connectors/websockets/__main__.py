# Your imports, unchanged as requested.
from data_connectors.websockets.producers.alpaca import AlpacaTradesProducer
from data_connectors.websockets.producers.binance import BinanceTradesProducer
from data_connectors.websockets.consumers import DataWriter
from data_connectors.websockets.bar import BarAggregator
import queue
import asyncio
import signal
import threading
import logging
import argparse
import time
# --- Imports for Multi-Processing ---
import multiprocessing as mp
import os
from concurrent.futures import ProcessPoolExecutor


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
    "binance": {
        "producer_class": BinanceTradesProducer,
        "symbols": ['BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'XRP/USDT', 'DOGE/USDT'],  # Example with more symbols
        "bar_interval_minutes": 1,
        "writer_destination": "API or DB endpoint for Binance"
    },
    "alpaca": {
        "producer_class": AlpacaTradesProducer,
        "symbols": ['SPY', 'AAPL', 'TSLA', 'NVDA', 'MSFT', 'GOOG'],  # Example with more symbols
        "bar_interval_minutes": 5,
        "writer_destination": "API or DB endpoint for Alpaca"
    }
}


# --- Process Target Functions ---
# These functions are the entry points for each new process.

def run_aggregator_process(input_queue, output_queue, interval_minutes):
    """Target function to run the BarAggregator in a dedicated process."""
    logger = setup_logger()
    # This instantiates the imported BarAggregator class
    aggregator = BarAggregator(input_queue, output_queue, interval_minutes, logger)
    aggregator.run()  # This is a blocking call that starts the aggregator's loop


def run_writer_process(data_queue):
    """
    Target function to run the DataWriter in a dedicated process.
    Includes robust error handling to aid in debugging.
    """
    logger = setup_logger()
    try:
        # This instantiates the imported DataWriter class
        writer = DataWriter( data_queue, logger=logger)
        writer.run() # This starts the writer's loop
    except Exception as e:
        # If the process fails for any reason, this will log the error
        # to the crawler.log file before the process exits.
        logger.exception(f"Writer process encountered a fatal error and is shutting down: {e}")



def distributor(input_queue, output_queues, symbol_to_worker_map, stop_event):
    """Distributes trades from the main queue to the correct worker queue."""
    logger = setup_logger()
    logger.info("Distributor thread started.")
    trade_count = 0
    while not stop_event.is_set():
        try:
            trade = input_queue.get(timeout=1.0)
            symbol = trade[1] # Assumes symbol is the second element in the trade tuple
            worker_index = symbol_to_worker_map.get(symbol)
            if worker_index is not None:
                output_queues[worker_index].put(trade)
                trade_count += 1
                if trade_count % 1000 == 0: # Log every 1000 trades
                    logger.info(f"Distributor has processed {trade_count} trades.")
        except queue.Empty:
            continue
        except Exception as e:
            logger.error(f"Distributor error: {e}")

# --- Health Check Function ---
def queue_health_check(queues: dict, stop_event: threading.Event):
    """Periodically logs the size of all queues in the pipeline."""
    logger = setup_logger()
    while not stop_event.is_set():
        try:
            sizes = {name: q.qsize() for name, q in queues.items()}
            logger.info(f"QUEUE SIZES: {sizes}")
            time.sleep(10) # Log sizes every 10 seconds
        except Exception as e:
            logger.error(f"Health check error: {e}")


# --- Main Application Logic ---

def start_crawler(config: dict):
    """
    This function starts the scalable, multi-process pipeline.
    """
    logger = setup_logger()
    # Use N-1 CPU cores for aggregation to leave one for the main process (producer, distributor)
    NUM_AGGREGATOR_WORKERS = min(4,max(1, os.cpu_count() - 1))
    SYMBOLS = config["symbols"]

    logger.info(f"Starting crawler with {NUM_AGGREGATOR_WORKERS} aggregator workers for {len(SYMBOLS)} symbols.")

    # 1. Use a multiprocessing Manager to create queues that can be shared between processes
    manager = mp.Manager()
    trades_queue = manager.Queue()
    aggregator_input_queues = [manager.Queue() for _ in range(NUM_AGGREGATOR_WORKERS)]
    bars_queue = manager.Queue()

    # 2. Map symbols to worker processes for even distribution
    symbol_to_worker_map = {symbol: i % NUM_AGGREGATOR_WORKERS for i, symbol in enumerate(SYMBOLS)}

    # 3. Start Distributor and Health Check Threads in the main process
    stop_event = threading.Event()

    distributor_thread = threading.Thread(
        target=distributor,
        args=(trades_queue, aggregator_input_queues, symbol_to_worker_map, stop_event),
        name="Distributor"
    )
    distributor_thread.start()

    health_check_queues = {"trades": trades_queue, "bars": bars_queue}
    health_check_thread = threading.Thread(
        target=queue_health_check,
        args=(health_check_queues, stop_event),
        name="HealthCheck"
    )
    health_check_thread.start()

    # 4. Start Worker Processes using a ProcessPoolExecutor
    # The pool will manage N aggregator processes + 1 writer process
    process_pool = ProcessPoolExecutor(max_workers=NUM_AGGREGATOR_WORKERS + 1)

    # Submit the single writer process
    process_pool.submit(run_writer_process, bars_queue)

    # Submit the pool of aggregator processes
    for i in range(NUM_AGGREGATOR_WORKERS):
        process_pool.submit(run_aggregator_process, aggregator_input_queues[i], bars_queue,
                            config["bar_interval_minutes"])

    # 5. Start the async Producer in the main process
    producer = config["producer_class"](
        symbols=config["symbols"],
        data_queue=trades_queue,  # Producer puts trades onto the shared queue
        logger=logger
    )
    loop = asyncio.get_event_loop()
    producer_task = loop.create_task(producer.run())

    # --- Graceful Shutdown ---
    def handle_shutdown():
        logger.info("Shutdown signal received. Stopping all components.")
        if not stop_event.is_set():
            # Stop producer first to halt new data
            if not producer_task.done():
                loop.call_soon_threadsafe(producer_task.cancel)
            # Stop background threads
            stop_event.set()
            # Shutdown the process pool, which will interrupt the workers
            process_pool.shutdown(wait=True, cancel_futures=True)
            # Join the threads
            distributor_thread.join()
            health_check_thread.join()
            logger.info("Application has shut down.")

    signal.signal(signal.SIGINT, lambda s, f: handle_shutdown())
    signal.signal(signal.SIGTERM, lambda s, f: handle_shutdown())

    try:
        loop.run_until_complete(producer_task)
    except asyncio.CancelledError:
        logger.info("Producer task was cancelled.")
    finally:
        # Ensure final shutdown is called
        handle_shutdown()


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
        start_crawler(config_to_run)
    else:
        logger.error(f"Invalid exchange specified. No configuration found for '{args.exchange}'.")


if __name__ == "__main__":
    # This block is required for multiprocessing on some platforms (Windows, macOS)
    # It ensures that child processes don't re-import and re-run the main script's code.
    mp.set_start_method("fork")  # 'fork' is generally faster, use 'spawn' if needed.
    main()