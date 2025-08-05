

from typing import List, Dict
from pydantic import BaseModel

from data_connectors.websockets.consumers import DataWriter
from data_connectors.websockets.bar import BarAggregator
from data_connectors.websockets.producers import ProducerBase
from concurrent.futures import ProcessPoolExecutor
import queue
import asyncio
import signal
import threading
import time
import os

from mainsequence.tdag import DataNode
import mainsequence.client as ms_client
from mainsequence.logconf import dump_structlog_bound_logger,load_structlog_bound_logger
import multiprocessing as mp


class BarConfiguration(BaseModel):
    bar_interval_minutes :int




# --- Process Target Functions ---
# These functions are the entry points for each new process.

def run_aggregator_process(input_queue, output_queue, interval_minutes,asset_symbols_to_uid_map,log_config):
    """Target function to run the BarAggregator in a dedicated process."""
    logger = load_structlog_bound_logger(log_config)
    name = threading.current_thread().name
    logger = logger.bind(thread_id=name)
    # This instantiates the imported BarAggregator class
    try:
        aggregator = BarAggregator(input_queue, output_queue, interval_minutes, logger, asset_symbols_to_uid_map)
        aggregator.run()
    except Exception as e:
        # Log any fatal error that occurs within the aggregator process
        logger.exception(f"Aggregator process encountered a fatal error and is shutting down: {e}")


def run_writer_process(data_queue,local_metadata_id,log_config):
    """
    Target function to run the DataWriter in a dedicated process.
    Includes robust error handling to aid in debugging.
    """
    logger = load_structlog_bound_logger(log_config)
    name = threading.current_thread().name
    logger = logger.bind(thread_id=name)
    try:
        # This instantiates the imported DataWriter class
        writer = DataWriter( data_queue=data_queue, local_metadata_id=local_metadata_id,logger=logger)
        writer.run() # This starts the writer's loop
    except Exception as e:
        # If the process fails for any reason, this will log the error
        # to the crawler.log file before the process exits.
        logger.exception(f"Writer process encountered a fatal error and is shutting down: {e}")



def distributor(input_queue, output_queues, symbol_to_worker_map, stop_event,log_config):
    """Distributes trades from the main queue to the correct worker queue."""
    logger = load_structlog_bound_logger(log_config)
    name = threading.current_thread().name
    logger= logger.bind(thread_id=name)
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
def queue_health_check(queues: dict, stop_event: threading.Event,
                       log_config                       ):
    logger = load_structlog_bound_logger(log_config)
    name = threading.current_thread().name
    logger = logger.bind(thread_id=name)
    """Periodically logs the size of all queues in the pipeline."""
    while not stop_event.is_set():
        try:
            sizes = {name: q.qsize() for name, q in queues.items()}
            logger.info(f"QUEUE SIZES: {sizes}")
            time.sleep(10) # Log sizes every 10 seconds
        except Exception as e:
            logger.error(f"Health check error: {e}")



class LiveBarsTimeSeries(DataNode):

    NUM_AGGREGATOR_WORKERS = min(4, max(1, os.cpu_count() - 1))
    _ARGS_IGNORE_IN_STORAGE_HASH=["asset_list"]
    def __init__(self ,prices_source :str,
                 asset_list :List[ms_client.AssetMixin],
                 bar_configuration :BarConfiguration,
                 *args, **kwargs):

        self.prices_source = prices_source
        self.asset_list = asset_list
        self.bar_configuration = bar_configuration
        self.asset_symbols_to_uid_map={f"{a.base_asset.ticker}/{a.quote_asset.ticker}" :a.unique_identifier for a in asset_list}

        super().__init__(*args, **kwargs)
    def dependencies(self):
        return {}
    def set_producer_class(self,producer_class:ProducerBase):
        self.ProducerClass=producer_class

    def get_table_metadata(self) -> ms_client.TableMetaData:
        """

        """

        TS_ID = f"{self.prices_source}_{self.bar_configuration.bar_interval_minutes}_live_bars"

        mapping = {
            1: ms_client.DataFrequency.one_m,
            5: ms_client.DataFrequency.five_m,
            24 * 60: ms_client.DataFrequency.one_d,
            7 * 24 * 60: ms_client.DataFrequency.one_w,
            30 * 24 * 60: ms_client.DataFrequency.one_month,  # 30 days
            3 * 30 * 24 * 60: ms_client.DataFrequency.one_quarter,  # 90 days
            365 * 24 * 60: ms_client.DataFrequency.one_year,  # non‑leap year
        }

        meta = ms_client.TableMetaData(identifier=TS_ID,
                                       description=f"{self.prices_source} {self.bar_configuration.bar_interval_minutes}  minute bars, from live trades",
                                       data_frequency_id=mapping[self.bar_configuration.bar_interval_minutes],
                                       )

        return meta
    def run_after_post_init_routines(self):
        pass
    def update(self):
        pass
    def live_update(self):
        """

        """
        self.logger.info(
            f"Starting crawler with {self.NUM_AGGREGATOR_WORKERS} aggregator workers for {len(self.asset_list)} symbols.")

        logger_config=dump_structlog_bound_logger(self.logger)
        # 1. Use a multiprocessing Manager to create queues that can be shared between processes
        manager = mp.Manager()
        trades_queue = manager.Queue()
        aggregator_input_queues = [manager.Queue() for _ in range(self.NUM_AGGREGATOR_WORKERS)]
        bars_queue = manager.Queue()

        symbols=list(self.asset_symbols_to_uid_map.keys())
        # 2. Map symbols to worker processes for even distribution
        symbol_to_worker_map = {symbol: i % self.NUM_AGGREGATOR_WORKERS for i, symbol in enumerate(symbols)}

        # 3. Start Distributor and Health Check Threads in the main process
        stop_event = threading.Event()

        distributor_thread = threading.Thread(
            target=distributor,
            args=(trades_queue, aggregator_input_queues, symbol_to_worker_map, stop_event,logger_config),
            name="Distributor"
        )
        distributor_thread.start()

        health_check_queues = {"trades": trades_queue, "bars": bars_queue}
        health_check_thread = threading.Thread(
            target=queue_health_check,
            args=(health_check_queues, stop_event,logger_config),
            name="HealthCheck"
        )
        health_check_thread.start()

        # 4. Start Worker Processes using a ProcessPoolExecutor
        # The pool will manage N aggregator processes + 1 writer process
        process_pool = ProcessPoolExecutor(max_workers=self.NUM_AGGREGATOR_WORKERS + 1)


        # Submit the pool of aggregator processes
        futures = []

        self.logger.info("Submitting writer process...")
        futures.append(process_pool.submit(run_writer_process, bars_queue,self.local_time_serie.id, logger_config))

        self.logger.info(f"Submitting {self.NUM_AGGREGATOR_WORKERS} aggregator processes...")
        for i in range(self.NUM_AGGREGATOR_WORKERS):
            future = process_pool.submit(
                run_aggregator_process,
                aggregator_input_queues[i],
                bars_queue,
                self.bar_configuration.bar_interval_minutes,
                self.asset_symbols_to_uid_map,
                logger_config
            )
            futures.append(future)

        # --- Graceful Shutdown ---
        def handle_shutdown():
            self.logger.info("Shutdown signal received. Stopping all components.")
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
                self.logger.info("Application has shut down.")

        time.sleep(2)  # Give processes a moment to start and potentially fail
        for future in futures:
            if future.done() and future.exception() is not None:
                self.logger.critical(f"A worker process failed to start: {future.exception()}")
                self.logger.critical("Shutting down the application due to startup failure.")
                handle_shutdown()
                return  # Exit the live_update method

        # 5. Start the async Producer in the main process
        producer = self.ProducerClass(
            symbols=list(self.asset_symbols_to_uid_map.keys()),
            data_queue=trades_queue,  # Producer puts trades onto the shared queue
            logger=self.logger
        )
        loop = asyncio.get_event_loop()
        producer_task = loop.create_task(producer.run())



        signal.signal(signal.SIGINT, lambda s, f: handle_shutdown())
        signal.signal(signal.SIGTERM, lambda s, f: handle_shutdown())

        try:
            loop.run_until_complete(producer_task)
        except asyncio.CancelledError:
            self.logger.info("Producer task was cancelled.")
        finally:
            # Ensure final shutdown is called
            handle_shutdown()
