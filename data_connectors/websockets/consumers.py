# file: consumers.py

import queue
import json
import logging
import time
import mainsequence.client as ms_client
from concurrent.futures import ThreadPoolExecutor

class DataWriter:
    """
    Consumes final data from a queue and sends it as JSON.
    Designed to be run as the target of a multiprocessing.Process.
    """

    def __init__(self,
                 local_metadata_id:int,
                 data_queue: queue.Queue, logger: logging.Logger):
        self.queue = data_queue
        self.logger = logger
        self.executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="ApiWorker")
        self.local_metadata_id=local_metadata_id


    def _send_batch_to_api(self, batch: list):
        """
        This function contains the blocking I/O call and is executed by a worker thread.
        """
        try:
            # Using a session object is more efficient for multiple requests
            # Submit the blocking REST call to the thread pool.
            # This call is non-blocking and returns immediately.
            ms_client.LocalTimeSerie.insert_data_into_table(local_metadata_id=self.local_metadata_id,
                                                            records=batch)
            if batch:
                self.logger.info(f"Sample bar: {json.dumps(batch[0])}")

        except Exception as e:
            self.logger.error(f"Failed to submit batch to API: {e}")
            # Here you could implement logic to re-queue the failed batch if needed

    def run(self):
        """The main loop that pulls data from the queue and logs it."""
        self.logger.info("Writer process started.")
        while True:
            try:
                # Using a batching mechanism can be more efficient for the final output
                batch = self._get_batch()
                if not batch:
                    continue

                self.logger.info(f"WRITING BATCH of {len(batch)} bars.")
                self.executor.submit(self._send_batch_to_api, batch)


            except (IOError, EOFError):
                self.logger.warning("Writer input queue connection lost. Exiting.")
                break
            except Exception as e:
                self.logger.exception(f"An error occurred in the writer process: {e}")

    def _get_batch(self, batch_size=100) -> list:
        """Collects a batch of items from the queue."""
        batch = []
        try:
            # Block for the first item with a timeout
            item = self.queue.get(timeout=1.0)
            batch.append(item)
        except queue.Empty:
            return None

        # Pull any other items already in the queue up to the batch size
        while len(batch) < batch_size:
            try:
                item = self.queue.get_nowait()
                batch.append(item)
            except queue.Empty:
                break  # Queue is empty
        return batch