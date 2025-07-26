# file: consumers.py

import queue
import json
import logging
import time


class DataWriter:
    """
    Consumes final data from a queue and sends it as JSON.
    Designed to be run as the target of a multiprocessing.Process.
    """

    def __init__(self, data_queue: queue.Queue, logger: logging.Logger):
        self.queue = data_queue
        self.logger = logger

    def run(self):
        """The main loop that pulls data from the queue and logs it."""
        self.logger.info("Writer process started.")
        while True:
            try:
                # Using a batching mechanism can be more efficient for the final output
                batch = self._get_batch()
                if not batch:
                    continue

                # In a real app, you would send this batch to an API, database, etc.
                self.logger.info(f"WRITING BATCH of {len(batch)} bars.")
                # For demonstration, log the first item in the batch
                if batch:
                    self.logger.info(f"Sample bar: {json.dumps(batch[0])}")

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