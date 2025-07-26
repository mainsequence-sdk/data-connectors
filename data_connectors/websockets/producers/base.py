# file: producers/base.py

import asyncio
import queue
import datetime
import ccxt.pro
import logging
from abc import ABC, abstractmethod

class ProducerBase(ABC):
    """Abstract base class for all data producers."""

    def __init__(self, data_queue: queue.Queue, logger=None):
        self.queue = data_queue
        self.logger = logger or logging.getLogger(__name__)

    @abstractmethod
    async def run(self):
        """
        The main async method to start fetching data and putting it on the queue.
        Subclasses MUST implement this.
        """
        raise NotImplementedError

    @abstractmethod
    async def stop(self):
        """
        Method for gracefully cleaning up resources (e.g., closing websocket clients).
        Subclasses MUST implement this.
        """
        raise NotImplementedError


