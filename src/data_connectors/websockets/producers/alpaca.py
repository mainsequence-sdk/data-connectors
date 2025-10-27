# file: producers/alpaca.py

import os
import asyncio
import queue
from alpaca.data.live import StockDataStream
from .base import ProducerBase


class AlpacaTradesProducer(ProducerBase):
    """Producer for fetching trades from Alpaca."""

    def __init__(self, symbols: list, data_queue: queue.Queue, logger=None):
        super().__init__(data_queue, logger)
        self.symbols = symbols
        self.client = StockDataStream(
            os.getenv("ALPACA_API_KEY"),
            os.getenv("ALPACA_SECRET_KEY")
        )

    def _format_trade(self, trade) -> tuple:
        """Formats an Alpaca trade object into a tuple for the database."""
        # Note: The 'symbol' from Alpaca is just the ticker (e.g., 'SPY')
        return (
            trade.timestamp,
            trade.symbol,
            str(trade.id),
            trade.price,
            trade.size
        )

    async def _trade_callback(self, trade):
        """Callback executed by the client for each received trade."""
        formatted_trade = self._format_trade(trade)
        self.queue.put(formatted_trade)

    async def run(self):
        """
        Subscribes to trades and runs the client directly. This is the corrected implementation.
        """
        self.logger.info(f"Starting Alpaca producer for symbols: {self.symbols}")
        self.client.subscribe_trades(self._trade_callback, *self.symbols)

        # THE FIX: The client's run() method is awaitable and should be called directly.
        # There is no need for run_in_executor.
        await self.client.run()

    async def stop(self):
        self.logger.info("Closing Alpaca client connection.")
        await self.client.stop_ws()