# file: producers/binance.py

import asyncio
import datetime
import queue
import ccxt.pro
from .base import ProducerBase

class BinanceTradesProducer(ProducerBase):
    """Producer for fetching trades from Binance."""

    def __init__(self, symbols: list, data_queue: queue.Queue, logger=None):
        super().__init__(data_queue, logger)
        self.symbols = symbols
        self.client = ccxt.pro.binance({'newUpdates': True})

    def _format_trade(self, trade: dict) -> tuple:
        """Formats a trade from ccxt into a tuple for the database."""
        timestamp = datetime.datetime.utcfromtimestamp(trade["timestamp"] / 1000)
        # Note: The 'symbol' from ccxt includes the quote currency (e.g., 'BTC/USDT')
        return (
            timestamp,
            trade['symbol'],
            str(trade['id']),
            float(trade['price']),
            float(trade['amount'])
        )

    async def _watch_loop_for_symbol(self, symbol: str):
        """Continuously watches trades for a single symbol."""
        self.logger.info(f"Starting trade watch loop for {symbol}")
        while True:
            try:
                trades = await self.client.watch_trades(symbol)
                for trade in trades:
                    formatted_trade = self._format_trade(trade)
                    self.queue.put(formatted_trade)
            except Exception as e:
                self.logger.error(f"Error in watch loop for {symbol}: {e}. Restarting.")
                await asyncio.sleep(5) # Wait before retrying

    async def run(self):
        self.logger.info(f"Starting Binance producer for {len(self.symbols)} symbols.")
        tasks = [self._watch_loop_for_symbol(s) for s in self.symbols]
        await asyncio.gather(*tasks)

    async def stop(self):
        self.logger.info("Closing Binance client connection.")
        await self.client.close()