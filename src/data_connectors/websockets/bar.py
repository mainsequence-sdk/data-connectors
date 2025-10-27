# file: bars.py

import queue
import datetime
import logging
from dataclasses import asdict, dataclass, field, fields
from typing import Dict,Optional
@dataclass
class Bar:
    """Represents the state of a single OHLCV bar, including VWAP and trade times."""
    unique_identifier: str
    interval_minutes: int
    time_index: datetime.datetime
    open: float
    first_trade_time: datetime.datetime
    volume: float = 0.0
    turnover: float = 0.0
    trade_count: int = 1  # ADDED: Initialize trade count with the first trade.
    high: float = field(init=False)
    low: float = field(init=False)
    close: float = field(init=False)
    last_trade_time: datetime.datetime = field(init=False)

    def __post_init__(self):
        self.high = self.open
        self.low = self.open
        self.close = self.open
        self.turnover = self.open * self.volume
        self.last_trade_time = self.first_trade_time

    def update(self, price: float, quantity: float, trade_time: datetime.datetime):
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price
        self.volume += quantity
        self.turnover += (price * quantity)
        self.last_trade_time = trade_time
        self.trade_count += 1  # ADDED: Increment trade count on each update.


    @property
    def vwap(self) -> float:
        return self.turnover / self.volume if self.volume != 0 else self.close

    def to_dict(self) -> dict:
        data = asdict(self)
        data.pop("interval_minutes",None)
        data['vwap'] = self.vwap
        data['first_trade_time'] = self.first_trade_time.timestamp()
        data['time_index'] = self.time_index.isoformat()
        data['last_trade_time'] = self.last_trade_time.timestamp()
        return data

    @classmethod
    def get_dtypes(cls) -> dict:
        """
        Returns a dictionary of field names to their string data types,
        matching the structure of the dictionary from to_dict().
        This is analogous to a Pandas DataFrame's dtypes.
        """
        # Start with the original Python types from the dataclass definition
        # Use __name__ to get the simple string representation (e.g., 'str', 'int')
        dtypes = {f.name: f.type.__name__ for f in fields(cls) if f.name != "interval_minutes"}

        # Manually override the types for fields that are transformed or have special names
        dtypes['time_index'] = "datetime64[ns, UTC]"
        dtypes['first_trade_time'] = "float"
        dtypes['last_trade_time'] = "float"
        dtypes['trade_count'] = "int64"

        # Add the type for the calculated 'vwap' property
        dtypes['vwap'] = "float"

        return dtypes

class BarAggregator:
    """
    Consumes trades, aggregates them into bars.
    Designed to be run as the target of a multiprocessing.Process.
    """
    def __init__(self, input_queue: queue.Queue, output_queue: queue.Queue, interval_minutes: int, logger: logging.Logger,
                 asset_symbols_to_uid_map:Dict[str,str]
                 ):
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.interval_minutes = interval_minutes
        self.logger = logger
        self.pending_bars = {}
        self.asset_map = asset_symbols_to_uid_map # Store the map


    def run(self):
        """The main loop that pulls trades and processes them into bars."""
        self.logger.info(f"Aggregator process started for {self.interval_minutes}-min bars.")
        while True:
            try:
                trade = self.input_queue.get(timeout=1.0)
                trade_ts, symbol, _, price, quantity, *_ = trade
            except queue.Empty:
                continue
            except (IOError, EOFError):
                self.logger.warning("Input queue connection lost. Exiting.")
                break
            except Exception as e:
                self.logger.exception(f"An error occurred in the aggregator process: {e}")
                continue

            uid = self.asset_map.get(symbol)
            if not uid:
                self.logger.warning(f"No UID found for symbol '{symbol}'. Skipping trade.")
                continue

            bar_start_ts = self._get_bar_start_timestamp(trade_ts)
            # The key for the dictionary is now (uid, timestamp)
            bar_key = (uid, bar_start_ts)

            # Flush any completed bars before processing the new trade
            completed_keys = [k for k in self.pending_bars if k[0] == uid and k[1] < bar_start_ts]
            for key in completed_keys:
                self.output_queue.put(self.pending_bars.pop(key).to_dict())

            # Update or create the bar for the current trade
            if bar_key not in self.pending_bars:
                bar_end_ts = bar_start_ts + datetime.timedelta(minutes=self.interval_minutes)
                self.pending_bars[bar_key] = Bar(unique_identifier=uid,
                                                 interval_minutes=self.interval_minutes,
                                                 time_index=bar_end_ts,
                                                 open=price,
                                                 volume=quantity, first_trade_time=trade_ts)
            else:
                self.pending_bars[bar_key].update(price=price, quantity=quantity, trade_time=trade_ts)

    def _get_bar_start_timestamp(self, ts: datetime.datetime) -> datetime.datetime:
        """Calculates the timestamp for the beginning of the bar's interval bucket."""
        minute = (ts.minute // self.interval_minutes) * self.interval_minutes
        return ts.replace(minute=minute, second=0, microsecond=0)