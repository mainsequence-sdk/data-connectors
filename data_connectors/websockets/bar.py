# file: bars.py

import queue
import datetime
import logging
from dataclasses import asdict, dataclass, field

@dataclass
class Bar:
    """Represents the state of a single OHLCV bar, including VWAP and trade times."""
    symbol: str
    interval_minutes: int
    timestamp: datetime.datetime
    open: float
    first_trade_time: datetime.datetime
    volume: float = 0.0
    turnover: float = 0.0
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

    @property
    def vwap(self) -> float:
        return self.turnover / self.volume if self.volume != 0 else self.close

    def to_dict(self) -> dict:
        data = asdict(self)
        data['vwap'] = self.vwap
        data['timestamp'] = self.timestamp.isoformat()
        data['first_trade_time'] = self.first_trade_time.isoformat()
        data['last_trade_time'] = self.last_trade_time.isoformat()
        return data

class BarAggregator:
    """
    Consumes trades, aggregates them into bars.
    Designed to be run as the target of a multiprocessing.Process.
    """
    def __init__(self, input_queue: queue.Queue, output_queue: queue.Queue, interval_minutes: int, logger: logging.Logger):
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.interval_minutes = interval_minutes
        self.logger = logger
        self.pending_bars = {}

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

            bar_start_ts = self._get_bar_start_timestamp(trade_ts)
            bar_key = (symbol, bar_start_ts)

            # Flush any completed bars before processing the new trade
            completed_keys = [k for k in self.pending_bars if k[0] == symbol and k[1] < bar_start_ts]
            for key in completed_keys:
                self.output_queue.put(self.pending_bars.pop(key).to_dict())

            # Update or create the bar for the current trade
            if bar_key not in self.pending_bars:
                bar_end_ts = bar_start_ts + datetime.timedelta(minutes=self.interval_minutes)
                self.pending_bars[bar_key] = Bar(symbol=symbol, interval_minutes=self.interval_minutes, timestamp=bar_end_ts, open=price, volume=quantity, first_trade_time=trade_ts)
            else:
                self.pending_bars[bar_key].update(price=price, quantity=quantity, trade_time=trade_ts)

    def _get_bar_start_timestamp(self, ts: datetime.datetime) -> datetime.datetime:
        """Calculates the timestamp for the beginning of the bar's interval bucket."""
        minute = (ts.minute // self.interval_minutes) * self.interval_minutes
        return ts.replace(minute=minute, second=0, microsecond=0)