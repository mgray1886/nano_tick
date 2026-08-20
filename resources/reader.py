"""Read side of the KDB-X store: q analytics -> pandas DataFrames (Phase 3).

Thin pykx wrapper over platform/analytics.q. Loads the date-partitioned HDB
and the analytics functions into an embedded q, then exposes the HDB entry
points (bars, quoteBars, featureTable, vwapDay, counts) returning pandas
objects ready for modelling.

MUST run in a SEPARATE process from any writer (HdbWriter / the recorder): a
table name is in-memory OR partitioned in a given q, never both. This is the
reader, so it only ever maps partitions read-only.

HdbReader needs a licensed q; the pure helpers (bar-size / date coercion,
config) are covered by tests. Live coverage is against a real HDB in WSL.
See platform/analytics.q for the q-side definitions.
"""
import logging
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional, Union

logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("reader")

BarSize = Union[timedelta, int, float]   # timedelta, or a number of SECONDS
DateLike = Union[date, datetime, str]    # date/datetime, or "YYYY.MM.DD" / "YYYY-MM-DD"

_NS_PER_SEC = 1_000_000_000


# --- pure helpers (unit-tested) --------------------------------------------

def to_bar_ns(size: BarSize) -> int:
    """Bar width as q-timespan nanoseconds. Accepts a timedelta or a number of
    seconds (int/float). Must be strictly positive."""
    if isinstance(size, timedelta):
        ns = round(size.total_seconds() * _NS_PER_SEC)
    elif isinstance(size, bool):                      # bool is an int subclass; reject it
        raise TypeError("bar size must be a timedelta or a number of seconds")
    elif isinstance(size, (int, float)):
        ns = round(size * _NS_PER_SEC)
    else:
        raise TypeError("bar size must be a timedelta or a number of seconds")
    if ns <= 0:
        raise ValueError(f"bar size must be positive, got {size!r}")
    return ns


def coerce_date(value: DateLike) -> date:
    """Normalise a date/datetime/str to a datetime.date. Strings may use q's
    dotted form (2026.08.13) or ISO dashes (2026-08-13)."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return datetime.strptime(value.strip().replace("-", "."), "%Y.%m.%d").date()
    raise TypeError(f"expected date/datetime/str, got {type(value).__name__}")


# --- config ----------------------------------------------------------------

@dataclass(frozen=True)
class ReaderConfig:
    symbol: str
    hdb_path: Path
    analytics_q: Path
    bar_seconds: int = 60          # default bar width for the convenience methods
    horizon: int = 1              # label horizon h (bars ahead)
    window: int = 20              # trailing window w for rolling features
    cost: float = 0.002           # round-trip cost as a return (Binance taker ~0.1%/side)

    @classmethod
    def from_env(cls) -> "ReaderConfig":
        return cls(
            symbol=os.environ.get("SYMBOL", "BTCUSDT").upper(),
            hdb_path=Path(os.environ.get("HDB_PATH", Path.home() / "nano_tick_hdb")),
            analytics_q=Path(__file__).resolve().parent.parent / "platform" / "analytics.q",
            bar_seconds=int(os.environ.get("BAR_SECONDS", "60")),
            horizon=int(os.environ.get("LABEL_HORIZON", "1")),
            window=int(os.environ.get("FEATURE_WINDOW", "20")),
            cost=float(os.environ.get("ROUNDTRIP_COST", "0.002")),
        )


# --- query side (pykx; needs a licensed q) ---------------------------------

class HdbReader:
    """Query the trade/quote HDB through analytics.q, returning pandas.

    Loads the HDB (mapping the partitions read-only) then analytics.q. `\\l`
    chdirs into the HDB dir, so analytics.q is loaded by ABSOLUTE path
    afterwards (a relative path would resolve against the HDB dir and miss).
    """

    def __init__(self, hdb_path: Union[str, Path], analytics_q: Union[str, Path], kx=None):
        if kx is None:
            import pykx as kx  # deferred: pure helpers / tests need no pykx
        self._kx = kx
        self._hdb_path = Path(hdb_path).resolve()
        # Resolve analytics.q BEFORE loading the HDB: `\l <hdb>` chdirs the whole
        # process into the HDB dir, so a relative path resolved afterwards would
        # miss (Path.resolve uses the process cwd).
        analytics_abs = Path(analytics_q).resolve()
        if not self._hdb_path.exists():
            raise FileNotFoundError(f"HDB not found: {self._hdb_path}")
        if not analytics_abs.exists():
            raise FileNotFoundError(f"analytics.q not found: {analytics_abs}")
        kx.q(f"\\l {self._hdb_path.as_posix()}")        # map partitions (chdirs process)
        kx.q(f"\\l {analytics_abs.as_posix()}")         # then analytics by pre-resolved abs path
        logger.info("kdb reader ready; HDB=%s", self._hdb_path)

    # -- argument coercion to q atoms --
    def _sym(self, symbol: str):
        return self._kx.SymbolAtom(symbol)

    def _date(self, day: DateLike):
        return self._kx.DateAtom(coerce_date(day))

    def _span(self, size: BarSize):
        # cast ns long -> q timespan; avoids depending on numpy for the temporal type
        return self._kx.q("`timespan$", self._kx.LongAtom(to_bar_ns(size)))

    # -- HDB entry points (see platform/analytics.q) --
    def bars(self, symbol: str, day: DateLike, size: Optional[BarSize] = None):
        """OHLCV + order-flow bars for one symbol/day, indexed by bar time."""
        size = self._default_size(size)
        return self._kx.q("bars", self._sym(symbol), self._date(day), self._span(size)).pd()

    def quote_bars(self, symbol: str, day: DateLike, size: Optional[BarSize] = None):
        """Best bid/ask features (mid, spread, micro, qimb) per bar."""
        size = self._default_size(size)
        return self._kx.q("quoteBars", self._sym(symbol), self._date(day), self._span(size)).pd()

    def feature_table(self, symbol: str, day: DateLike, size: Optional[BarSize] = None,
                      horizon: Optional[int] = None, window: Optional[int] = None,
                      cost: Optional[float] = None):
        """Labelled feature table (trailing features + cost-aware forward label),
        warm-up head and unlabelled tail already dropped. Ready for modelling."""
        size = self._default_size(size)
        h = self._config_default(horizon, "horizon", 1)
        w = self._config_default(window, "window", 20)
        c = self._config_default(cost, "cost", 0.002)
        return self._kx.q("featureTable", self._sym(symbol), self._date(day), self._span(size),
                          self._kx.LongAtom(int(h)), self._kx.LongAtom(int(w)),
                          self._kx.FloatAtom(float(c))).pd()

    def vwap_day(self, symbol: str, day: DateLike) -> float:
        """Whole-day VWAP for a symbol."""
        return float(self._kx.q("vwapDay", self._sym(symbol), self._date(day)).py())

    def counts(self, symbol: str):
        """Trades per day for a symbol (data-coverage sanity check), by date."""
        return self._kx.q("counts", self._sym(symbol)).pd()

    # -- defaults from an optional attached config --
    def _default_size(self, size: Optional[BarSize]) -> BarSize:
        if size is not None:
            return size
        cfg = getattr(self, "config", None)
        return cfg.bar_seconds if cfg is not None else 60

    def _config_default(self, value, attr, fallback):
        if value is not None:
            return value
        cfg = getattr(self, "config", None)
        return getattr(cfg, attr) if cfg is not None else fallback


def open_reader(config: ReaderConfig, kx=None) -> HdbReader:
    """Build an HdbReader from config and attach it, so the convenience methods
    pick up the configured bar size / horizon / window / cost as defaults."""
    reader = HdbReader(config.hdb_path, config.analytics_q, kx=kx)
    reader.config = config
    return reader
