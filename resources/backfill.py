"""Historical + gap backfill into the KDB-X HDB. Runs on the 4B.

Orchestration and the kdb write path. Binance HTTP lives in clients.binance;
parsing/normalising in resources.binance. Two fill paths:

- run(): loads complete archived days ([today-N .. yesterday]) into the HDB,
  idempotent (existing days skipped).
- bridge(): pages REST /api/v3/historicalTrades from the last stored id up to
  the live stream's first id, filling the current-day gap the archive can't
  cover.

prune() caps the store to a rolling window; RdbRoller savedowns the RDB at the
UTC day boundary for the live path. HdbWriter needs a licensed q; the rest is
covered by the tests. Design: platform/KDBX_SETUP.md.
"""
import logging
import os
import shutil
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable, Iterator, Optional

from clients import binance as binance_client
from resources import binance as binance_data

logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("backfill")


# --- date planning ---------------------------------------------------------

def plan_dates(today: date, days: int) -> list[date]:
    """The N complete days ending yesterday, ascending. Today is excluded (not
    in the archive yet)."""
    return [today - timedelta(days=n) for n in range(days, 0, -1)]


# --- HDB inspection --------------------------------------------------------

def _iter_partitions(hdb_path: Path) -> Iterator[tuple[date, Path]]:
    """Yield (date, dir) for each date-partition. .Q.dpft writes
    <hdb>/YYYY.MM.DD/trade/, so a partition is a date-named dir containing
    `trade`; skips the sym file and non-date dirs."""
    if not hdb_path.exists():
        return
    for entry in hdb_path.iterdir():
        if entry.is_dir() and (entry / "trade").exists():
            try:
                yield datetime.strptime(entry.name, "%Y.%m.%d").date(), entry
            except ValueError:
                continue


def existing_partition_dates(hdb_path: Path) -> set[date]:
    """Dates already written to the HDB (lets run() skip work)."""
    return {d for d, _ in _iter_partitions(hdb_path)}


def latest_partition_col(hdb_path: Path, table: str, col: str) -> Optional[Path]:
    """Path to `col`'s column file in the newest partition holding `table`, or
    None. Binance ids are monotonic, so that column's max is the whole HDB's
    max — reading one column file avoids loading the HDB."""
    dates = []
    if hdb_path.exists():
        for entry in hdb_path.iterdir():
            if entry.is_dir() and (entry / table).exists():
                try:
                    dates.append((datetime.strptime(entry.name, "%Y.%m.%d").date(), entry))
                except ValueError:
                    continue
    if not dates:
        return None
    return max(dates)[1] / table / col


def latest_partition_tradeid_path(hdb_path: Path) -> Optional[Path]:
    return latest_partition_col(hdb_path, "trade", "tradeID")


# --- write path (pykx bridge; needs a licensed q) --------------------------

class HdbWriter:
    """Writes to the HDB via schema.q's insertRaw + savedown. Needs a licensed
    q; unit tests use a fake writer instead."""

    def __init__(self, hdb_path: Path, schema_q: Path):
        import pykx as kx  # deferred: pure-Python paths and tests need no pykx
        self._kx = kx
        self._hdb_path = Path(hdb_path).resolve()
        self._hdb_path.mkdir(parents=True, exist_ok=True)  # .Q.dpft writes here
        # \l needs an absolute path: pykx's q resolves it against its own cwd.
        kx.q(f"\\l {Path(schema_q).resolve().as_posix()}")
        # A Python str becomes a q symbol in pykx, so pass a SymbolAtom and hsym
        # it (casting a symbol with `$ would throw 'type).
        kx.q("{HDB::hsym x}", kx.SymbolAtom(str(self._hdb_path)))
        logger.info("kdb writer ready; HDB=%s", self._hdb_path)

    def _table(self, cols: dict):
        kx = self._kx
        return kx.Table(data={
            "time": kx.LongVector(cols["time"]),
            "sym": kx.SymbolVector(cols["sym"]),
            "venue": kx.SymbolVector(cols["venue"]),
            "tradeID": kx.LongVector(cols["tradeID"]),
            "price": kx.FloatVector(cols["price"]),
            "qty": kx.FloatVector(cols["qty"]),
            "eventTime": kx.LongVector(cols["eventTime"]),
            "buyerMaker": kx.BooleanVector(cols["buyerMaker"]),
        })

    def _quote_table(self, cols: dict):
        kx = self._kx
        return kx.Table(data={
            "time": kx.LongVector(cols["time"]),
            "sym": kx.SymbolVector(cols["sym"]),
            "venue": kx.SymbolVector(cols["venue"]),
            "updateID": kx.LongVector(cols["updateID"]),
            "bid": kx.FloatVector(cols["bid"]),
            "bidSize": kx.FloatVector(cols["bidSize"]),
            "ask": kx.FloatVector(cols["ask"]),
            "askSize": kx.FloatVector(cols["askSize"]),
        })

    def insert(self, cols: dict) -> int:
        """Insert a chunk into the in-memory RDB `trade` (no savedown)."""
        return int(self._kx.q("insertRaw", self._table(cols)).py())

    def insert_quote(self, cols: dict) -> int:
        """Insert a chunk into the in-memory RDB `quote` (no savedown)."""
        return int(self._kx.q("insertRawQuote", self._quote_table(cols)).py())

    def savedown(self, day: date) -> None:
        """Persist the RDB (trade + quote) to the HDB partition, then clear."""
        self._kx.q("savedown", day)  # .Q.dpft on both tables

    def _max_id(self, table: str, col: str) -> Optional[int]:
        kx = self._kx
        if int(kx.q(f"count {table}").py()) > 0:      # RDB has today's data
            return int(kx.q(f"exec max {col} from {table}").py())
        # RDB empty: read the newest partition's id column directly. No \l, so
        # it neither clobbers the live table nor hits 'nyi from exec-over-parts.
        path = latest_partition_col(self._hdb_path, table, col)
        if path is None:
            return None
        return int(kx.q("{max get hsym x}", kx.SymbolAtom(str(path))).py())

    def max_stored_id(self) -> Optional[int]:
        """Highest trade_id in the store — the trade gap bridge fetches from here."""
        return self._max_id("trade", "tradeID")

    def max_stored_quote_id(self) -> Optional[int]:
        """Highest quote update_id in the store — for restart-safe quote dedup."""
        return self._max_id("quote", "updateID")

    def write_day(self, day: date, chunks: Iterable[dict]) -> int:
        """Backfill a whole archived day: insert its chunks, then savedown."""
        total = sum(self.insert(cols) for cols in chunks)
        self.savedown(day)
        return total


# --- live RDB rollover ------------------------------------------------------

def _utc_date(ms: int) -> date:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).date()


class RdbRoller:
    """Live-path RDB lifecycle for BOTH tables: accumulate the current day's
    trades and quotes in the RDB and savedown the finished day (trade + quote)
    when the feed crosses UTC midnight.

    The day comes from the data (a chunk's last time), so the roll fires when
    the first record of the new day arrives — no timer needed. Trades and quotes
    share one day boundary (whichever crosses first rolls both). A chunk
    straddling the boundary is negligible for a liquid symbol; an illiquid one
    could add a midnight timer that calls flush().
    """

    def __init__(self, writer):
        self._writer = writer
        self._current_day: Optional[date] = None

    def _roll(self, day: date) -> Optional[date]:
        """Savedown + advance if `day` is past the current one. Never moves the
        day backward (late cross-boundary records stay in the current day)."""
        if self._current_day is None:
            self._current_day = day
            return None
        if day > self._current_day:
            self._writer.savedown(self._current_day)  # persist + clear both tables
            rolled, self._current_day = self._current_day, day
            return rolled
        return None

    def feed(self, cols: dict) -> dict:
        rolled = self._roll(_utc_date(cols["time"][-1]))
        return {"inserted": self._writer.insert(cols),
                "day": self._current_day, "rolled": rolled}

    def feed_quote(self, cols: dict) -> dict:
        rolled = self._roll(_utc_date(cols["time"][-1]))
        return {"inserted": self._writer.insert_quote(cols),
                "day": self._current_day, "rolled": rolled}

    def flush(self) -> Optional[date]:
        """Savedown the current day (shutdown / manual). Returns it, or None."""
        if self._current_day is None:
            return None
        day, self._current_day = self._current_day, None
        self._writer.savedown(day)
        return day


# --- config ----------------------------------------------------------------

@dataclass(frozen=True)
class BackfillConfig:
    symbol: str
    venue: str
    days: int
    chunk_size: int
    hdb_path: Path
    schema_q: Path
    api_key: Optional[str] = None    # Binance key for the REST bridge (historicalTrades)
    mqtt_host: str = "192.168.100.2"  # live feed broker (the 4B's own mosquitto)
    mqtt_port: int = 1883

    @classmethod
    def from_env(cls) -> "BackfillConfig":
        return cls(
            symbol=os.environ.get("SYMBOL", "BTCUSDT").upper(),
            venue=os.environ.get("VENUE", "binance"),
            days=int(os.environ.get("BACKFILL_DAYS", "30")),
            chunk_size=int(os.environ.get("BACKFILL_CHUNK", "500000")),
            hdb_path=Path(os.environ.get("HDB_PATH", Path.home() / "nano_tick_hdb")),
            schema_q=Path(__file__).resolve().parent.parent / "platform" / "schema.q",
            api_key=os.environ.get("BINANCE_API_KEY"),
            mqtt_host=os.environ.get("MQTT_HOST", "192.168.100.2"),
            mqtt_port=int(os.environ.get("MQTT_PORT", "1883")),
        )


# --- archive backfill ------------------------------------------------------

def run_backfill(
    dates: list[date],
    existing_dates: set[date],
    fetch_day: Callable[[date], Optional[Iterable[dict]]],
    writer,
) -> dict:
    """Fetch and write each missing day. fetch_day returns chunk iterables, or
    None if the archive lacks that day. Injectable for tests."""
    summary = {"written": 0, "rows": 0, "skipped": 0, "missing": 0, "failed": 0}
    for day in dates:
        if day in existing_dates:
            summary["skipped"] += 1
            continue
        try:
            chunks = fetch_day(day)
            if chunks is None:
                logger.warning("archive has no data for %s yet; skipping", day)
                summary["missing"] += 1
                continue
            rows = writer.write_day(day, chunks)
            summary["written"] += 1
            summary["rows"] += rows
            logger.info("wrote %s: %d trades", day, rows)
        except Exception:
            logger.exception("failed to backfill %s", day)
            summary["failed"] += 1
    return summary


def make_fetch_day(symbol: str, venue: str, chunk_size: int) -> Callable[[date], Optional[Iterator[dict]]]:
    """Real fetch: download the day's zip, verify its checksum, and return a
    streaming chunk iterator (or None if the day isn't published)."""
    def fetch_day(day: date) -> Optional[Iterator[dict]]:
        zip_bytes = binance_client.download_archive(symbol, day)
        if zip_bytes is None:
            return None
        checksum = binance_client.download_checksum(symbol, day)
        if checksum is not None and not binance_data.verify_checksum(zip_bytes, checksum.decode()):
            raise ValueError(f"checksum mismatch for {symbol} {day}")
        return binance_data.iter_trade_chunks(zip_bytes, symbol, venue, chunk_size)
    return fetch_day


def run(config: BackfillConfig, writer: Optional[HdbWriter] = None) -> dict:
    """Ensure the last config.days complete days are in the HDB.

    Idempotent, so it's safe to call on every startup. Pass `writer` to reuse
    the app's kdb connection; otherwise one is built from config. Returns a
    summary.
    """
    dates = plan_dates(datetime.now(timezone.utc).date(), config.days)
    existing = existing_partition_dates(config.hdb_path)
    logger.info(
        "backfill %s: %d days [%s .. %s], %d already in HDB",
        config.symbol, config.days, dates[0], dates[-1], len(existing & set(dates)),
    )
    if writer is None:
        writer = HdbWriter(config.hdb_path, config.schema_q)
    fetch_day = make_fetch_day(config.symbol, config.venue, config.chunk_size)
    summary = run_backfill(dates, existing, fetch_day, writer)
    logger.info(
        "backfill done: %(written)d days written (%(rows)d trades), "
        "%(skipped)d already present, %(missing)d not published, %(failed)d failed",
        summary,
    )
    return summary


# --- current-day gap bridge (REST) -----------------------------------------

def bridge(config: BackfillConfig, writer: HdbWriter, target_id: Optional[int] = None,
           page: int = binance_client.TRADES_LIMIT) -> dict:
    """Fill trades from the last stored id up to `target_id` (exclusive — the
    live stream's first id) by paging REST. With target_id=None, fills up to
    the current tip. Dedup is downstream by tradeID; here we just page forward.
    """
    floor = writer.max_stored_id()
    if floor is None:
        logger.warning("no stored trade_id to bridge from; skipping REST bridge")
        return {"inserted": 0, "from_id": None}
    start = floor + 1
    inserted = 0
    while target_id is None or start < target_id:
        trades = binance_client.fetch_trades(config.symbol, start, page, config.api_key)
        if not trades:
            break
        if target_id is not None:
            trades = [t for t in trades if int(t["id"]) < target_id]
            if not trades:
                break
        inserted += writer.insert(binance_data.parse_rest_trades(trades, config.symbol, config.venue))
        start = int(trades[-1]["id"]) + 1
        if len(trades) < page:  # caught up to the live tip
            break
    logger.info("bridge: inserted %d trades from id %d", inserted, floor + 1)
    return {"inserted": inserted, "from_id": floor + 1}


# --- retention ------------------------------------------------------------

def prune(config: BackfillConfig, today: Optional[date] = None) -> dict:
    """Delete partitions older than the window (today - config.days) so the HDB
    stays a fixed rolling window. run() fills the window; prune() caps it.

    The cutoff is backfill's oldest day, so prune never deletes a day run()
    just wrote. Filesystem-only, idempotent; only date-partition dirs are
    removed. Returns a summary.
    """
    today = today or datetime.now(timezone.utc).date()
    cutoff = today - timedelta(days=config.days)
    removed = []
    for day, path in sorted(_iter_partitions(config.hdb_path)):
        if day < cutoff:
            shutil.rmtree(path)
            removed.append(day)
            logger.info("pruned partition %s", day)
    logger.info(
        "prune: removed %d partitions before %s (retention %d days)",
        len(removed), cutoff, config.days,
    )
    return {"removed": len(removed), "cutoff": cutoff}
