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


def latest_partition_tradeid_path(hdb_path: Path) -> Optional[Path]:
    """Path to the tradeID column file of the newest partition, or None if the
    HDB is empty. Binance ids are monotonic, so that column's max is the whole
    HDB's max — and reading one column file avoids loading the HDB."""
    parts = existing_partition_dates(hdb_path)
    if not parts:
        return None
    return hdb_path / max(parts).strftime("%Y.%m.%d") / "trade" / "tradeID"


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

    def insert(self, cols: dict) -> int:
        """Insert a chunk into the in-memory RDB `trade` (no savedown)."""
        return int(self._kx.q("insertRaw", self._table(cols)).py())

    def savedown(self, day: date) -> None:
        """Persist the RDB to the HDB as `day`'s partition, then clear it."""
        self._kx.q("savedown", day)  # .Q.dpft

    def max_stored_id(self) -> Optional[int]:
        """Highest trade_id in the store: the RDB when it holds today's data,
        else the newest HDB partition. The gap bridge fetches from here."""
        kx = self._kx
        if int(kx.q("count trade").py()) > 0:
            return int(kx.q("exec max tradeID from trade").py())
        # RDB empty (fresh start): read the tradeID column of the newest
        # partition directly. No \l, so it neither clobbers the live `trade`
        # nor hits 'nyi from exec-over-partitions, and stays in this one q.
        col = latest_partition_tradeid_path(self._hdb_path)
        if col is None:
            return None
        return int(kx.q("{max get hsym x}", kx.SymbolAtom(str(col))).py())

    def write_day(self, day: date, chunks: Iterable[dict]) -> int:
        """Backfill a whole archived day: insert its chunks, then savedown."""
        total = sum(self.insert(cols) for cols in chunks)
        self.savedown(day)
        return total


# --- live RDB rollover ------------------------------------------------------

def _utc_date(ms: int) -> date:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).date()


class RdbRoller:
    """Live-path RDB lifecycle: accumulate the current day's ticks in the RDB
    and savedown the finished day when the feed crosses UTC midnight.

    The day comes from the data (the chunk's last trade time), so the roll
    fires when the first tick of the new day arrives — no timer needed. A chunk
    straddling the boundary is negligible for a liquid symbol; an illiquid one
    could add a midnight timer that calls flush().
    """

    def __init__(self, writer):
        self._writer = writer
        self._current_day: Optional[date] = None

    def feed(self, cols: dict) -> dict:
        """Insert a live chunk, savedown-ing the previous day first if this
        chunk has crossed into a new UTC day."""
        day = _utc_date(cols["time"][-1])
        rolled = None
        if self._current_day is not None and day > self._current_day:
            self._writer.savedown(self._current_day)  # persist + clear finished day
            rolled = self._current_day
        self._current_day = day
        inserted = self._writer.insert(cols)
        return {"inserted": inserted, "day": day, "rolled": rolled}

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
    api_key: Optional[str] = None  # Binance key for the REST bridge (historicalTrades)

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
