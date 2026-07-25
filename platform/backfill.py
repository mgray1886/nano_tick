"""Historical trade backfill into the KDB-X HDB. Runs on the 4B.

Loads the last N complete days (default 30) of a symbol's spot trades from the
Binance archive (data.binance.vision) and writes each as an HDB partition via
schema.q's insertRaw/savedown. Idempotent - existing days are skipped.

Notes:
- Archive lags ~1 day, so this only covers [today-N .. yesterday]. The
  yesterday->live-first-id bridge lives with the feedhandler, not here.
- Uses `trades`, not `aggTrades`: same trade_id space as the @trade stream.
- Streams each day in chunks to bound memory (one day is millions of rows).

HdbWriter needs a licensed q; the rest is covered by test_backfill.py.
Config and design: platform/KDBX_SETUP.md.
"""
import csv
import hashlib
import io
import logging
import os
import shutil
import urllib.error
import urllib.request
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable, Iterator, Optional

logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("backfill")

ARCHIVE_BASE = "https://data.binance.vision/data/spot/daily/trades"
HTTP_TIMEOUT = 60
HTTP_RETRIES = 3

# schema.q trade columns. time/eventTime are epoch-millis longs here;
# insertRaw converts them to timestamps.
COLUMNS = ("time", "sym", "venue", "tradeID", "price", "qty", "eventTime", "buyerMaker")


# --- URLs ------------------------------------------------------------------

def archive_url(symbol: str, day: date) -> str:
    return f"{ARCHIVE_BASE}/{symbol}/{symbol}-trades-{day:%Y-%m-%d}.zip"


def checksum_url(symbol: str, day: date) -> str:
    return archive_url(symbol, day) + ".CHECKSUM"


# --- date planning ---------------------------------------------------------

def plan_dates(today: date, days: int) -> list[date]:
    """The N complete days ending yesterday, ascending. Today is excluded (not
    in the archive yet)."""
    return [today - timedelta(days=n) for n in range(days, 0, -1)]


# --- HTTP ------------------------------------------------------------------

def http_get(url: str, timeout: int = HTTP_TIMEOUT, retries: int = HTTP_RETRIES) -> Optional[bytes]:
    """GET url. Returns the body, None on 404 (day not published), or raises
    after `retries` failures."""
    last_exc: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as resp:
                return resp.read()
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return None
            last_exc = exc
        except (urllib.error.URLError, TimeoutError) as exc:
            last_exc = exc
        logger.warning("GET %s failed (attempt %d/%d): %s", url, attempt, retries, last_exc)
    raise RuntimeError(f"GET {url} failed after {retries} attempts") from last_exc


# --- integrity -------------------------------------------------------------

def verify_checksum(zip_bytes: bytes, checksum_text: str) -> bool:
    """Binance .CHECKSUM files are '<sha256>  <filename>'."""
    expected = checksum_text.split()[0].strip().lower()
    actual = hashlib.sha256(zip_bytes).hexdigest()
    return actual == expected


# --- parsing (streaming, chunked) ------------------------------------------
# Binance spot daily `trades` CSV columns (newer files carry a header row):
#   id, price, qty, quoteQty, time, isBuyerMaker, isBestMatch
_ID, _PRICE, _QTY, _TIME, _MAKER = 0, 1, 2, 4, 5


def _empty_cols() -> dict:
    return {c: [] for c in COLUMNS}


def iter_trade_chunks(
    zip_bytes: bytes, symbol: str, venue: str, chunk_size: int = 500_000
) -> Iterator[dict]:
    """Stream the day's CSV from the zip, yielding column-oriented chunks in
    schema.q's raw layout. Peak memory is ~one chunk, not a whole day."""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if len(names) != 1:
            raise ValueError(f"expected exactly one CSV in archive, found {names}")
        with zf.open(names[0]) as raw:
            reader = csv.reader(io.TextIOWrapper(raw, encoding="utf-8"))
            cols = _empty_cols()
            n = 0
            for row in reader:
                if not row or not row[_ID].lstrip("-").isdigit():
                    continue  # blank line or header row
                ts = int(row[_TIME])
                cols["time"].append(ts)
                cols["eventTime"].append(ts)  # archive has no emit time; use trade time
                cols["sym"].append(symbol)
                cols["venue"].append(venue)
                cols["tradeID"].append(int(row[_ID]))
                cols["price"].append(float(row[_PRICE]))
                cols["qty"].append(float(row[_QTY]))
                cols["buyerMaker"].append(row[_MAKER].strip().lower() == "true")
                n += 1
                if n >= chunk_size:
                    yield cols
                    cols = _empty_cols()
                    n = 0
            if n:
                yield cols


# --- idempotency -----------------------------------------------------------

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


# --- write path (pykx bridge; needs a licensed q) --------------------------

class HdbWriter:
    """Writes days to the HDB via schema.q's insertRaw + savedown. Needs a
    licensed q; unit tests use a fake writer instead."""

    def __init__(self, hdb_path: Path, schema_q: Path):
        import pykx as kx  # deferred: pure-Python paths and tests need no pykx
        self._kx = kx
        hdb_path = Path(hdb_path).resolve()
        hdb_path.mkdir(parents=True, exist_ok=True)  # .Q.dpft writes partitions here
        # \l needs an absolute path: pykx's q resolves it against its own cwd.
        kx.q(f"\\l {Path(schema_q).resolve().as_posix()}")
        # A Python str becomes a q symbol in pykx, so pass a SymbolAtom and hsym
        # it (casting a symbol with `$ would throw 'type).
        kx.q("{HDB::hsym x}", kx.SymbolAtom(str(hdb_path)))
        logger.info("kdb writer ready; HDB=%s", hdb_path)

    def write_day(self, day: date, chunks: Iterable[dict]) -> int:
        kx = self._kx
        total = 0
        for cols in chunks:
            table = kx.Table(data={
                "time": kx.LongVector(cols["time"]),
                "sym": kx.SymbolVector(cols["sym"]),
                "venue": kx.SymbolVector(cols["venue"]),
                "tradeID": kx.LongVector(cols["tradeID"]),
                "price": kx.FloatVector(cols["price"]),
                "qty": kx.FloatVector(cols["qty"]),
                "eventTime": kx.LongVector(cols["eventTime"]),
                "buyerMaker": kx.BooleanVector(cols["buyerMaker"]),
            })
            total += int(kx.q("insertRaw", table).py())
        kx.q("savedown", day)  # .Q.dpft writes the partition and clears the RDB
        return total


# --- orchestration ---------------------------------------------------------

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
        zip_bytes = http_get(archive_url(symbol, day))
        if zip_bytes is None:
            return None
        checksum = http_get(checksum_url(symbol, day))
        if checksum is not None and not verify_checksum(zip_bytes, checksum.decode()):
            raise ValueError(f"checksum mismatch for {symbol} {day}")
        return iter_trade_chunks(zip_bytes, symbol, venue, chunk_size)
    return fetch_day


@dataclass(frozen=True)
class BackfillConfig:
    symbol: str
    venue: str
    days: int
    chunk_size: int
    hdb_path: Path
    schema_q: Path

    @classmethod
    def from_env(cls) -> "BackfillConfig":
        return cls(
            symbol=os.environ.get("SYMBOL", "BTCUSDT").upper(),
            venue=os.environ.get("VENUE", "binance"),
            days=int(os.environ.get("BACKFILL_DAYS", "30")),
            chunk_size=int(os.environ.get("BACKFILL_CHUNK", "500000")),
            hdb_path=Path(os.environ.get("HDB_PATH", Path.home() / "nano_tick_hdb")),
            schema_q=Path(__file__).resolve().parent / "schema.q",
        )


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
