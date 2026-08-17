import csv
import hashlib
import io
import zipfile
from typing import Iterator

# schema.q's trade columns. time/eventTime are epoch-millis longs here;
# insertRaw converts them to timestamps.
COLUMNS = ("time", "sym", "venue", "tradeID", "price", "qty", "eventTime", "buyerMaker")

# Binance spot daily `trades` CSV (newer files carry a header row):
#   id, price, qty, quoteQty, time, isBuyerMaker, isBestMatch
_ID, _PRICE, _QTY, _TIME, _MAKER = 0, 1, 2, 4, 5


def _empty_cols() -> dict:
    return {c: [] for c in COLUMNS}


def _to_millis(ts: int) -> int:
    """Normalise a Binance timestamp to milliseconds. Daily archives use
    MICROSECONDS (16 digits); REST/websocket use milliseconds (13). schema.q's
    ms2ts expects ms, so anything above the ms range is scaled down."""
    return ts // 1000 if ts > 10 ** 14 else ts


def verify_checksum(zip_bytes: bytes, checksum_text: str) -> bool:
    """Binance .CHECKSUM files are '<sha256>  <filename>'."""
    expected = checksum_text.split()[0].strip().lower()
    return hashlib.sha256(zip_bytes).hexdigest() == expected


def iter_trade_chunks(
    zip_bytes: bytes, symbol: str, venue: str, chunk_size: int = 500_000
) -> Iterator[dict]:
    """Stream the archive day's CSV from the zip, yielding column-oriented
    chunks. Peak memory is ~one chunk, not a whole day."""
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
                ts = _to_millis(int(row[_TIME]))  # archive is microseconds
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


def parse_rest_trades(trades: list, symbol: str, venue: str) -> dict:
    """REST /api/v3/historicalTrades JSON -> one column chunk in schema layout."""
    cols = _empty_cols()
    for t in trades:
        ts = _to_millis(int(t["time"]))
        cols["time"].append(ts)
        cols["eventTime"].append(ts)  # REST trades carry only trade time
        cols["sym"].append(symbol)
        cols["venue"].append(venue)
        cols["tradeID"].append(int(t["id"]))
        cols["price"].append(float(t["price"]))
        cols["qty"].append(float(t["qty"]))
        cols["buyerMaker"].append(bool(t["isBuyerMaker"]))
    return cols


def parse_live_ticks(ticks: list) -> dict:
    """Normalised MQTT ticks (from the 3A+ normaliser) -> one column chunk.
    Live ticks carry a real emit time, unlike archive/REST."""
    cols = _empty_cols()
    for t in ticks:
        cols["time"].append(_to_millis(int(t["trade_ts"])))
        cols["eventTime"].append(_to_millis(int(t["event_ts"])))
        cols["sym"].append(t["symbol"])
        cols["venue"].append(t["venue"])
        cols["tradeID"].append(int(t["trade_id"]))
        cols["price"].append(float(t["price"]))
        cols["qty"].append(float(t["qty"]))
        cols["buyerMaker"].append(bool(t["is_buyer_maker"]))
    return cols
