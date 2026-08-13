import hashlib
import io
import zipfile

import pytest

from resources import binance

HEADER = "id,price,qty,quoteQty,time,isBuyerMaker,isBestMatch"


def csv_row(tid, price, qty, t, maker):
    return f"{tid},{price},{qty},{price * qty},{t},{str(maker).lower()},true"


def make_zip(csv_text, name="BTCUSDT-trades-2026-07-15.csv", extra=None):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(name, csv_text)
        if extra:
            zf.writestr(extra, csv_text)
    return buf.getvalue()


# --- checksum --------------------------------------------------------------

def test_verify_checksum_match_and_mismatch():
    blob = b"some-zip-bytes"
    digest = hashlib.sha256(blob).hexdigest()
    assert binance.verify_checksum(blob, f"{digest}  BTCUSDT-trades-2026-07-15.zip")
    assert not binance.verify_checksum(blob, "deadbeef  BTCUSDT-trades-2026-07-15.zip")


# --- archive CSV parsing ---------------------------------------------------

def test_iter_trade_chunks_maps_columns_with_header():
    csv_text = "\n".join([
        HEADER,
        csv_row(100, 42000.5, 0.01, 1700000000000, True),
        csv_row(101, 42001.0, 0.02, 1700000000100, False),
    ])
    chunks = list(binance.iter_trade_chunks(make_zip(csv_text), "BTCUSDT", "binance"))
    assert len(chunks) == 1
    cols = chunks[0]
    assert cols["tradeID"] == [100, 101]
    assert cols["price"] == [42000.5, 42001.0]
    assert cols["qty"] == [0.01, 0.02]
    assert cols["time"] == [1700000000000, 1700000000100]
    assert cols["eventTime"] == cols["time"]  # archive has no emit time
    assert cols["sym"] == ["BTCUSDT", "BTCUSDT"]
    assert cols["venue"] == ["binance", "binance"]
    assert cols["buyerMaker"] == [True, False]


def test_iter_trade_chunks_without_header():
    csv_text = csv_row(100, 1.0, 2.0, 1700000000000, True)
    chunks = list(binance.iter_trade_chunks(make_zip(csv_text), "BTCUSDT", "binance"))
    assert chunks[0]["tradeID"] == [100]


def test_iter_trade_chunks_respects_chunk_size():
    rows = [csv_row(i, 1.0, 1.0, 1700000000000 + i, True) for i in range(5)]
    chunks = list(binance.iter_trade_chunks(
        make_zip("\n".join(rows)), "BTCUSDT", "binance", chunk_size=2))
    assert [len(c["tradeID"]) for c in chunks] == [2, 2, 1]
    assert [tid for c in chunks for tid in c["tradeID"]] == [0, 1, 2, 3, 4]


def test_iter_trade_chunks_skips_blank_lines():
    csv_text = csv_row(1, 1.0, 1.0, 1700000000000, True) + "\n\n"
    chunks = list(binance.iter_trade_chunks(make_zip(csv_text), "BTCUSDT", "binance"))
    assert chunks[0]["tradeID"] == [1]


def test_iter_trade_chunks_rejects_ambiguous_archive():
    zip_bytes = make_zip("x", extra="BTCUSDT-trades-2026-07-15-extra.csv")
    with pytest.raises(ValueError):
        list(binance.iter_trade_chunks(zip_bytes, "BTCUSDT", "binance"))


# --- REST JSON parsing -----------------------------------------------------

def test_parse_rest_trades():
    trades = [
        {"id": 100, "price": "42000.5", "qty": "0.01",
         "time": 1700000000000, "isBuyerMaker": True},
        {"id": 101, "price": "42001.0", "qty": "0.02",
         "time": 1700000000100, "isBuyerMaker": False},
    ]
    cols = binance.parse_rest_trades(trades, "BTCUSDT", "binance")
    assert cols["tradeID"] == [100, 101]
    assert cols["price"] == [42000.5, 42001.0]
    assert cols["qty"] == [0.01, 0.02]
    assert cols["time"] == [1700000000000, 1700000000100]
    assert cols["eventTime"] == cols["time"]
    assert cols["sym"] == ["BTCUSDT", "BTCUSDT"]
    assert cols["venue"] == ["binance", "binance"]
    assert cols["buyerMaker"] == [True, False]


def test_parse_live_ticks():
    ticks = [{"venue": "binance", "symbol": "BTCUSDT", "trade_id": 5, "price": "1.5",
              "qty": "2.0", "event_ts": 1700000000001, "trade_ts": 1700000000000,
              "is_buyer_maker": True}]
    cols = binance.parse_live_ticks(ticks)
    assert cols["tradeID"] == [5]
    assert cols["time"] == [1700000000000]
    assert cols["eventTime"] == [1700000000001]   # live carries a real emit time
    assert cols["price"] == [1.5]
    assert cols["qty"] == [2.0]
    assert cols["sym"] == ["BTCUSDT"]
    assert cols["venue"] == ["binance"]
    assert cols["buyerMaker"] == [True]
