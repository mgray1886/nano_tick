import pytest

from src.normalisers.binance import normalize_trade

RAW = {
    "e": "trade",
    "E": 1783763087212,
    "s": "BTCUSDT",
    "t": 6497302296,
    "p": "64208.32000000",
    "q": "0.00014000",
    "T": 1783763087210,
    "m": True,
    "M": True,
}


def test_normalize_trade_maps_all_fields():
    tick = normalize_trade(RAW)
    assert tick == {
        "venue": "binance",
        "symbol": "BTCUSDT",
        "trade_id": 6497302296,
        "price": 64208.32,
        "qty": 0.00014,
        "event_ts": 1783763087212,
        "trade_ts": 1783763087210,
        "is_buyer_maker": True,
    }


def test_normalize_trade_price_and_qty_are_floats():
    tick = normalize_trade(RAW)
    assert isinstance(tick["price"], float)
    assert isinstance(tick["qty"], float)


def test_normalize_trade_missing_field_raises():
    # main.run() relies on this raising (it catches, logs, and drops).
    incomplete = {k: v for k, v in RAW.items() if k != "p"}
    with pytest.raises(KeyError):
        normalize_trade(incomplete)
