import json
from types import SimpleNamespace

from receiver import Receiver


def mqtt_msg(payload) -> SimpleNamespace:
    if isinstance(payload, dict):
        payload = json.dumps(payload).encode()
    return SimpleNamespace(topic="ticks/binance/btcusdt", payload=payload)


def tick(trade_id: int, symbol: str = "BTCUSDT") -> dict:
    return {"venue": "binance", "symbol": symbol, "trade_id": trade_id, "price": 1.0}


def test_dedupes_replayed_trade_ids():
    receiver = Receiver()
    receiver.on_message(None, None, mqtt_msg(tick(1)))
    receiver.on_message(None, None, mqtt_msg(tick(2)))
    receiver.on_message(None, None, mqtt_msg(tick(2)))
    receiver.on_message(None, None, mqtt_msg(tick(1)))
    assert receiver.received == 2
    assert receiver.duplicates == 2


def test_symbols_dedupe_independently():
    receiver = Receiver()
    receiver.on_message(None, None, mqtt_msg(tick(7, "BTCUSDT")))
    receiver.on_message(None, None, mqtt_msg(tick(7, "ETHUSDT")))
    assert receiver.received == 2
    assert receiver.duplicates == 0


def test_malformed_messages_are_dropped_not_raised():
    receiver = Receiver()
    receiver.on_message(None, None, mqtt_msg(b"{broken"))
    receiver.on_message(None, None, mqtt_msg({"no": "fields"}))
    assert receiver.received == 0
