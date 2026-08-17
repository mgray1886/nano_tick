import time


def normalize_trade(msg: dict) -> dict:
    return {
        "venue": "binance",
        "symbol": msg["s"],
        "trade_id": msg["t"],
        "price": float(msg["p"]),
        "qty": float(msg["q"]),
        "event_ts": msg["E"],
        "trade_ts": msg["T"],
        "is_buyer_maker": msg["m"],
    }


def normalize_quote(msg: dict, recv_ts: int = None) -> dict:
    """Binance spot @bookTicker -> normalised best bid/ask. bookTicker carries
    no exchange timestamp, so the quote is stamped with receive time (ms)."""
    return {
        "venue": "binance",
        "symbol": msg["s"],
        "update_id": msg["u"],
        "bid": float(msg["b"]),
        "bid_qty": float(msg["B"]),
        "ask": float(msg["a"]),
        "ask_qty": float(msg["A"]),
        "recv_ts": recv_ts if recv_ts is not None else int(time.time() * 1000),
    }
