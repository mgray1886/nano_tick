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
