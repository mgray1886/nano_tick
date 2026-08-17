from src.streams.base import WebsocketStream

WS_BASE = "wss://stream.binance.com:9443"


class BinanceTradeStream(WebsocketStream):
    def __init__(self, symbol: str):
        self.symbol = symbol

    def url(self) -> str:
        return f"{WS_BASE}/ws/{self.symbol.lower()}@trade"


class BinanceCombinedStream(WebsocketStream):
    """Trades (and optionally @bookTicker quotes) over ONE combined-stream
    websocket — cheaper than a connection per stream on the 512MB 3A+.
    Combined-stream messages are wrapped: {"stream": "...", "data": {...}}."""

    def __init__(self, symbol: str, quotes: bool = True):
        self.symbol = symbol
        self.quotes = quotes

    def url(self) -> str:
        s = self.symbol.lower()
        streams = [f"{s}@trade"]
        if self.quotes:
            streams.append(f"{s}@bookTicker")
        return f"{WS_BASE}/stream?streams=" + "/".join(streams)
