from src.streams.base import WebsocketStream

WS_BASE = "wss://stream.binance.com:9443"


class BinanceTradeStream(WebsocketStream):
    def __init__(self, symbol: str):
        self.symbol = symbol

    def url(self) -> str:
        return f"{WS_BASE}/ws/{self.symbol.lower()}@trade"


class BinanceCombinedStream(WebsocketStream):
    """Trades (and optionally @bookTicker quotes) for one or more symbols over
    ONE combined-stream websocket — cheaper than a connection per stream on the
    512MB 3A+, and Binance multiplexes many symbols on a single connection.
    Combined-stream messages are wrapped: {"stream": "...", "data": {...}}."""

    def __init__(self, symbols, quotes: bool = True):
        if isinstance(symbols, str):
            symbols = [symbols]
        self.symbols = [s.lower() for s in symbols]
        self.quotes = quotes

    def url(self) -> str:
        streams = []
        for s in self.symbols:
            streams.append(f"{s}@trade")
            if self.quotes:
                streams.append(f"{s}@bookTicker")
        return f"{WS_BASE}/stream?streams=" + "/".join(streams)
