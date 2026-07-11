from src.streams.base import WebsocketStream


class BinanceTradeStream(WebsocketStream):
    def __init__(self, symbol: str):
        self.symbol = symbol

    def url(self) -> str:
        return f"wss://stream.binance.com:9443/ws/{self.symbol.lower()}@trade"
