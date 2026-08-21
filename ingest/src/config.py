import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    log_level: str
    symbols: tuple  # instruments to stream, e.g. ("btcusdt", "ethusdt", "solusdt")
    stream_quotes: bool  # also stream @bookTicker quotes alongside trades
    sink_type: str  # "mqtt" (default) or "tcp" (legacy fallback)
    mqtt_host: str
    mqtt_port: int
    mqtt_max_queued: int
    sink_host: str
    sink_port: int
    sink_buffer_mb: int

    @classmethod
    def from_env(cls) -> "Config":
        """Build config from environment (populated by systemd EnvironmentFile=.env)."""
        # SYMBOLS (comma-separated) for multi-instrument; SYMBOL kept for one.
        raw = os.environ.get("SYMBOLS") or os.environ.get("SYMBOL", "btcusdt")
        symbols = tuple(s.strip().lower() for s in raw.split(",") if s.strip())
        return cls(
            log_level=os.environ.get("LOG_LEVEL", "INFO"),
            symbols=symbols,
            stream_quotes=os.environ.get("STREAM_QUOTES", "true").lower() != "false",
            sink_type=os.environ.get("SINK_TYPE", "mqtt"),
            mqtt_host=os.environ.get("MQTT_HOST", "192.168.100.2"),
            mqtt_port=int(os.environ.get("MQTT_PORT", "1883")),
            mqtt_max_queued=int(os.environ.get("MQTT_MAX_QUEUED", "50000")),
            sink_host=os.environ.get("SINK_HOST", "192.168.100.2"),
            sink_port=int(os.environ.get("SINK_PORT", "9000")),
            sink_buffer_mb=int(os.environ.get("SINK_BUFFER_MB", "64")),
        )
