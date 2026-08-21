from src.config import Config

ALL_VARS = [
    "LOG_LEVEL", "SYMBOL", "SYMBOLS", "STREAM_QUOTES", "SINK_TYPE", "MQTT_HOST", "MQTT_PORT",
    "MQTT_MAX_QUEUED", "SINK_HOST", "SINK_PORT", "SINK_BUFFER_MB",
]


def test_defaults(monkeypatch):
    for var in ALL_VARS:
        monkeypatch.delenv(var, raising=False)
    config = Config.from_env()
    assert config.log_level == "INFO"
    assert config.symbols == ("btcusdt",)
    assert config.stream_quotes is True
    assert config.sink_type == "mqtt"
    assert config.mqtt_host == "192.168.100.2"
    assert config.mqtt_port == 1883
    assert config.mqtt_max_queued == 50000
    assert config.sink_host == "192.168.100.2"
    assert config.sink_port == 9000
    assert config.sink_buffer_mb == 64


def test_env_overrides(monkeypatch):
    monkeypatch.setenv("SYMBOL", "ethusdt")
    monkeypatch.setenv("SINK_TYPE", "tcp")
    monkeypatch.setenv("MQTT_PORT", "2883")
    monkeypatch.setenv("SINK_BUFFER_MB", "32")
    monkeypatch.setenv("STREAM_QUOTES", "false")
    config = Config.from_env()
    assert config.symbols == ("ethusdt",)
    assert config.sink_type == "tcp"
    assert config.mqtt_port == 2883
    assert config.sink_buffer_mb == 32
    assert config.stream_quotes is False


def test_symbols_list_and_precedence(monkeypatch):
    monkeypatch.setenv("SYMBOLS", "BTCUSDT, ethusdt , solusdt")
    monkeypatch.setenv("SYMBOL", "adausdt")           # SYMBOLS wins when both set
    assert Config.from_env().symbols == ("btcusdt", "ethusdt", "solusdt")
    monkeypatch.delenv("SYMBOLS")
    assert Config.from_env().symbols == ("adausdt",)   # falls back to SYMBOL


def test_int_fields_are_ints(monkeypatch):
    for var in ALL_VARS:
        monkeypatch.delenv(var, raising=False)
    config = Config.from_env()
    for field in ("mqtt_port", "mqtt_max_queued", "sink_port", "sink_buffer_mb"):
        assert isinstance(getattr(config, field), int)
