import json

import paho.mqtt.client as mqtt

from src.sinks import mqtt as mqtt_sink_module
from src.sinks.mqtt import MqttSink

TICK = {"venue": "binance", "symbol": "BTCUSDT", "trade_id": 1, "price": 1.0}


class FakeMqttClient:
    """Mocks paho's Client: records configuration and publishes, and reports
    a full queue after `queue_capacity` unsent publishes."""

    def __init__(self, *args, **kwargs):
        self.published = []
        self.queue_capacity = None
        self.reconnect_delays = None
        self.connected_to = None
        self.loop_started = False
        self.on_connect = None
        self.on_disconnect = None

    def max_queued_messages_set(self, n):
        self.queue_capacity = n

    def reconnect_delay_set(self, min_delay, max_delay):
        self.reconnect_delays = (min_delay, max_delay)

    def connect_async(self, host, port):
        self.connected_to = (host, port)

    def loop_start(self):
        self.loop_started = True

    def loop_stop(self):
        self.loop_started = False

    def disconnect(self):
        pass

    def publish(self, topic, payload, qos):
        if self.queue_capacity is not None and len(self.published) >= self.queue_capacity:
            return _info(mqtt.MQTT_ERR_QUEUE_SIZE)
        self.published.append((topic, payload, qos))
        return _info(mqtt.MQTT_ERR_SUCCESS)


def _info(rc):
    info = mqtt.MQTTMessageInfo(0)
    info.rc = rc
    return info


def make_sink(monkeypatch, **kwargs) -> MqttSink:
    monkeypatch.setattr(mqtt_sink_module.mqtt, "Client", FakeMqttClient)
    return MqttSink("broker-host", **kwargs)


def test_send_publishes_to_venue_symbol_topic_at_qos1(monkeypatch):
    sink = make_sink(monkeypatch)
    sink.send(TICK)
    topic, payload, qos = sink._client.published[0]
    assert topic == "ticks/binance/btcusdt"
    assert qos == 1
    assert json.loads(payload) == TICK


def test_constructor_configures_queue_cap_and_reconnect(monkeypatch):
    sink = make_sink(monkeypatch, max_queued_messages=1234)
    assert sink._client.queue_capacity == 1234
    assert sink._client.reconnect_delays == (1, 60)


def test_send_counts_drops_when_queue_full(monkeypatch):
    sink = make_sink(monkeypatch, max_queued_messages=5)
    for i in range(20):
        sink.send({**TICK, "trade_id": i})  # must not raise
    assert len(sink._client.published) == 5
    assert sink._dropped == 15
