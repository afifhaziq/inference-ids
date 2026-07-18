import json

import pytest

from inference_ids.adapters import kafka_source


class FakeMessage:
    def __init__(self, value: bytes, error=None):
        self._value = value
        self._error = error

    def value(self) -> bytes:
        return self._value

    def error(self):
        return self._error


class FakeConsumer:
    instances = []

    def __init__(self, conf: dict):
        self.conf = conf
        self.subscribed_topics = []
        self.closed = False
        self._messages = []
        FakeConsumer.instances.append(self)

    def subscribe(self, topics):
        self.subscribed_topics = topics

    def poll(self, timeout):
        if self._messages:
            return self._messages.pop(0)
        return None

    def close(self):
        self.closed = True


@pytest.fixture(autouse=True)
def patch_consumer(monkeypatch):
    FakeConsumer.instances.clear()
    monkeypatch.setattr(kafka_source, "Consumer", FakeConsumer)
    yield


def test_source_subscribes_to_configured_topic():
    kafka_source.KafkaFlowSource(
        bootstrap_servers="kafka:9092", topic="zeek-flows", group_id="inference-ids"
    )

    consumer = FakeConsumer.instances[0]
    assert consumer.subscribed_topics == ["zeek-flows"]
    assert consumer.conf["bootstrap.servers"] == "kafka:9092"
    assert consumer.conf["group.id"] == "inference-ids"
    assert consumer.conf["auto.offset.reset"] == "latest"


def test_poll_returns_none_when_no_message():
    source = kafka_source.KafkaFlowSource(
        bootstrap_servers="kafka:9092", topic="zeek-flows", group_id="inference-ids"
    )
    assert source.poll(0.01) is None


def test_poll_decodes_json_message_value():
    source = kafka_source.KafkaFlowSource(
        bootstrap_servers="kafka:9092", topic="zeek-flows", group_id="inference-ids"
    )
    consumer = FakeConsumer.instances[0]
    payload = json.dumps({"conn": {"uid": "C1"}}).encode("utf-8")
    consumer._messages.append(FakeMessage(value=payload))

    result = source.poll(0.01)
    assert result == {"conn": {"uid": "C1"}}


def test_poll_raises_on_kafka_error():
    source = kafka_source.KafkaFlowSource(
        bootstrap_servers="kafka:9092", topic="zeek-flows", group_id="inference-ids"
    )
    consumer = FakeConsumer.instances[0]
    consumer._messages.append(FakeMessage(value=b"", error="broker unavailable"))

    with pytest.raises(RuntimeError, match="broker unavailable"):
        source.poll(0.01)


def test_close_closes_underlying_consumer():
    source = kafka_source.KafkaFlowSource(
        bootstrap_servers="kafka:9092", topic="zeek-flows", group_id="inference-ids"
    )
    source.close()
    assert FakeConsumer.instances[0].closed is True
