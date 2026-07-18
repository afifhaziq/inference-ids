from __future__ import annotations

import json

from confluent_kafka import Consumer


class KafkaFlowSource:
    """Transport-only adapter: returns the JSON-decoded message value unmodified.
    Unwrapping zeek-kafka's {"conn": {...}} shape is JSONFlowParser's responsibility.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        auto_offset_reset: str = "latest",
    ) -> None:
        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": auto_offset_reset,
            }
        )
        self._consumer.subscribe([topic])

    def poll(self, timeout_seconds: float) -> dict | None:
        message = self._consumer.poll(timeout_seconds)
        if message is None:
            return None
        if message.error():
            raise RuntimeError(f"Kafka error: {message.error()}")
        return json.loads(message.value())

    def close(self) -> None:
        self._consumer.close()
