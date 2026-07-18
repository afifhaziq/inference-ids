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
        error = message.error()
        if error is not None:
            if error.fatal():
                raise RuntimeError(f"Kafka error: {error}")
            # Non-fatal (e.g. UNKNOWN_TOPIC_OR_PART while the topic hasn't been
            # created yet, the normal state on a fresh boot before the sensor
            # has produced anything) -- retry on the next poll instead of
            # crashing the consumer.
            return None
        return json.loads(message.value())

    def close(self) -> None:
        self._consumer.close()
