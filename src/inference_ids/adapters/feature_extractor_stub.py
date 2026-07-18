from __future__ import annotations

import numpy as np

from inference_ids.domain.models import FlowRecord

_PROTO_INDEX = {"tcp": 0, "udp": 1, "icmp": 2}


class StubFeatureExtractor:
    """
    PLACEHOLDER feature extractor.

    The real feature contract (exact feature list, order, units) for the production
    model is owned by a separate model-training pipeline and does not live in this
    repo. This stub derives a small feature vector directly from Zeek conn.log fields
    so Kafka -> parse -> batch -> inference -> sink is runnable and testable end to end.

    Do not treat this as a production feature set. Replace with an adapter matching
    the trained model's real input contract before scoring real traffic.
    """

    feature_count = 11

    def extract(self, records: list[FlowRecord]) -> np.ndarray:
        if not records:
            return np.empty((0, self.feature_count), dtype=np.float32)
        return np.array([self._extract_one(record) for record in records], dtype=np.float32)

    @staticmethod
    def _extract_one(record: FlowRecord) -> list[float]:
        total_bytes = record.orig_bytes + record.resp_bytes
        total_pkts = record.orig_pkts + record.resp_pkts
        byte_ratio = record.orig_bytes / record.resp_bytes if record.resp_bytes else float(record.orig_bytes)
        return [
            record.duration,
            float(_PROTO_INDEX.get(record.proto, -1)),
            float(record.orig_bytes),
            float(record.resp_bytes),
            float(record.orig_pkts),
            float(record.resp_pkts),
            float(total_bytes),
            float(total_pkts),
            byte_ratio,
            float(len(record.history)),
            float(record.missed_bytes),
        ]
