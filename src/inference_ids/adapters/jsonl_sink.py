from __future__ import annotations

import json

from inference_ids.domain.models import FlowRecord, Prediction


class JSONLResultSink:
    """Persists one JSON line per prediction, keyed by Zeek uid, for offline label validation."""

    def __init__(self, path: str) -> None:
        self._file = open(path, "a", encoding="utf-8")

    def emit(self, record: FlowRecord, prediction: Prediction) -> None:
        row = {
            "uid": record.uid,
            "predicted_index": prediction.class_index,
            "predicted_label": prediction.label,
            "confidence": prediction.confidence,
            "logits": prediction.logits,
        }
        self._file.write(json.dumps(row) + "\n")
        self._file.flush()
