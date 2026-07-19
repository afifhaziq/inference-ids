from __future__ import annotations

from inference_ids.domain.models import FlowRecord, Prediction
from inference_ids.domain.ports import ResultSink


class MultiResultSink:
    """Fans out each emit() call to every wrapped sink, in order."""

    def __init__(self, sinks: list[ResultSink]) -> None:
        self._sinks = sinks

    def emit(self, record: FlowRecord, prediction: Prediction) -> None:
        for sink in self._sinks:
            sink.emit(record, prediction)
