from __future__ import annotations

import time
from typing import Callable

from inference_ids.domain.models import FlowRecord
from inference_ids.domain.ports import FeatureExtractor, FlowParser, FlowSource, InferenceEngine, ResultSink


class InferencePipeline:
    def __init__(
        self,
        source: FlowSource,
        parser: FlowParser,
        feature_extractor: FeatureExtractor,
        inference_engine: InferenceEngine,
        sink: ResultSink,
        batch_window_ms: int = 100,
        max_batch_size: int = 256,
        poll_timeout_seconds: float = 0.05,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._source = source
        self._parser = parser
        self._feature_extractor = feature_extractor
        self._inference_engine = inference_engine
        self._sink = sink
        self._batch_window_seconds = batch_window_ms / 1000
        self._max_batch_size = max_batch_size
        self._poll_timeout_seconds = poll_timeout_seconds
        self._clock = clock

    def run_forever(self) -> None:
        try:
            while True:
                self.run_one_window()
        finally:
            self._source.close()

    def run_one_window(self) -> int:
        records: list[FlowRecord] = []
        window_deadline = self._clock() + self._batch_window_seconds

        while self._clock() < window_deadline and len(records) < self._max_batch_size:
            raw = self._source.poll(self._poll_timeout_seconds)
            if raw is not None:
                records.append(self._parser.parse(raw))

        if records:
            self._score_and_emit(records)
        return len(records)

    def _score_and_emit(self, records: list[FlowRecord]) -> None:
        features = self._feature_extractor.extract(records)
        predictions = self._inference_engine.predict(features)
        for record, prediction in zip(records, predictions):
            self._sink.emit(record, prediction)
