from __future__ import annotations

from typing import Protocol, runtime_checkable

import numpy as np

from inference_ids.domain.models import FlowRecord, Prediction


@runtime_checkable
class FlowSource(Protocol):
    def poll(self, timeout_seconds: float) -> dict | None:
        """Return the next raw flow message as a dict, or None if none arrived within timeout_seconds."""
        ...

    def close(self) -> None:
        ...


@runtime_checkable
class FlowParser(Protocol):
    def parse(self, raw: dict) -> FlowRecord:
        ...


@runtime_checkable
class FeatureExtractor(Protocol):
    def extract(self, records: list[FlowRecord]) -> np.ndarray:
        """Return a 2D float32 array of shape (len(records), n_features)."""
        ...


@runtime_checkable
class InferenceEngine(Protocol):
    def predict(self, features: np.ndarray) -> list[Prediction]:
        ...


@runtime_checkable
class ResultSink(Protocol):
    def emit(self, record: FlowRecord, prediction: Prediction) -> None:
        ...
