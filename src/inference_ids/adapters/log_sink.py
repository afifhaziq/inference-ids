from __future__ import annotations

import logging

from inference_ids.domain.models import FlowRecord, Prediction

logger = logging.getLogger("inference_ids.results")


class LoggingResultSink:
    def emit(self, record: FlowRecord, prediction: Prediction) -> None:
        logger.info(
            "flow=%s %s:%s -> %s:%s proto=%s label=%s confidence=%.3f",
            record.uid,
            record.orig_h,
            record.orig_p,
            record.resp_h,
            record.resp_p,
            record.proto,
            prediction.label,
            prediction.confidence,
        )
