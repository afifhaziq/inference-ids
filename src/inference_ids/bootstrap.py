from __future__ import annotations

from inference_ids.application.pipeline import InferencePipeline
from inference_ids.config import AppConfig
from inference_ids.factories import (
    create_feature_extractor,
    create_flow_parser,
    create_flow_source,
    create_inference_engine,
    create_result_sink,
)


def build_pipeline(config: AppConfig) -> InferencePipeline:
    return InferencePipeline(
        source=create_flow_source(config),
        parser=create_flow_parser(config),
        feature_extractor=create_feature_extractor(config),
        inference_engine=create_inference_engine(config),
        sink=create_result_sink(config),
        batch_window_ms=config.pipeline.batch_window_ms,
        max_batch_size=config.pipeline.max_batch_size,
        poll_timeout_seconds=config.pipeline.poll_timeout_seconds,
    )
