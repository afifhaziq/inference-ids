from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass
class KafkaSourceConfig:
    bootstrap_servers: str
    topic: str
    group_id: str
    auto_offset_reset: str = "latest"


@dataclass
class SourceConfig:
    type: str
    kafka: KafkaSourceConfig | None = None


@dataclass
class ParserConfig:
    type: str


@dataclass
class FeatureExtractorConfig:
    type: str


@dataclass
class PyTorchEngineConfig:
    module: str
    class_name: str
    state_dict_path: str
    init_kwargs: dict
    class_names: list[str]
    device: str = "cpu"
    precision: str = "fp32"


@dataclass
class InferenceEngineConfig:
    type: str
    pytorch: PyTorchEngineConfig | None = None


@dataclass
class SinkConfig:
    type: str


@dataclass
class PipelineConfig:
    batch_window_ms: int = 100
    max_batch_size: int = 256
    poll_timeout_seconds: float = 0.05


@dataclass
class AppConfig:
    source: SourceConfig
    parser: ParserConfig
    feature_extractor: FeatureExtractorConfig
    inference_engine: InferenceEngineConfig
    sink: SinkConfig
    pipeline: PipelineConfig = field(default_factory=PipelineConfig)


def load_config(path: str | Path) -> AppConfig:
    with Path(path).open() as handle:
        raw = yaml.safe_load(handle)

    source_raw = raw["source"]
    inference_raw = raw["inference_engine"]

    return AppConfig(
        source=SourceConfig(
            type=source_raw["type"],
            kafka=KafkaSourceConfig(**source_raw["kafka"]) if "kafka" in source_raw else None,
        ),
        parser=ParserConfig(**raw["parser"]),
        feature_extractor=FeatureExtractorConfig(**raw["feature_extractor"]),
        inference_engine=InferenceEngineConfig(
            type=inference_raw["type"],
            pytorch=PyTorchEngineConfig(**inference_raw["pytorch"]) if "pytorch" in inference_raw else None,
        ),
        sink=SinkConfig(**raw["sink"]),
        pipeline=PipelineConfig(**raw.get("pipeline", {})),
    )
