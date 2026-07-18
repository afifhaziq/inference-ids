from __future__ import annotations

from inference_ids.adapters.feature_extractor_stub import StubFeatureExtractor
from inference_ids.adapters.json_parser import JSONFlowParser
from inference_ids.adapters.kafka_source import KafkaFlowSource
from inference_ids.adapters.log_sink import LoggingResultSink
from inference_ids.adapters.pytorch_inference import PyTorchInferenceEngine
from inference_ids.adapters.tsv_parser import TSVFlowParser
from inference_ids.config import AppConfig
from inference_ids.domain.ports import FeatureExtractor, FlowParser, FlowSource, InferenceEngine, ResultSink


def create_flow_source(config: AppConfig) -> FlowSource:
    if config.source.type == "kafka":
        kafka = config.source.kafka
        return KafkaFlowSource(
            bootstrap_servers=kafka.bootstrap_servers,
            topic=kafka.topic,
            group_id=kafka.group_id,
            auto_offset_reset=kafka.auto_offset_reset,
        )
    raise ValueError(f"Unknown source type: {config.source.type!r}")


def create_flow_parser(config: AppConfig) -> FlowParser:
    if config.parser.type == "json":
        return JSONFlowParser()
    if config.parser.type == "tsv":
        return TSVFlowParser()
    raise ValueError(f"Unknown parser type: {config.parser.type!r}")


def create_feature_extractor(config: AppConfig) -> FeatureExtractor:
    if config.feature_extractor.type == "stub":
        return StubFeatureExtractor()
    raise ValueError(f"Unknown feature_extractor type: {config.feature_extractor.type!r}")


def create_inference_engine(config: AppConfig) -> InferenceEngine:
    if config.inference_engine.type == "pytorch":
        pytorch = config.inference_engine.pytorch
        return PyTorchInferenceEngine(
            model_module=pytorch.module,
            model_class=pytorch.class_name,
            state_dict_path=pytorch.state_dict_path,
            init_kwargs=pytorch.init_kwargs,
            class_names=pytorch.class_names,
            device=pytorch.device,
            precision=pytorch.precision,
        )
    raise ValueError(f"Unknown inference_engine type: {config.inference_engine.type!r}")


def create_result_sink(config: AppConfig) -> ResultSink:
    if config.sink.type == "log":
        return LoggingResultSink()
    raise ValueError(f"Unknown sink type: {config.sink.type!r}")
