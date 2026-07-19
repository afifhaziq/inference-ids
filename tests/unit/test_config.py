from pathlib import Path

import pytest
import yaml

from inference_ids.config import load_config
from inference_ids.factories import (
    create_feature_extractor,
    create_flow_parser,
    create_flow_source,
    create_inference_engine,
    create_result_sink,
)
from inference_ids.adapters.feature_extractor_stub import StubFeatureExtractor
from inference_ids.adapters.json_parser import JSONFlowParser
from inference_ids.adapters.log_sink import LoggingResultSink
from inference_ids.adapters.tsv_parser import TSVFlowParser

RAW_CONFIG = {
    "source": {
        "type": "kafka",
        "kafka": {
            "bootstrap_servers": "kafka:9092",
            "topic": "zeek-flows",
            "group_id": "inference-ids",
            "auto_offset_reset": "latest",
        },
    },
    "parser": {"type": "json"},
    "feature_extractor": {"type": "stub"},
    "inference_engine": {
        "type": "pytorch",
        "pytorch": {
            "module": "inference_ids.reference_model",
            "class_name": "IDSModel",
            "state_dict_path": "/models/reference.pth",
            "init_kwargs": {"input_features": 11, "num_classes": 3},
            "class_names": ["a", "b", "c"],
            "device": "cpu",
            "precision": "fp32",
        },
    },
    "sink": {"type": "log"},
    "pipeline": {"batch_window_ms": 100, "max_batch_size": 256, "poll_timeout_seconds": 0.05},
}


@pytest.fixture
def config_path(tmp_path: Path) -> Path:
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(RAW_CONFIG))
    return path


def test_load_config_parses_all_sections(config_path):
    config = load_config(config_path)

    assert config.source.type == "kafka"
    assert config.source.kafka.bootstrap_servers == "kafka:9092"
    assert config.parser.type == "json"
    assert config.feature_extractor.type == "stub"
    assert config.inference_engine.type == "pytorch"
    assert config.inference_engine.pytorch.class_name == "IDSModel"
    assert config.sink.type == "log"
    assert config.pipeline.batch_window_ms == 100


def test_load_config_applies_pipeline_defaults(tmp_path):
    raw = dict(RAW_CONFIG)
    raw.pop("pipeline")
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(raw))

    config = load_config(path)

    assert config.pipeline.batch_window_ms == 100
    assert config.pipeline.max_batch_size == 256


def test_create_flow_parser_dispatches_on_type(config_path):
    config = load_config(config_path)
    assert isinstance(create_flow_parser(config), JSONFlowParser)

    config.parser.type = "tsv"
    assert isinstance(create_flow_parser(config), TSVFlowParser)


def test_create_feature_extractor_dispatches_on_type(config_path):
    config = load_config(config_path)
    assert isinstance(create_feature_extractor(config), StubFeatureExtractor)


def test_create_result_sink_dispatches_on_type(config_path):
    config = load_config(config_path)
    assert isinstance(create_result_sink(config), LoggingResultSink)


def test_create_flow_source_unknown_type_raises(config_path):
    config = load_config(config_path)
    config.source.type = "not-a-real-adapter"
    with pytest.raises(ValueError, match="not-a-real-adapter"):
        create_flow_source(config)


def test_load_config_parses_multi_jsonl_sink(tmp_path):
    raw = dict(RAW_CONFIG)
    raw["sink"] = {
        "type": "multi",
        "multi": {
            "sinks": [
                {"type": "log"},
                {"type": "jsonl", "jsonl": {"path": "/data/predictions.jsonl"}},
            ]
        },
    }
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(raw))

    config = load_config(path)

    assert config.sink.type == "multi"
    assert len(config.sink.multi.sinks) == 2
    assert config.sink.multi.sinks[0].type == "log"
    assert config.sink.multi.sinks[1].type == "jsonl"
    assert config.sink.multi.sinks[1].jsonl.path == "/data/predictions.jsonl"


def test_load_config_evaluation_label_map_defaults_to_empty(config_path):
    config = load_config(config_path)
    assert config.evaluation.label_map == {}


def test_load_config_parses_evaluation_label_map(tmp_path):
    raw = dict(RAW_CONFIG)
    raw["evaluation"] = {"label_map": {5: 0, 6: 1}}
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(raw))

    config = load_config(path)

    assert config.evaluation.label_map == {5: 0, 6: 1}
