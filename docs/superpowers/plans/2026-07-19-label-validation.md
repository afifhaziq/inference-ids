# Offline Label Validation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let predictions be checked offline against a labeled dataset (Zeek `uid` + integer
`label`, JSONL), after a run finishes — with zero changes to the live pipeline's ports or
behavior when the feature isn't configured.

**Architecture:** Two new `ResultSink` adapters (`JSONLResultSink` persists predictions keyed by
`uid`; `MultiResultSink` fans out to several sinks, e.g. `log` + `jsonl` together), wired through
the existing config-driven factory dispatch. A separate, fully decoupled offline CLI
(`inference_ids.evaluate`) joins the persisted predictions against a labels file by `uid` and
prints an `sklearn` classification report + confusion matrix + unmatched-record counts.

**Tech Stack:** Python 3.12, dataclasses, PyYAML, scikit-learn (new dev-group dependency), pytest.

## Global Constraints

- No changes to `application/pipeline.py`, `bootstrap.py`, or the five domain port `Protocol`s in
  `domain/ports.py`.
- Labels file format: JSONL, one `{"uid": <str>, "label": <int>}` object per line.
- Predictions persisted as JSONL: one `{"uid", "predicted_index", "predicted_label", "confidence",
  "logits"}` object per line, written by the new `JSONLResultSink`.
- Label-to-class-index mapping is config-driven (`evaluation.label_map` in YAML), defaulting to
  identity (`{i: i for i in range(len(class_names))}`) when omitted.
- Metrics via `sklearn.metrics.classification_report` + `confusion_matrix`; console output only,
  no file report in this iteration.
- `scikit-learn` must NOT end up in the `backend` Docker image — the Dockerfile installs via
  `uv sync --frozen --no-dev --no-install-project`, so anything in the existing `dev`
  dependency-group is automatically excluded from that image already. (Plan places scikit-learn in
  the existing `dev` group rather than a new group, so `uv run pytest` and `uv run python -m
  inference_ids.evaluate` both work without extra flags — this is a refinement over the spec's
  literal "new eval group" wording, made because the spec's actual constraint — keep it out of the
  `backend` image — is already satisfied by `dev`, and a second group would otherwise break the
  documented plain `uv run pytest` command for the new tests.)
- Follow existing test conventions exactly: hand-rolled fakes (no mocks), each test file defines
  its own local fixtures/helpers (no shared `conftest.py` exists in this repo — don't add one).
- `pyproject.toml` sets `pythonpath = ["src"]`; tests import `inference_ids` directly.

---

### Task 1: `Prediction.class_index` field

**Files:**
- Modify: `src/inference_ids/domain/models.py` (the `Prediction` dataclass)
- Modify: `src/inference_ids/adapters/pytorch_inference.py:44-51` (the `predict()` loop)
- Test: `tests/unit/test_pytorch_inference.py`

**Interfaces:**
- Consumes: nothing new.
- Produces: `Prediction.class_index: int` — the argmax class index, `-1` if unset by an
  `InferenceEngine` that doesn't populate it. Task 2 (`JSONLResultSink`) reads this field.

- [ ] **Step 1: Write the failing test**

Add to `tests/unit/test_pytorch_inference.py` (after the existing
`test_predict_returns_one_prediction_per_row`):

```python
def test_predict_class_index_matches_label():
    model = IDSModel(input_features=4, num_classes=3)
    model.eval()
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as tmp:
        state_dict_path = Path(tmp) / "model.pth"
        torch.save(model.state_dict(), state_dict_path)

        engine = PyTorchInferenceEngine(
            model_module="inference_ids.reference_model",
            model_class="IDSModel",
            state_dict_path=str(state_dict_path),
            init_kwargs={"input_features": 4, "num_classes": 3},
            class_names=["benign", "scan", "dos"],
            device="cpu",
            precision="fp32",
        )

        features = np.random.rand(5, 4).astype(np.float32)
        predictions = engine.predict(features)

    class_names = ["benign", "scan", "dos"]
    for prediction in predictions:
        assert prediction.class_index == class_names.index(prediction.label)
        assert prediction.logits[prediction.class_index] == max(prediction.logits)
```

(Uses `tempfile` directly instead of the `tmp_path` fixture since it's added as a second test
function reusing the pattern of the first — either works; this keeps the diff self-contained.)

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_pytorch_inference.py::test_predict_class_index_matches_label -v`
Expected: FAIL with `AttributeError: 'Prediction' object has no attribute 'class_index'`

- [ ] **Step 3: Add the field to `Prediction`**

In `src/inference_ids/domain/models.py`, change:

```python
@dataclass(frozen=True)
class Prediction:
    label: str
    confidence: float
    logits: list[float] = field(default_factory=list)
```

to:

```python
@dataclass(frozen=True)
class Prediction:
    label: str
    confidence: float
    logits: list[float] = field(default_factory=list)
    class_index: int = -1
```

- [ ] **Step 4: Populate it in `PyTorchInferenceEngine.predict()`**

In `src/inference_ids/adapters/pytorch_inference.py`, change the loop body (currently lines 44-51):

```python
        predictions = []
        for index, confidence, logit_row in zip(indices.tolist(), confidences.tolist(), logits.tolist()):
            predictions.append(
                Prediction(
                    label=self._class_names[index],
                    confidence=confidence,
                    logits=logit_row,
                )
            )
        return predictions
```

to:

```python
        predictions = []
        for index, confidence, logit_row in zip(indices.tolist(), confidences.tolist(), logits.tolist()):
            predictions.append(
                Prediction(
                    label=self._class_names[index],
                    confidence=confidence,
                    logits=logit_row,
                    class_index=index,
                )
            )
        return predictions
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_pytorch_inference.py -v`
Expected: PASS (all tests in the file, including the two pre-existing ones)

- [ ] **Step 6: Commit**

```bash
git add src/inference_ids/domain/models.py src/inference_ids/adapters/pytorch_inference.py tests/unit/test_pytorch_inference.py
git commit -m "feat: add Prediction.class_index for label validation"
```

---

### Task 2: `JSONLResultSink` adapter

**Files:**
- Create: `src/inference_ids/adapters/jsonl_sink.py`
- Test: `tests/unit/test_jsonl_sink.py`

**Interfaces:**
- Consumes: `Prediction.class_index` (Task 1), `FlowRecord.uid`.
- Produces: `JSONLResultSink(path: str)` implementing `ResultSink.emit(record, prediction) -> None`.
  Task 5 (factories) constructs this; Task 7 (evaluate) reads the file it writes.

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_jsonl_sink.py`:

```python
import json

from inference_ids.adapters.jsonl_sink import JSONLResultSink
from inference_ids.domain.models import FlowRecord, Prediction


def _record(uid: str) -> FlowRecord:
    return FlowRecord(
        uid=uid, ts=0.0, duration=1.5, orig_h="10.0.0.1", orig_p=1111,
        resp_h="10.0.0.2", resp_p=80, proto="tcp", service="http",
        conn_state="SF", history="ShADadfF", missed_bytes=0,
        orig_pkts=6, orig_ip_bytes=740, resp_pkts=8, resp_ip_bytes=1620,
        orig_bytes=350, resp_bytes=1200, local_orig=True, local_resp=False,
    )


def test_emit_appends_one_json_line_per_call(tmp_path):
    path = tmp_path / "predictions.jsonl"
    sink = JSONLResultSink(str(path))

    sink.emit(_record("C1"), Prediction(label="benign", confidence=0.9, logits=[1.0, -1.0, 0.0], class_index=0))
    sink.emit(_record("C2"), Prediction(label="scan", confidence=0.8, logits=[-1.0, 1.0, 0.0], class_index=1))

    lines = path.read_text().strip().splitlines()
    assert len(lines) == 2

    row1 = json.loads(lines[0])
    assert row1 == {
        "uid": "C1",
        "predicted_index": 0,
        "predicted_label": "benign",
        "confidence": 0.9,
        "logits": [1.0, -1.0, 0.0],
    }
    row2 = json.loads(lines[1])
    assert row2["uid"] == "C2"
    assert row2["predicted_index"] == 1


def test_emit_flushes_so_file_is_readable_before_close(tmp_path):
    path = tmp_path / "predictions.jsonl"
    sink = JSONLResultSink(str(path))

    sink.emit(_record("C1"), Prediction(label="benign", confidence=0.5, logits=[0.0], class_index=0))

    # No sink.close() call -- file must already be readable via a second, independent open.
    assert path.read_text().strip() != ""
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_jsonl_sink.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'inference_ids.adapters.jsonl_sink'`

- [ ] **Step 3: Write the implementation**

Create `src/inference_ids/adapters/jsonl_sink.py`:

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_jsonl_sink.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/inference_ids/adapters/jsonl_sink.py tests/unit/test_jsonl_sink.py
git commit -m "feat: add JSONLResultSink for persisting predictions by uid"
```

---

### Task 3: `MultiResultSink` adapter

**Files:**
- Create: `src/inference_ids/adapters/multi_sink.py`
- Test: `tests/unit/test_multi_sink.py`

**Interfaces:**
- Consumes: any object implementing `ResultSink` (structural — no import of concrete adapters
  needed here).
- Produces: `MultiResultSink(sinks: list[ResultSink])` implementing `ResultSink.emit(record,
  prediction) -> None`, calling `emit` on every wrapped sink in order. Task 5 (factories)
  constructs this from config.

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_multi_sink.py`:

```python
from inference_ids.adapters.multi_sink import MultiResultSink
from inference_ids.domain.models import FlowRecord, Prediction


def _record(uid: str) -> FlowRecord:
    return FlowRecord(
        uid=uid, ts=0.0, duration=1.5, orig_h="10.0.0.1", orig_p=1111,
        resp_h="10.0.0.2", resp_p=80, proto="tcp", service="http",
        conn_state="SF", history="ShADadfF", missed_bytes=0,
        orig_pkts=6, orig_ip_bytes=740, resp_pkts=8, resp_ip_bytes=1620,
        orig_bytes=350, resp_bytes=1200, local_orig=True, local_resp=False,
    )


class FakeSink:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def emit(self, record: FlowRecord, prediction: Prediction) -> None:
        self.calls.append((record.uid, prediction.label))


def test_emit_calls_every_wrapped_sink_in_order():
    sink_a = FakeSink()
    sink_b = FakeSink()
    multi = MultiResultSink(sinks=[sink_a, sink_b])
    prediction = Prediction(label="benign", confidence=0.9, logits=[1.0], class_index=0)

    multi.emit(_record("C1"), prediction)

    assert sink_a.calls == [("C1", "benign")]
    assert sink_b.calls == [("C1", "benign")]


def test_emit_with_no_sinks_does_nothing():
    multi = MultiResultSink(sinks=[])
    prediction = Prediction(label="benign", confidence=0.9, logits=[1.0], class_index=0)

    multi.emit(_record("C1"), prediction)  # must not raise
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_multi_sink.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'inference_ids.adapters.multi_sink'`

- [ ] **Step 3: Write the implementation**

Create `src/inference_ids/adapters/multi_sink.py`:

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_multi_sink.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/inference_ids/adapters/multi_sink.py tests/unit/test_multi_sink.py
git commit -m "feat: add MultiResultSink to fan out predictions to several sinks"
```

---

### Task 4: `config.py` — nested sink config + `EvaluationConfig`

**Files:**
- Modify: `src/inference_ids/config.py` (all of it — shown in full below)
- Test: `tests/unit/test_config.py`

**Interfaces:**
- Consumes: nothing new.
- Produces:
  - `JSONLSinkConfig(path: str)`
  - `MultiSinkConfig(sinks: list[SinkConfig])`
  - `SinkConfig(type: str, jsonl: JSONLSinkConfig | None = None, multi: MultiSinkConfig | None = None)`
    (extended — `type` and the rest are unchanged for existing callers)
  - `EvaluationConfig(label_map: dict[int, int] = {})`
  - `AppConfig.evaluation: EvaluationConfig` (new field, defaults via `field(default_factory=EvaluationConfig)`)
  - `load_config(path)` now parses nested sink config recursively and the optional `evaluation:` section.
  - Task 5 (factories) consumes `SinkConfig`/`JSONLSinkConfig`/`MultiSinkConfig`. Task 7
    (evaluate.py) consumes `AppConfig.evaluation.label_map` and
    `AppConfig.inference_engine.pytorch.class_names`.

- [ ] **Step 1: Write the failing tests**

Add to `tests/unit/test_config.py` (after the existing tests, before the final
`test_create_flow_source_unknown_type_raises` or after it — order doesn't matter):

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_config.py -v`
Expected: FAIL — `test_load_config_parses_multi_jsonl_sink` with `TypeError:
SinkConfig.__init__() got an unexpected keyword argument 'multi'`; the two `evaluation` tests with
`AttributeError: 'AppConfig' object has no attribute 'evaluation'`

- [ ] **Step 3: Rewrite `config.py`**

Replace the full contents of `src/inference_ids/config.py` with:

```python
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
class JSONLSinkConfig:
    path: str


@dataclass
class SinkConfig:
    type: str
    jsonl: JSONLSinkConfig | None = None
    multi: MultiSinkConfig | None = None


@dataclass
class MultiSinkConfig:
    sinks: list[SinkConfig]


@dataclass
class PipelineConfig:
    batch_window_ms: int = 100
    max_batch_size: int = 256
    poll_timeout_seconds: float = 0.05


@dataclass
class EvaluationConfig:
    label_map: dict[int, int] = field(default_factory=dict)


@dataclass
class AppConfig:
    source: SourceConfig
    parser: ParserConfig
    feature_extractor: FeatureExtractorConfig
    inference_engine: InferenceEngineConfig
    sink: SinkConfig
    pipeline: PipelineConfig = field(default_factory=PipelineConfig)
    evaluation: EvaluationConfig = field(default_factory=EvaluationConfig)


def _parse_sink_config(raw: dict) -> SinkConfig:
    jsonl = JSONLSinkConfig(**raw["jsonl"]) if "jsonl" in raw else None
    multi = None
    if "multi" in raw:
        multi = MultiSinkConfig(sinks=[_parse_sink_config(child) for child in raw["multi"]["sinks"]])
    return SinkConfig(type=raw["type"], jsonl=jsonl, multi=multi)


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
        sink=_parse_sink_config(raw["sink"]),
        pipeline=PipelineConfig(**raw.get("pipeline", {})),
        evaluation=EvaluationConfig(**raw.get("evaluation", {})),
    )
```

(Note: `SinkConfig` references `MultiSinkConfig` before it's defined, and vice versa. This is safe
because of `from __future__ import annotations` at the top of the file — annotations are never
evaluated at class-definition time, only stored as strings, so the forward reference resolves fine
at both dataclass-construction and type-checking time.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_config.py -v`
Expected: PASS (all tests, old and new)

- [ ] **Step 5: Commit**

```bash
git add src/inference_ids/config.py tests/unit/test_config.py
git commit -m "feat: support nested multi/jsonl sink config and evaluation.label_map"
```

---

### Task 5: `factories.py` — dispatch `jsonl` and `multi` sink types

**Files:**
- Modify: `src/inference_ids/factories.py` (the `create_result_sink` function, currently lines 53-56)
- Test: `tests/unit/test_config.py`

**Interfaces:**
- Consumes: `SinkConfig`, `JSONLSinkConfig`, `MultiSinkConfig` (Task 4); `JSONLResultSink` (Task
  2); `MultiResultSink` (Task 3).
- Produces: `create_result_sink(config: AppConfig) -> ResultSink` now handles `type: "jsonl"` and
  `type: "multi"` in addition to the existing `type: "log"`. No signature change — existing
  callers (`bootstrap.py`) are unaffected.

- [ ] **Step 1: Write the failing tests**

Add to `tests/unit/test_config.py`. First, extend the imports at the top of the file:

```python
from inference_ids.adapters.jsonl_sink import JSONLResultSink
from inference_ids.adapters.multi_sink import MultiResultSink
from inference_ids.config import JSONLSinkConfig, MultiSinkConfig, SinkConfig
```

Then add:

```python
def test_create_result_sink_dispatches_jsonl(tmp_path, config_path):
    config = load_config(config_path)
    config.sink = SinkConfig(type="jsonl", jsonl=JSONLSinkConfig(path=str(tmp_path / "predictions.jsonl")))

    sink = create_result_sink(config)

    assert isinstance(sink, JSONLResultSink)


def test_create_result_sink_dispatches_multi_and_fans_out(tmp_path, config_path, caplog):
    import logging

    from inference_ids.domain.models import FlowRecord, Prediction

    jsonl_path = tmp_path / "predictions.jsonl"
    config = load_config(config_path)
    config.sink = SinkConfig(
        type="multi",
        multi=MultiSinkConfig(
            sinks=[
                SinkConfig(type="log"),
                SinkConfig(type="jsonl", jsonl=JSONLSinkConfig(path=str(jsonl_path))),
            ]
        ),
    )

    sink = create_result_sink(config)
    assert isinstance(sink, MultiResultSink)

    record = FlowRecord(
        uid="C1", ts=0.0, duration=1.5, orig_h="10.0.0.1", orig_p=1111,
        resp_h="10.0.0.2", resp_p=80, proto="tcp", service="http",
        conn_state="SF", history="ShADadfF", missed_bytes=0,
        orig_pkts=6, orig_ip_bytes=740, resp_pkts=8, resp_ip_bytes=1620,
        orig_bytes=350, resp_bytes=1200, local_orig=True, local_resp=False,
    )
    prediction = Prediction(label="benign", confidence=0.9, logits=[1.0], class_index=0)

    with caplog.at_level(logging.INFO, logger="inference_ids.results"):
        sink.emit(record, prediction)

    assert len(caplog.records) == 1
    assert "C1" in jsonl_path.read_text()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_config.py -v`
Expected: FAIL — both new tests with `ValueError: Unknown sink type: 'jsonl'` /
`ValueError: Unknown sink type: 'multi'`

- [ ] **Step 3: Rewrite `create_result_sink` in `factories.py`**

In `src/inference_ids/factories.py`, add imports at the top:

```python
from inference_ids.adapters.jsonl_sink import JSONLResultSink
from inference_ids.adapters.multi_sink import MultiResultSink
```

and change the existing `from inference_ids.config import AppConfig` line (line 9) to:

```python
from inference_ids.config import AppConfig, SinkConfig
```

then replace the existing `create_result_sink` function (currently):

```python
def create_result_sink(config: AppConfig) -> ResultSink:
    if config.sink.type == "log":
        return LoggingResultSink()
    raise ValueError(f"Unknown sink type: {config.sink.type!r}")
```

with:

```python
def create_result_sink(config: AppConfig) -> ResultSink:
    return _build_sink(config.sink)


def _build_sink(sink_config: SinkConfig) -> ResultSink:
    if sink_config.type == "log":
        return LoggingResultSink()
    if sink_config.type == "jsonl":
        return JSONLResultSink(path=sink_config.jsonl.path)
    if sink_config.type == "multi":
        return MultiResultSink(sinks=[_build_sink(child) for child in sink_config.multi.sinks])
    raise ValueError(f"Unknown sink type: {sink_config.type!r}")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_config.py -v`
Expected: PASS (all tests)

- [ ] **Step 5: Run the full suite to confirm nothing else broke**

Run: `uv run pytest -v`
Expected: PASS (every test in the repo)

- [ ] **Step 6: Commit**

```bash
git add src/inference_ids/factories.py tests/unit/test_config.py
git commit -m "feat: dispatch jsonl and multi sink types in create_result_sink"
```

---

### Task 6: Add `scikit-learn` dependency

**Files:**
- Modify: `pyproject.toml`
- Modify: `uv.lock` (regenerated, not hand-edited)

**Interfaces:**
- Consumes: nothing.
- Produces: `sklearn` importable via `uv run python -c "import sklearn"` and in `uv run pytest`.
  Task 7 (`evaluate.py`) imports `sklearn.metrics`.

- [ ] **Step 1: Edit `pyproject.toml`**

Change:

```toml
[dependency-groups]
dev = [
    "pytest>=8.0",
]
```

to:

```toml
[dependency-groups]
dev = [
    "pytest>=8.0",
    "scikit-learn>=1.3",
]
```

(In the `dev` group, not a new group — see Global Constraints above for why: it keeps sklearn out
of the `backend` Docker image, which already excludes `dev` via `--no-dev`, while keeping `uv run
pytest` and `uv run python -m inference_ids.evaluate` working with no extra flags on the host.)

- [ ] **Step 2: Sync and verify**

Run: `uv sync`
Expected: output includes a line installing `scikit-learn` (and its transitive deps `scipy`,
`joblib`, `threadpoolctl` if not already present)

Run: `uv run python -c "import sklearn; print(sklearn.__version__)"`
Expected: prints a version string >= 1.3, no error

- [ ] **Step 3: Confirm the Docker image is unaffected**

Run: `grep -- '--no-dev' docker/backend/Dockerfile`
Expected: the existing `uv sync --frozen --no-dev --no-install-project` line is unchanged — this
step is a read-only sanity check, not an edit; if it doesn't show `--no-dev`, stop and re-check the
Global Constraints section before proceeding, since that would mean scikit-learn is about to ship
in the live container image.

- [ ] **Step 4: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "chore: add scikit-learn to dev dependencies for offline evaluation"
```

---

### Task 7: `inference_ids.evaluate` — offline join + metrics CLI

**Files:**
- Create: `src/inference_ids/evaluate.py`
- Test: `tests/unit/test_evaluate.py`

**Interfaces:**
- Consumes: `AppConfig.evaluation.label_map`, `AppConfig.inference_engine.pytorch.class_names`
  (Task 4); the JSONL row shape written by `JSONLResultSink` (Task 2): `{"uid", "predicted_index",
  ...}`; `load_config` (existing, `config.py`).
- Produces: `load_jsonl(path) -> dict[str, dict]`, `load_labels(path) -> dict[str, int]`,
  `evaluate(predictions, labels, label_map, class_names) -> EvaluationResult`, and a `main()` CLI
  entrypoint runnable as `python -m inference_ids.evaluate`. Nothing else in the codebase depends
  on this module — it's a terminal, standalone tool.

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_evaluate.py`:

```python
import json

from inference_ids.evaluate import evaluate, load_jsonl, load_labels


def _write_jsonl(path, rows: list[dict]) -> None:
    path.write_text("\n".join(json.dumps(r) for r in rows) + "\n")


def test_load_jsonl_parses_happy_path(tmp_path):
    path = tmp_path / "rows.jsonl"
    _write_jsonl(path, [{"uid": "C1", "x": 1}, {"uid": "C2", "x": 2}])

    rows = load_jsonl(str(path))

    assert rows == {"C1": {"uid": "C1", "x": 1}, "C2": {"uid": "C2", "x": 2}}


def test_load_jsonl_skips_malformed_line(tmp_path, capsys):
    path = tmp_path / "rows.jsonl"
    path.write_text('{"uid": "C1", "x": 1}\nnot json\n{"uid": "C2", "x": 2}\n')

    rows = load_jsonl(str(path))

    assert set(rows.keys()) == {"C1", "C2"}
    assert "malformed" in capsys.readouterr().err


def test_load_labels_extracts_label_field(tmp_path):
    path = tmp_path / "labels.jsonl"
    _write_jsonl(path, [{"uid": "C1", "label": 0}, {"uid": "C2", "label": 2}])

    labels = load_labels(str(path))

    assert labels == {"C1": 0, "C2": 2}


def test_evaluate_computes_matched_and_unmatched_counts():
    predictions = {
        "C1": {"predicted_index": 0},
        "C2": {"predicted_index": 1},
        "C3": {"predicted_index": 0},  # no label -- unmatched prediction
    }
    labels = {
        "C1": 0,
        "C2": 1,
        "C4": 2,  # no prediction -- unmatched label
    }

    result = evaluate(predictions, labels, label_map={0: 0, 1: 1, 2: 2}, class_names=["benign", "scan", "dos"])

    assert result.matched == 2
    assert result.unmatched_predictions == 1
    assert result.unmatched_labels == 1
    assert "benign" in result.report_text
    assert result.confusion == [[1, 0, 0], [0, 1, 0], [0, 0, 0]]


def test_evaluate_applies_non_identity_label_map():
    predictions = {"C1": {"predicted_index": 0}}
    labels = {"C1": 5}  # dataset uses 5 to mean "benign"

    result = evaluate(predictions, labels, label_map={5: 0}, class_names=["benign", "scan", "dos"])

    assert result.matched == 1
    assert result.confusion == [[1, 0, 0], [0, 0, 0], [0, 0, 0]]


def test_evaluate_reports_zero_matched_without_raising_on_empty_overlap():
    predictions = {"C1": {"predicted_index": 0}}
    labels = {"C2": 1}

    result = evaluate(predictions, labels, label_map={0: 0, 1: 1, 2: 2}, class_names=["benign", "scan", "dos"])

    assert result.matched == 0
    assert result.unmatched_predictions == 1
    assert result.unmatched_labels == 1
    assert result.report_text == ""
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_evaluate.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'inference_ids.evaluate'`

- [ ] **Step 3: Write the implementation**

Create `src/inference_ids/evaluate.py`:

```python
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass

from sklearn.metrics import classification_report, confusion_matrix

from inference_ids.config import load_config


@dataclass
class EvaluationResult:
    matched: int
    unmatched_predictions: int
    unmatched_labels: int
    report_text: str
    confusion: list[list[int]]
    class_names: list[str]


def load_jsonl(path: str) -> dict[str, dict]:
    rows: dict[str, dict] = {}
    with open(path, encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                print(f"warning: skipping malformed line {line_number} in {path}", file=sys.stderr)
                continue
            rows[row["uid"]] = row
    return rows


def load_labels(path: str) -> dict[str, int]:
    return {uid: row["label"] for uid, row in load_jsonl(path).items()}


def _default_label_map(class_names: list[str]) -> dict[int, int]:
    return {i: i for i in range(len(class_names))}


def evaluate(
    predictions: dict[str, dict],
    labels: dict[str, int],
    label_map: dict[int, int],
    class_names: list[str],
) -> EvaluationResult:
    matched_uids = predictions.keys() & labels.keys()
    unmatched_predictions = len(predictions.keys() - labels.keys())
    unmatched_labels = len(labels.keys() - predictions.keys())

    if not matched_uids:
        return EvaluationResult(
            matched=0,
            unmatched_predictions=unmatched_predictions,
            unmatched_labels=unmatched_labels,
            report_text="",
            confusion=[],
            class_names=class_names,
        )

    y_true = [label_map.get(labels[uid], labels[uid]) for uid in matched_uids]
    y_pred = [predictions[uid]["predicted_index"] for uid in matched_uids]
    known_indices = list(range(len(class_names)))

    report_text = classification_report(
        y_true, y_pred, labels=known_indices, target_names=class_names, zero_division=0
    )
    confusion = confusion_matrix(y_true, y_pred, labels=known_indices).tolist()

    return EvaluationResult(
        matched=len(matched_uids),
        unmatched_predictions=unmatched_predictions,
        unmatched_labels=unmatched_labels,
        report_text=report_text,
        confusion=confusion,
        class_names=class_names,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate persisted predictions against a labeled dataset.")
    parser.add_argument("--predictions", required=True, help="Path to JSONL predictions written by JSONLResultSink")
    parser.add_argument("--labels", required=True, help="Path to JSONL labels: one {uid, label} object per line")
    parser.add_argument("--config", default="config/default.yaml", help="Config to read class_names/label_map from")
    args = parser.parse_args()

    config = load_config(args.config)
    class_names = config.inference_engine.pytorch.class_names
    label_map = config.evaluation.label_map or _default_label_map(class_names)

    predictions = load_jsonl(args.predictions)
    labels = load_labels(args.labels)

    result = evaluate(predictions, labels, label_map, class_names)

    print(f"Predictions: {len(predictions)}  Labels: {len(labels)}  Matched: {result.matched}")
    print(f"Unmatched predictions (no label): {result.unmatched_predictions}")
    print(f"Unmatched labels (no prediction): {result.unmatched_labels}")

    if result.matched == 0:
        print("\nNo overlapping uids between predictions and labels -- nothing to score.")
        return

    print("\n" + result.report_text)
    print("Confusion matrix (rows=true, cols=predicted):")
    header = "            " + "  ".join(f"{name:>8s}" for name in class_names)
    print(header)
    for name, row in zip(class_names, result.confusion):
        print(f"{name:>10s}  " + "  ".join(f"{v:8d}" for v in row))


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_evaluate.py -v`
Expected: PASS (all six tests)

- [ ] **Step 5: Manual smoke test of the CLI**

```bash
mkdir -p /tmp/eval-smoke
printf '{"uid": "C1", "predicted_index": 0, "predicted_label": "benign", "confidence": 0.9, "logits": [1.0, -1.0, -1.0]}\n{"uid": "C2", "predicted_index": 1, "predicted_label": "scan", "confidence": 0.8, "logits": [-1.0, 1.0, -1.0]}\n' > /tmp/eval-smoke/predictions.jsonl
printf '{"uid": "C1", "label": 0}\n{"uid": "C2", "label": 1}\n{"uid": "C3", "label": 2}\n' > /tmp/eval-smoke/labels.jsonl

uv run python -m inference_ids.evaluate \
  --predictions /tmp/eval-smoke/predictions.jsonl \
  --labels /tmp/eval-smoke/labels.jsonl \
  --config config/default.yaml
```

Expected: prints `Matched: 2`, `Unmatched predictions (no label): 0`, `Unmatched labels (no
prediction): 1`, followed by a classification report and a 3x3 confusion matrix. No traceback.

- [ ] **Step 6: Commit**

```bash
git add src/inference_ids/evaluate.py tests/unit/test_evaluate.py
git commit -m "feat: add offline evaluate CLI joining predictions and labels by uid"
```

---

### Task 8: Example config, volume mount, and `predictions/` directory

**Files:**
- Create: `config/validation.yaml`
- Create: `predictions/.gitkeep`
- Modify: `.gitignore`
- Modify: `docker-compose.yml`

**Interfaces:**
- Consumes: everything from Tasks 1-7 (this task only wires config/deployment, no new code).
- Produces: a runnable example showing how to configure the live pipeline for a validation run,
  and a mount point so its output survives on the host after the `backend` container exits.

- [ ] **Step 1: Create `config/validation.yaml`**

This is `config/default.yaml` with the sink changed to `multi` (log + jsonl) and an example
`evaluation.label_map` added:

```yaml
source:
  type: kafka
  kafka:
    bootstrap_servers: "localhost:9092"
    topic: "zeek-flows"
    group_id: "inference-ids"
    auto_offset_reset: "latest"

parser:
  type: json

feature_extractor:
  type: stub

inference_engine:
  type: pytorch
  pytorch:
    module: "inference_ids.reference_model"
    class_name: "IDSModel"
    state_dict_path: "/models/reference_ids_model.pth"
    init_kwargs:
      input_features: 11
      num_classes: 3
    class_names:
      - benign
      - scan
      - dos
    device: cpu
    precision: fp32

sink:
  type: multi
  multi:
    sinks:
      - type: log
      - type: jsonl
        jsonl:
          path: /data/predictions.jsonl

pipeline:
  batch_window_ms: 100
  max_batch_size: 256
  poll_timeout_seconds: 0.05

# Maps dataset label ints -> model class indices (0=benign, 1=scan, 2=dos here).
# Defaults to identity if omitted entirely; override per-key if your dataset's
# label encoding differs from config's inference_engine.pytorch.class_names order.
evaluation:
  label_map:
    0: 0
    1: 1
    2: 2
```

- [ ] **Step 2: Add the `predictions/` output directory**

```bash
mkdir -p predictions
touch predictions/.gitkeep
```

- [ ] **Step 3: Update `.gitignore`**

In `.gitignore`, add (following the existing `pcaps/`/`models/` pattern):

```
predictions/*
!predictions/.gitkeep
```

- [ ] **Step 4: Add the writable mount to `docker-compose.yml`**

In `docker-compose.yml`, under `backend.volumes`, change:

```yaml
    volumes:
      - ./config:/app/config:ro
      - ./models:/models:ro
      - kafka-data:/var/lib/kafka/data
```

to:

```yaml
    volumes:
      - ./config:/app/config:ro
      - ./models:/models:ro
      - ./predictions:/data:rw
      - kafka-data:/var/lib/kafka/data
```

- [ ] **Step 5: Verify the example config parses**

Run: `uv run python -c "from inference_ids.config import load_config; c = load_config('config/validation.yaml'); print(c.sink.type, c.sink.multi.sinks, c.evaluation.label_map)"`
Expected: prints `multi [SinkConfig(...), SinkConfig(...)] {0: 0, 1: 1, 2: 2}` with no traceback

- [ ] **Step 6: Verify docker compose config is still valid**

Run: `docker compose config --quiet`
Expected: no output, exit code 0 (validates YAML + compose schema without starting anything)

- [ ] **Step 7: Run the full test suite one more time**

Run: `uv run pytest -v`
Expected: PASS (every test in the repo)

- [ ] **Step 8: Commit**

```bash
git add config/validation.yaml predictions/.gitkeep .gitignore docker-compose.yml
git commit -m "feat: add validation.yaml example config and predictions/ mount for label validation"
```

---

## Manual end-to-end check (not automated, do after Task 8)

To confirm this works against the real stack (not just unit tests), after all 8 tasks:

1. `docker compose up -d backend` with `docker-compose.yml`'s new mount in place, but pointed at
   `config/validation.yaml` instead of `config/default.yaml` (temporarily edit the `backend`
   service's command/entrypoint config path, or copy `validation.yaml` over `default.yaml` for the
   test — either is fine, this is a one-off manual check, not part of the automated suite).
2. `docker compose up -d sensor`, replay any pcap via `./scripts/replay.sh pcaps/quickstart.pcap`.
3. Confirm `predictions/predictions.jsonl` appears on the host with rows matching the replayed
   flows' `uid`s (cross-check against `docker compose logs backend | grep flow=`).
4. Hand-write a matching `labels.jsonl` using a couple of those real `uid`s, then run
   `uv run python -m inference_ids.evaluate --predictions predictions/predictions.jsonl --labels
   labels.jsonl --config config/validation.yaml` and confirm it prints a real report.

This isn't a plan task because it depends on the live Docker stack (this repo's existing
convention per `docs/VERIFICATION.md` is to keep such checks manual/documented, not automated).
