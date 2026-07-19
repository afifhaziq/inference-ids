# Offline label validation — design

## Problem

The pipeline currently has no way to check predictions against ground truth. `LoggingResultSink`
only prints one line per prediction; there's no port, adapter, or script that ingests a labeled
dataset, joins it against predictions, or computes accuracy. The model itself
(`IDSModel`/`reference_model.py`) is also random-init and the feature extractor
(`StubFeatureExtractor`) is an explicit placeholder — this design does not address either of
those; it only builds the plumbing to compare predictions against labels once a real model and
feature extractor exist.

This feature is **optional**: it does not change the live pipeline's default behavior, and
nothing about `application/pipeline.py`, `bootstrap.py`, or the five domain ports changes.

## Requirements (from stakeholder conversation)

- Labels arrive as a file with exactly two fields per record: Zeek `uid` (string) and `label`
  (integer). Format: **JSONL** (one `{"uid": ..., "label": ...}` object per line) — chosen over a
  single JSON array or a uid-keyed object because it streams (no full-file parse), tolerates
  partial/appended files, and matches the format predictions are persisted in (see below).
- Checking happens **after** classifying the whole dataset, not on the fly. There is no code path
  where a live prediction is compared to a label during pipeline execution.
- The label integer's mapping to model class indices is not assumed to match
  `config/default.yaml`'s `class_names` ordering — it must be config-driven and overridable,
  defaulting to identity.
- Metrics: use `sklearn.metrics.classification_report` + `confusion_matrix`, plus explicit
  unmatched-record counts (predictions with no label, and labels with no matching prediction —
  the latter matters because we've already observed flows that take minutes to appear in Kafka
  due to Zeek's connection-expiry timers; silently dropping them would hide that).
- Output: console only. No file report in this iteration.

## Architecture

```
Live pipeline (unchanged path, new optional sink):
  Kafka -> JSONFlowParser -> StubFeatureExtractor -> PyTorchInferenceEngine
                                                            |
                                                            v
                                      MultiResultSink(sinks=[LoggingResultSink, JSONLResultSink])
                                                            |
                                              +-------------+-------------+
                                              v                           v
                                         stdout logs              predictions.jsonl
                                                                   (uid, predicted_index,
                                                                    predicted_label,
                                                                    confidence, logits)

Offline, after the run finishes (separate, decoupled step):
  labels.jsonl    --+
                     +--> inference_ids.evaluate  --> sklearn classification_report
  predictions.jsonl--+       (join on uid)              + confusion matrix
                                                          + unmatched-record counts
```

Evaluation is a stateless offline step: it reads two files and joins them by `uid`. It has no
knowledge of Kafka, Zeek, or the live pipeline.

## Components

### Domain (`domain/models.py`)

Add one field to `Prediction`:

```python
@dataclass(frozen=True)
class Prediction:
    label: str
    confidence: float
    logits: list[float] = field(default_factory=list)
    class_index: int = -1   # argmax index; -1 = unset, for any InferenceEngine that doesn't set it
```

`PyTorchInferenceEngine.predict()` (`adapters/pytorch_inference.py`) already computes `index` via
`torch.max` — it just needs to pass it through to `Prediction(class_index=index, ...)`.
Backward compatible: existing callers that don't care about `class_index` are unaffected.

### `adapters/jsonl_sink.py` — `JSONLResultSink`

```python
class JSONLResultSink:
    def __init__(self, path: str) -> None: ...
    def emit(self, record: FlowRecord, prediction: Prediction) -> None:
        # appends one line: {"uid", "predicted_index", "predicted_label", "confidence", "logits"}
```

Opens the file once in append mode at construction; one `json.dumps()` + newline per `emit()`.

### `adapters/multi_sink.py` — `MultiResultSink`

```python
class MultiResultSink:
    def __init__(self, sinks: list[ResultSink]) -> None: ...
    def emit(self, record: FlowRecord, prediction: Prediction) -> None:
        for sink in self._sinks:
            sink.emit(record, prediction)
```

Generic fan-out — not specific to log+jsonl, so it composes for free if another sink type shows
up later.

### Config (`config.py`, `config/default.yaml`)

Extend `sink:` to support `type: multi` (a list of child sink configs, each dispatched through the
same `create_result_sink` factory) and `type: jsonl` (a `path`). Example:

```yaml
sink:
  type: multi
  multi:
    sinks:
      - type: log
      - type: jsonl
        jsonl:
          path: /data/predictions.jsonl
```

Add a new top-level `evaluation:` section, read only by the offline `evaluate` CLI (not by the
live pipeline):

```yaml
evaluation:
  label_map:
    0: 0
    1: 1
    2: 2
```

Defaults to identity mapping (`{i: i for i in range(len(class_names))}`) if the section is
omitted, so existing configs need no changes.

### `factories.py`

`create_result_sink(config: AppConfig)` currently dispatches directly on `config.sink.type`. The
`multi` branch needs to recurse on a *sink* config, not a full `AppConfig`, so the dispatch logic
is extracted into a helper that both the public function and the recursive case share:

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

This requires `config.py`'s sink config dataclass (`SinkConfig`) to be usable standalone — each
entry in `multi.sinks:` parses into the same `SinkConfig` shape as the top-level `sink:` block, so
nesting is uniform (a `multi` sink could itself contain another `multi`, though nothing in this
design needs that). Same dispatch-on-`type` pattern already used for all five ports; no changes to
`pipeline.py` or `bootstrap.py`.

### `src/inference_ids/evaluate.py` — offline CLI

```bash
uv run --group eval python -m inference_ids.evaluate \
    --predictions predictions.jsonl \
    --labels labels.jsonl \
    --config config/default.yaml
```

Core functions (pure, no side effects beyond the two file reads — unit-testable in isolation):

```python
def load_jsonl(path: str) -> dict[str, dict]:
    """uid -> row. Malformed lines are skipped with a warning, not fatal."""

def evaluate(
    predictions: dict[str, dict],   # uid -> {"predicted_index": int, ...}
    labels: dict[str, int],         # uid -> raw label int
    label_map: dict[int, int],      # dataset label -> model class index
    class_names: list[str],
) -> EvaluationResult:
    """Joins on uid, computes sklearn classification_report + confusion_matrix,
    and unmatched-record counts. Raises no exception on zero overlap — reports it clearly instead."""
```

Join semantics:
- `matched = predictions.keys() & labels.keys()`; `y_true`/`y_pred` built only from matched uids.
- `unmatched_predictions = predictions.keys() - labels.keys()` (untagged traffic) and
  `unmatched_labels = labels.keys() - predictions.keys()` (e.g. a flow still behind Zeek's
  connection-expiry timer) are both counted and printed, never silently dropped.
- `classification_report(y_true, y_pred, labels=range(len(class_names)), target_names=class_names)`
  — passing `labels=` explicitly keeps the report bounded to known classes even if a stray label
  value shows up (which usually means a `label_map` typo, and gets flagged separately).
- Zero matched uids prints a clear "no overlap" message instead of letting sklearn raise.

### Dependency and deployment notes

- **`scikit-learn` is a new optional dependency group**, not a core dependency:
  `[dependency-groups] eval = ["scikit-learn>=1.3"]` in `pyproject.toml`. The `backend` Docker
  image installs via `uv sync --frozen --no-dev --no-install-project` and has no reason to ship
  sklearn for a tool that runs offline on the host.
- **`docker-compose.yml` needs a new writable mount** on `backend` so `JSONLResultSink`'s output
  file is visible on the host afterward, mirroring the existing read-only `./models:/models:ro`
  mount but read-write: `./predictions:/data:rw`.

## Testing plan

Hand-rolled fakes, no mocks, no real Kafka/Zeek — matching this repo's existing convention.

- `test_jsonl_sink.py` — one well-formed JSON line per `emit()`, appends across multiple calls,
  round-trips via `json.loads` (uses `tmp_path`).
- `test_multi_sink.py` — `MultiResultSink` calls `emit()` on every wrapped sink in order, using a
  `FakeSink` that records calls (same style as `FakeConsumer` in `test_kafka_source.py`).
- `test_evaluate.py`:
  - `load_jsonl`: happy path, and a malformed line is skipped while the rest still parses.
  - `evaluate()`: small deterministic fixture (~6-8 uids) covering matched pairs, an unmatched
    prediction, an unmatched label, and a non-identity `label_map` — assert exact
    accuracy/confusion-matrix/unmatched-counts.
  - Zero-overlap edge case produces a clear message, not an exception.
- `test_factories.py` (extends existing coverage) — `create_result_sink` dispatches `jsonl` and
  `multi` correctly; `multi` recurses to build child sinks.
- `test_config.py` (extends existing coverage) — `evaluation.label_map` parses; defaults to
  identity when the section is omitted.
- `test_pytorch_inference.py` (extends existing) — `Prediction.class_index` matches the argmax
  index already asserted for `label`.

No new manual-verification step in `docs/VERIFICATION.md` is needed beyond what exists — this is
pure unit-test territory since evaluation is fully decoupled from Kafka/Zeek.

## Explicitly out of scope

- Training a real model or building the real feature contract (unchanged: still owned by a
  separate model-training pipeline per `docs/VERIFICATION.md` step 4).
- On-the-fly / streaming validation during pipeline execution.
- A file-based (JSON) evaluation report — console output only for this iteration.
- Any change to `application/pipeline.py`, `bootstrap.py`, or the five domain port `Protocol`s.
