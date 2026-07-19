# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Live network traffic classification pipeline: `tcpreplay` → `Zeek` (+ `zeek-kafka` plugin) → `Kafka` → PyTorch
inference, built as a hexagonal (ports & adapters) Python application so transport, parsing, feature
extraction, and the inference engine can each be swapped independently via config.

Deployed as two Docker containers, not the four you might expect from a naive reading of the pipeline:
- **`sensor`** — `tcpreplay` + Zeek + `zeek-kafka`, sharing one network interface (`eth0`) in the same
  container/netns. `tcpreplay-edit` (not plain `tcpreplay`) replays a pcap onto `eth0`, and Zeek captures on
  that same interface via Linux `AF_PACKET`'s default both-directions capture — it sees both incoming and
  Zeek's/tcpreplay's own locally-generated outgoing traffic, which is what lets the two processes share an
  interface with no bridge/veth between them and no dependency on the pcap's original IPs existing anywhere
  in the Docker network.
- **`backend`** — a single-node KRaft Kafka broker + the Python inference consumer, bundled in one
  container, with the consumer connecting to `localhost:9092`.

This split is a deliberate simplification for local dev, not a production topology — see the "Known gap"
section of README.md and `docs/VERIFICATION.md`'s closing notes for what changes in a real deployment
(no `tcpreplay`; Kafka and inference typically live on separate, independently-scaled infrastructure).

## Commands

```bash
uv sync                          # install deps (creates .venv/, uv.lock)
uv run pytest                    # run the full test suite
uv run pytest tests/unit/test_pipeline.py -v   # run a single test file
uv run pytest tests/unit/test_pipeline.py::test_run_one_window_batches_all_messages_within_window  # single test

make build                       # docker compose build (sensor + backend)
make up                          # docker compose up -d
make down                        # docker compose down
make replay PCAP=pcaps/quickstart.pcap [PPS=100]   # replay a pcap into the running sensor container
make test                        # uv run pytest

uv run python -m inference_ids.evaluate \
  --predictions predictions/predictions.jsonl --labels labels.jsonl --config config/validation.yaml
  # offline: score a JSONLResultSink run against a labels file (see "Offline label validation" below)
```

Before first `docker compose up`, a reference model checkpoint must exist at
`models/reference_ids_model.pth` (gitignored, referenced by `config/default.yaml`'s
`inference_engine.pytorch.state_dict_path: /models/reference_ids_model.pth`, which `docker-compose.yml`
mounts read-only into `backend`):

```bash
uv run python -c "
import torch
from inference_ids.reference_model import IDSModel
model = IDSModel(input_features=11, num_classes=3)
torch.save(model.state_dict(), 'models/reference_ids_model.pth')
"
```

Full staged bring-up/debugging order (don't wire the whole stack and then debug — build incrementally):
`docs/VERIFICATION.md`.

## Architecture

### Domain core (`src/inference_ids/domain/`)

- `models.py` — `FlowRecord` (frozen dataclass, 20 fields, one Zeek `conn.log` entry — the field set both
  parser adapters must map identically) and `Prediction` (`label`, `confidence`, `logits`).
- `ports.py` — five `typing.Protocol` classes every adapter implements: `FlowSource`, `FlowParser`,
  `FeatureExtractor`, `InferenceEngine`, `ResultSink`. These are the only types `application/pipeline.py`
  depends on — no adapter is ever imported there, or in `bootstrap.py`. That boundary is what makes the
  "swap in Kafka/parsing/inference for something else" requirement real rather than aspirational: swapping
  a concrete implementation is a one-line change in `factories.py` plus a config value, never a change to
  the pipeline or bootstrap.

### Application layer (`src/inference_ids/application/pipeline.py`)

`InferencePipeline` is the only place batching logic lives. Each call to `run_one_window()` collects records
from `FlowSource.poll()` until *either* `batch_window_ms` elapses *or* `max_batch_size` is reached (whichever
comes first), then does one `FeatureExtractor.extract()` + one `InferenceEngine.predict()` call for the whole
batch, and emits each result via `ResultSink.emit()`. An empty window never reaches `extract`/`predict`. The
clock is injectable (`clock: Callable[[], float] = time.monotonic`) specifically so tests don't depend on
real time — see `tests/unit/test_pipeline.py`'s `FakeClock`. `max_batch_size=1` is the documented way to get
near-immediate per-flow inference; larger values trade latency for throughput.

### Adapters (`src/inference_ids/adapters/`)

- `_zeek_coercion.py` — shared `coerce_float`/`coerce_int`/`coerce_bool`/`coerce_str` helpers. The load-
  bearing invariant: a missing JSON key (`.get()` returning `None`) and Zeek's TSV unset marker (`-`) must
  coerce to the *same* value, or the TSV and JSON parsers silently drift apart.
- `tsv_parser.py` / `json_parser.py` — `TSVFlowParser` (offline/regression, reads Zeek ASCII `conn.log`) and
  `JSONFlowParser` (the live path, unwraps zeek-kafka's `tag_json=T` `{"conn": {...}}` wrapper). Both route
  every field through the shared coercion helpers — never duplicate unset-handling in either parser.
  `tests/unit/test_parser_equivalence.py` is the project's core proof these two paths don't drift: it asserts
  both parsers produce byte-for-byte identical `FlowRecord`s from matched fixtures
  (`tests/fixtures/conn.log` / `conn.json` — if you touch one fixture, keep the other in sync field-for-field).
- `feature_extractor_stub.py` — `StubFeatureExtractor` is an **explicitly-labeled placeholder** (11 features
  derived directly from `FlowRecord`, not a production feature set). The real feature contract belongs to a
  separate model-training pipeline outside this repo. `feature_count = 11` is load-bearing: it must match
  `config/default.yaml`'s `inference_engine.pytorch.init_kwargs.input_features`.
- `pytorch_inference.py` — `PyTorchInferenceEngine` loads a model class dynamically by dotted module/class
  name (config-driven, so any `nn.Module` can be swapped in without touching this file); device/precision
  come from config, never hardcoded. The model class it loads by default, `IDSModel`, lives one level up at
  `src/inference_ids/reference_model.py` (not in `adapters/`) — a placeholder architecture with random-init
  weights for exercising the pipeline, not a trained model.
- `kafka_source.py` — `KafkaFlowSource` is transport-only: `poll()` returns the JSON-decoded message value
  exactly as received, still wrapped as `{"conn": {...}}` — unwrapping is `JSONFlowParser`'s job, not this
  adapter's. Only errors where `confluent_kafka`'s `error.fatal()` is true are raised; everything else
  (including `UNKNOWN_TOPIC_OR_PART`, the normal state before the sensor's first record creates the topic) is
  treated as "no message this cycle" and retried on the next poll — this was a real crash-on-fresh-boot bug,
  not a defensive nicety, so don't revert it to raising on every error.
- `log_sink.py` — `LoggingResultSink` logs one line per prediction via `logging.getLogger("inference_ids.results")`.
- `jsonl_sink.py` — `JSONLResultSink` persists one JSON line per prediction (`uid`, `predicted_index`,
  `predicted_label`, `confidence`, `logits`), keyed by Zeek `uid` so predictions can later be joined against
  a labels file. It **truncates its output file on construction** (open in `"w"` mode) specifically so
  re-running a validation pass never silently mixes in a prior run's predictions.
- `multi_sink.py` — `MultiResultSink` fans one `emit()` call out to a list of other `ResultSink`s, in order
  (e.g. `log` + `jsonl` together) — used by `config/validation.yaml` so console logging and JSONL persistence
  happen from a single pipeline run.

### Wiring (`config.py`, `factories.py`, `bootstrap.py`)

`config.py`'s `AppConfig` is a dataclass tree parsed from YAML (`config/default.yaml`), with a `type:` field
per port. `factories.py` has one `create_*` function per port that dispatches on that `type` and constructs
the concrete adapter — this file is the *only* place allowed to import concrete adapter classes; everything
it returns is typed as the port Protocol. Adding a new adapter means adding one `elif` branch here, never
touching `pipeline.py` or `bootstrap.py`. `bootstrap.py`'s `build_pipeline(config)` calls all five factories
and constructs the `InferencePipeline`; `__main__.py` is the CLI entrypoint (`python -m inference_ids
--config <path>`).

### Offline label validation (`evaluate.py`, `config/validation.yaml`)

Fully decoupled from the live pipeline — checking happens *after* a batch is classified, not on the fly, and
none of `application/pipeline.py`, `bootstrap.py`, or the five domain ports know this feature exists.
`config/validation.yaml` swaps `sink` to `multi` (`log` + `jsonl`, writing to `/data/predictions.jsonl`,
`docker-compose.yml`'s mount for the host's `predictions/` dir) and adds an `evaluation.label_map` section
that only `evaluate.py`'s CLI reads (`AppConfig.evaluation`, `config.py`) — the live pipeline ignores it
entirely. `evaluate.py` joins predictions and a separately-supplied `{uid, label}` JSONL labels file on
`uid`, remaps dataset label ints through `label_map` (identity if omitted) to the model's class-index order
(`inference_engine.pytorch.class_names`), and prints matched/unmatched counts plus an `sklearn`
classification report and confusion matrix — unmatched predictions/labels are always counted and shown,
never silently dropped.

### Zeek config (`docker/sensor/local.zeek`)

Two non-obvious things, both found only by actually running the stack (not from documentation):
1. Uses `Log::add_filter(Conn::LOG, ...)` directly, not a global `redef Log::default_writer =
   Log::WRITER_KAFKAWRITER`. The global redef sends *every* log stream to Kafka (not just `Conn::LOG`) and
   collides with the filter's own `"conn"` path (Zeek renames it to `"conn-2"`), which breaks
   `JSONFlowParser`'s assumption that records are wrapped under a literal `"conn"` key.
2. A `Conn::log_policy` hook vetoes logging connections to/from port 9092 in either direction. Zeek's
   `AF_PACKET` capture on `eth0` also picks up the sensor's *own* librdkafka producer connection to
   `backend:9092` as if it were monitored traffic; without this it gets logged and republished in a
   self-referential loop. Both `orig_p` and `resp_p` must be checked — Zeek sometimes assigns the broker
   side as "originator" when it doesn't see the connection's SYN.

## Testing conventions

All adapters are tested against hand-rolled fakes, not mocks (see `FakeConsumer`/`FakeMessage` in
`test_kafka_source.py`, `FakeClock` in `test_pipeline.py`) — no test exercises a real Kafka broker or Zeek
process; that's what `docs/VERIFICATION.md` is for. `pyproject.toml` sets `pythonpath = ["src"]` so tests
import `inference_ids` without an editable install.
