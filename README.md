# inference-ids

Live network traffic classification pipeline: `tcpreplay` → `Zeek` (+ `zeek-kafka`) → `Kafka` → PyTorch
inference. Built as a hexagonal (ports & adapters) Python application so each stage — transport, parsing,
feature extraction, inference engine, result output — can be swapped independently via config, without
touching the pipeline's own code.

```
pcap file --tcpreplay--> [ Zeek + zeek-kafka ]  --produces-->  [ Kafka topic: zeek-flows ]
                                                                        |
                                                                        v
                                                          [ Python inference pipeline ]
                                                          poll -> parse -> extract features
                                                          -> predict -> emit result
```

Deployed as **two** Docker containers, not the four you might expect from a naive reading of the pipeline
above:

- **`sensor`** — `tcpreplay`/`tcpreplay-edit` + Zeek + the `zeek-kafka` plugin, sharing one network
  interface (`eth0`) in the same container. Zeek's `AF_PACKET` capture sees both directions of traffic on
  that interface, which is what lets `tcpreplay` and Zeek coexist with no bridge/veth between them.
- **`backend`** — a single-node KRaft Kafka broker + the Python inference consumer, bundled together, with
  the consumer connecting to `localhost:9092`.

This split is a deliberate simplification for local development, not a production topology — see "Known
gaps and limitations" below and `docs/VERIFICATION.md`'s closing notes for what changes in a real
deployment (no `tcpreplay`; Kafka and inference typically live on separate, independently-scaled
infrastructure).

## Table of contents

- [Repository layout](#repository-layout)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Configuration](#configuration)
- [Running the tests](#running-the-tests)
- [Running the full stack](#running-the-full-stack)
- [Offline label validation](#offline-label-validation)
- [Known gaps and limitations](#known-gaps-and-limitations)

## Repository layout

```
.
├── src/inference_ids/
│   ├── domain/
│   │   ├── models.py            # FlowRecord (20-field Zeek conn.log record), Prediction
│   │   └── ports.py             # FlowSource, FlowParser, FeatureExtractor, InferenceEngine, ResultSink
│   ├── application/
│   │   └── pipeline.py          # InferencePipeline — the only place batching logic lives
│   ├── adapters/
│   │   ├── _zeek_coercion.py    # shared coerce_float/int/bool/str helpers (TSV "-" == missing JSON key)
│   │   ├── tsv_parser.py        # TSVFlowParser — offline/regression, reads Zeek ASCII conn.log
│   │   ├── json_parser.py       # JSONFlowParser — the live path, unwraps zeek-kafka's {"conn": {...}}
│   │   ├── feature_extractor_stub.py  # StubFeatureExtractor — placeholder, 11 features from FlowRecord
│   │   ├── kafka_source.py      # KafkaFlowSource — transport-only Kafka consumer wrapper
│   │   ├── pytorch_inference.py # PyTorchInferenceEngine — loads any nn.Module by dotted module/class name
│   │   ├── log_sink.py          # LoggingResultSink — one log line per prediction
│   │   ├── jsonl_sink.py        # JSONLResultSink — persists predictions as JSONL, keyed by Zeek uid
│   │   └── multi_sink.py        # MultiResultSink — fans out to several ResultSinks at once
│   ├── config.py                # AppConfig dataclass tree, parsed from YAML
│   ├── factories.py             # one create_* function per port; the only place adapters are imported
│   ├── bootstrap.py             # build_pipeline(config) — wires all five ports into an InferencePipeline
│   ├── reference_model.py       # IDSModel — placeholder nn.Module, random-init weights
│   ├── evaluate.py              # standalone offline CLI: score predictions.jsonl against a labels file
│   └── __main__.py              # live pipeline entrypoint: python -m inference_ids --config <path>
├── tests/
│   ├── fixtures/                # conn.log / conn.json — matched fixtures for parser-equivalence testing
│   └── unit/                    # one test file per adapter/module, hand-rolled fakes (no mocks)
├── config/
│   ├── default.yaml             # the live pipeline's default config (sink: log)
│   └── validation.yaml          # example config for a label-validation run (sink: multi [log, jsonl])
├── docker/
│   ├── sensor/                  # tcpreplay + Zeek + zeek-kafka image, local.zeek site policy
│   └── backend/                 # Kafka (KRaft) + Python inference consumer image
├── docker-compose.yml           # the two-container stack (sensor, backend)
├── docs/VERIFICATION.md         # manual, staged bring-up/debugging runbook
├── scripts/replay.sh            # copies a pcap into pcaps/ and replays it into the running sensor
├── pcaps/                       # pcap files for replay (gitignored except .gitkeep)
├── models/                      # model checkpoints (gitignored except .gitkeep)
├── predictions/                 # JSONLResultSink output, mounted read-write into backend (gitignored)
├── Makefile                     # build / up / down / replay / test shortcuts
└── pyproject.toml               # uv-managed deps; pythonpath=["src"] so tests import inference_ids directly
```

## Prerequisites

- [`uv`](https://docs.astral.sh/uv/) (Python 3.12 is pinned via `pyproject.toml`/`uv.lock`)
- Docker + Docker Compose (for the full sensor/backend stack)
- A pcap to replay — `pcaps/quickstart.pcap` is checked in for a first run; drop your own into `pcaps/`

## Setup

```bash
git clone https://github.com/afifhaziq/inference-ids.git
cd inference-ids
uv sync                 # installs deps into .venv/, honoring uv.lock
```

Before the first `docker compose up`, a reference model checkpoint must exist at
`models/reference_ids_model.pth` (gitignored — `config/default.yaml`'s
`inference_engine.pytorch.state_dict_path` points at it, and `docker-compose.yml` mounts it read-only into
`backend`). `IDSModel` here is an explicitly-labeled placeholder architecture with random-init weights, not
a trained model — generating a checkpoint just saves its random initial state so the pipeline has something
to load:

```bash
uv run python -c "
import torch
from inference_ids.reference_model import IDSModel
model = IDSModel(input_features=11, num_classes=3)
torch.save(model.state_dict(), 'models/reference_ids_model.pth')
"
```

## Configuration

Everything is driven by one YAML file (`config/default.yaml` by default, passed via `--config` to both
`python -m inference_ids` and `python -m inference_ids.evaluate`). Each section has a `type:` field that
`factories.py` dispatches on to pick a concrete adapter — this is the only place adapters are ever imported
by name, so adding a new one is a config value plus one `elif` branch, never a change to the pipeline
itself.

| Section | `type` options | Notes |
|---|---|---|
| `source` | `kafka` | `bootstrap_servers`, `topic`, `group_id`, `auto_offset_reset` |
| `parser` | `json`, `tsv` | `json` is the live Kafka path; `tsv` reads Zeek's ASCII `conn.log` for offline/regression use |
| `feature_extractor` | `stub` | `StubFeatureExtractor` — 11 features derived directly from `FlowRecord`; **not** a production feature set (see Known gaps) |
| `inference_engine` | `pytorch` | `module`/`class_name` load any `nn.Module` dynamically; `state_dict_path`, `init_kwargs`, `class_names`, `device`, `precision` |
| `sink` | `log`, `jsonl`, `multi` | `log` prints one line per prediction; `jsonl` persists predictions keyed by Zeek `uid` (`jsonl.path`); `multi` fans out to a `sinks:` list of any of these, e.g. `log` + `jsonl` together |
| `pipeline` | — | `batch_window_ms`, `max_batch_size`, `poll_timeout_seconds` — see below |
| `evaluation` | — | `label_map` — only read by the offline `evaluate` CLI, not the live pipeline; see [Offline label validation](#offline-label-validation) |

`InferencePipeline.run_one_window()` collects records from `FlowSource.poll()` until either
`batch_window_ms` elapses or `max_batch_size` is reached, then does one `extract()` + `predict()` call for
the whole batch. Set `max_batch_size: 1` for near-immediate per-flow inference; larger values trade latency
for throughput. An empty window never reaches `extract`/`predict`.

Two ready-made configs are checked in:
- **`config/default.yaml`** — the live pipeline's default, `sink: {type: log}`.
- **`config/validation.yaml`** — identical except `sink: {type: multi}` (log + jsonl) and an example
  `evaluation.label_map`, for a validation run — see below.

## Running the tests

```bash
uv run pytest                                                          # full suite
uv run pytest tests/unit/test_pipeline.py -v                           # one file
uv run pytest tests/unit/test_pipeline.py::test_run_one_window_batches_all_messages_within_window  # one test
```

All adapters are tested against hand-rolled fakes, not mocks — no test exercises a real Kafka broker or
Zeek process; that's what `docs/VERIFICATION.md` is for.

## Running the full stack

```bash
make build                              # docker compose build (sensor + backend)
make up                                 # docker compose up -d
make replay PCAP=pcaps/quickstart.pcap  # replay a pcap into the running sensor (PPS=<n> optional, default 100)
```

Bring-up order matters: `backend` should be up first so Kafka is listening before `sensor` starts producing
(`docker-compose.yml`'s `depends_on` handles the ordering; `docker/backend/entrypoint.sh` waits for Kafka to
accept connections before starting the consumer). Watch predictions land:

```bash
docker compose logs -f backend | grep flow=
```

**Do not wire the whole stack and then debug it end-to-end** — `docs/VERIFICATION.md` is a staged runbook
(capture-only, then Kafka writer, then parser equivalence, then feature provenance, then single-sample
inference, then batching) that isolates failures to one layer at a time; follow it if anything doesn't work
on the first try.

To reproduce a run from a clean clone end-to-end:

```bash
git clone https://github.com/afifhaziq/inference-ids.git && cd inference-ids
uv sync
uv run python -c "
import torch
from inference_ids.reference_model import IDSModel
model = IDSModel(input_features=11, num_classes=3)
torch.save(model.state_dict(), 'models/reference_ids_model.pth')
"
docker compose build
docker compose up -d backend
docker compose up -d sensor
./scripts/replay.sh pcaps/quickstart.pcap
docker compose logs backend | grep flow=
```

`make down` (`docker compose down`) tears the stack down.

## Offline label validation

Optional, and fully decoupled from the live pipeline — checking happens *after* a batch is classified, not
on the fly, and none of `application/pipeline.py`, `bootstrap.py`, or the five domain ports know this
feature exists.

**1. Point the pipeline at `config/validation.yaml`** instead of `config/default.yaml` (edit
`docker/backend/entrypoint.sh`'s `--config` argument, or copy `validation.yaml` over `default.yaml` for a
one-off run) so predictions get persisted alongside the usual console logging:

```yaml
sink:
  type: multi
  multi:
    sinks:
      - type: log
      - type: jsonl
        jsonl:
          path: /data/predictions.jsonl   # /data is docker-compose.yml's writable ./predictions mount
```

`JSONLResultSink` truncates its output file when the pipeline starts, so re-running a validation pass never
silently mixes in a prior run's predictions. Each line is
`{"uid", "predicted_index", "predicted_label", "confidence", "logits"}`.

**2. Bring up the stack and replay your pcap(s)** as in "Running the full stack" above.
`predictions/predictions.jsonl` appears on the host as flows are logged.

**3. Supply a labels file** as JSONL, one object per line, matched to predictions by Zeek `uid`:

```json
{"uid": "CHhAvVGS1DHFjwGM9", "label": 0}
{"uid": "Cuu0j21aaqoTUrxsAj", "label": 2}
```

`label` is your dataset's class index. If it doesn't already match this model's class order
(`inference_engine.pytorch.class_names` in the config, e.g. `[benign, scan, dos]` → `0, 1, 2`), override
`evaluation.label_map` in the config to translate — it defaults to identity if omitted.

**4. Score it:**

```bash
uv run python -m inference_ids.evaluate \
  --predictions predictions/predictions.jsonl \
  --labels labels.jsonl \
  --config config/validation.yaml
```

This prints matched/unmatched counts, an `sklearn` classification report, and a confusion matrix.
Predictions with no matching label (untagged traffic) and labels with no matching prediction are both
counted and shown, never silently dropped — a nonzero "unmatched labels" count usually means that flow
hadn't been logged by Zeek yet when you captured `predictions.jsonl` (`conn.log` entries are
termination-triggered, so a long-lived or not-yet-closed flow can take a while to appear — see
`docs/VERIFICATION.md`'s notes on this).

## Known gaps and limitations

- **`StubFeatureExtractor` is a placeholder** — 11 features derived directly from `FlowRecord`, not a
  production feature set. The real feature contract (list, order, units) is owned by a separate
  model-training pipeline outside this repo. Do not swap in a production `FeatureExtractor` until that's
  been confirmed — see `docs/VERIFICATION.md` step 4.
- **`IDSModel` is untrained** — random-init weights, for exercising the pipeline's plumbing only. Nothing in
  this repo trains a real model or validates against a real labeled dataset; the label-validation feature
  above is the scoring plumbing, not a trained classifier.
- **The `sensor`/`backend` two-container split is a local-dev convenience, not a deployment topology.** A
  real deployment drops `tcpreplay` entirely (Zeek listens on a real mirrored interface instead) and very
  likely puts Kafka and the inference consumer on separate, independently-scaled infrastructure rather than
  one container.
- **WSL2 / Docker Desktop specifics** (if that's your environment): the internal bridge's MTU can be
  sub-1500 (check `docker compose exec sensor ip link show eth0` if a pcap with large frames shows
  unexplained truncation), and throughput numbers from this environment aren't representative of bare-metal
  or edge-device (e.g. Jetson) numbers.
