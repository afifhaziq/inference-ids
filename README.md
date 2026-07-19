# inference-ids

Live network traffic classification: `tcpreplay` -> `Zeek` (+ `zeek-kafka`) -> `Kafka` -> PyTorch inference,
built as a hexagonal (ports & adapters) Python application so any stage (transport, parsing, feature
extraction, inference engine) can be swapped independently.

Runs as two containers: `sensor` (tcpreplay + Zeek, sharing one interface) and `backend` (Kafka + the Python
inference consumer). See `docs/VERIFICATION.md` for why, and for what would change in a real deployment.

## Layout

- `src/inference_ids/domain/` — `FlowRecord`, `Prediction`, and the five ports (`FlowSource`, `FlowParser`,
  `FeatureExtractor`, `InferenceEngine`, `ResultSink`).
- `src/inference_ids/application/pipeline.py` — orchestrates the ports; the only place batching logic lives.
- `src/inference_ids/adapters/` — concrete implementations (Kafka, JSON/TSV parsers, PyTorch, and three
  `ResultSink`s: `log` prints one line per prediction, `jsonl` persists predictions keyed by Zeek `uid`,
  `multi` fans out to several sinks at once, e.g. `log` + `jsonl` together).
- `src/inference_ids/config.py` + `factories.py` — YAML config selects an adapter per port by `type`; this is
  the swap point (see `config/default.yaml`).
- `src/inference_ids/evaluate.py` — standalone offline CLI, decoupled from the live pipeline: joins a
  `jsonl`-sink predictions file against a labeled dataset by `uid` and prints an accuracy report. See
  "Offline label validation" below.
- `docker/sensor/`, `docker/backend/`, `docker-compose.yml` — the two-container stack.
- `docs/VERIFICATION.md` — manual, staged verification runbook (build and wire incrementally, don't debug
  the whole stack at once).

## Known gap

`StubFeatureExtractor` is a placeholder. The production feature contract (list, order, units) is owned by a
separate model-training pipeline and is not implemented here — see `docs/VERIFICATION.md` step 4.

## Offline label validation

Optional, and fully decoupled from the live pipeline — checking happens *after* a batch is classified, not
on the fly. Point the pipeline at `config/validation.yaml` instead of `config/default.yaml` to persist
predictions alongside the usual console logging:

```yaml
sink:
  type: multi
  multi:
    sinks:
      - type: log
      - type: jsonl
        jsonl:
          path: /data/predictions.jsonl   # /data is docker-compose.yml's writable predictions/ mount
```

Then, once you have a labeled dataset as JSONL (one `{"uid": "<zeek-uid>", "label": <int>}` object per
line — `label` is the dataset's class index, mapped to this model's class order via
`config/validation.yaml`'s `evaluation.label_map`, identity by default), score the run:

```bash
uv run python -m inference_ids.evaluate \
  --predictions predictions/predictions.jsonl \
  --labels labels.jsonl \
  --config config/validation.yaml
```

Prints matched/unmatched counts (a label with no matching prediction usually means that flow hadn't been
logged by Zeek yet — see `docs/VERIFICATION.md`'s notes on `conn.log`'s termination-triggered write timing)
plus an `sklearn` classification report and confusion matrix. `JSONLResultSink` truncates its output file on
each pipeline start, so re-running a validation pass never mixes in a prior run's predictions.

## Quick start

```bash
uv sync
uv run pytest
uv run python -c "
import torch
from inference_ids.reference_model import IDSModel
model = IDSModel(input_features=11, num_classes=3)
torch.save(model.state_dict(), 'models/reference_ids_model.pth')
"
docker compose build
make replay PCAP=pcaps/quickstart.pcap
```
