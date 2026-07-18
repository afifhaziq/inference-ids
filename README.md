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
- `src/inference_ids/adapters/` — concrete implementations (Kafka, JSON/TSV parsers, PyTorch, logging sink).
- `src/inference_ids/config.py` + `factories.py` — YAML config selects an adapter per port by `type`; this is
  the swap point (see `config/default.yaml`).
- `docker/sensor/`, `docker/backend/`, `docker-compose.yml` — the two-container stack.
- `docs/VERIFICATION.md` — manual, staged verification runbook (build and wire incrementally, don't debug
  the whole stack at once).

## Known gap

`StubFeatureExtractor` is a placeholder. The production feature contract (list, order, units) is owned by a
separate model-training pipeline and is not implemented here — see `docs/VERIFICATION.md` step 4.

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
