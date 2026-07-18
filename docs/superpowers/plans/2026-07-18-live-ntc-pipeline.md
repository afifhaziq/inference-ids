# Live NTC Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a replay-driven live network traffic classification pipeline (tcpreplay → Zeek → Kafka → PyTorch inference) as a hexagonal (ports & adapters) Python application, so transport (Kafka), parsing (JSON/TSV), feature extraction, and the inference engine can each be swapped independently.

**Architecture:** A framework-agnostic domain/application core (`FlowRecord`, `Prediction`, and five `Protocol` ports: `FlowSource`, `FlowParser`, `FeatureExtractor`, `InferenceEngine`, `ResultSink`) orchestrated by a single `InferencePipeline` use case. Concrete adapters (Kafka, JSON/TSV parsers, PyTorch, logging sink) implement the ports and are wired together at the edge by a config-driven factory, so no adapter is imported by name inside the application core. Two Docker services run on one user-defined bridge network: **`sensor`** (tcpreplay + Zeek + zeek-kafka, sharing one network interface) and **`backend`** (Kafka in KRaft mode + the Python inference app, bundled together).

**Tech Stack:** Python 3.12, `uv` for packaging, `confluent-kafka` (librdkafka), PyTorch, PyYAML, numpy, pytest, Docker Compose, Zeek 6.x + `SeisoLLC/zeek-kafka`.

## Global Constraints

- Target environment for this build is Docker Compose on WSL2. Do not optimize for Jetson/bare-metal; do not choose anything that blocks it later (spec §1, §6).
- `tcpreplay` must always run with checksum recalculation enabled (spec §3.1) — checksum offload on veth pairs produces bad checksums that don't exist on real wire. **Verified against a real build:** the real flag is `-C`/`--fixcsum`, on the `tcpreplay-edit` binary specifically — Debian's `apt-get install tcpreplay` ships a build with "Packet editing: disabled" (no checksum support at all); the plugin/binary must be compiled from source to get `tcpreplay-edit`. (`--fix-checksums` never existed as a real flag; that was a planning error, not a real tcpreplay option.)
- Zeek must use `SeisoLLC/zeek-kafka` (actively maintained fork), never the archived Apache Metron plugin (spec §3.2). Pin `librdkafka` and `zeek-kafka` versions in the Dockerfile; do not float on `master`.
- Kafka runs single-broker KRaft mode — no ZooKeeper, no Confluent REST Proxy (spec §3.3).
- `Kafka::tag_json = T` wraps each record as `{"conn": {...}}`; only `Conn::LOG` is sent (`Kafka::logs_to_send = set(Conn::LOG)`) — restrict to what the model needs (spec §3.2).
- **No CICFlowMeter.** This pipeline scores flows using Zeek-derived features, not the 78-feature CICFlowMeter schema the reference `DL4IDS` model was trained on. The real feature contract for the production model is owned by a separate team/repo and does not exist here yet.
- The real `FeatureExtractor` implementation is out of scope for this repo. Ship the port (interface) plus a clearly-labeled placeholder adapter (`StubFeatureExtractor`) so the pipeline is runnable end to end; do not present the stub's feature set as production-accurate (spec §5 — "surface this and stop rather than approximating").
- JSON parsing must not assume all keys are present: a missing key (JSON omits unset fields) and an explicit unset marker (TSV's `-`) must map to the same internal representation (spec §4).
- Every port is a `typing.Protocol`. Adapters are selected by config (`type:` field per port), never hardcoded by import inside `application/pipeline.py` or `bootstrap.py` — this is what lets Kafka/parsing/inference be swapped later.
- Use `uv` for Python dependency management (matches existing sibling project conventions).
- **Two containers, not four**, chosen deliberately over the original 4-service split (spec §2's diagram) to match a real single-sensor deployment and to avoid an unnecessary Kafka broker dependency on packet-capture-path reliability:
  - `sensor` = tcpreplay + Zeek sharing one interface (`eth0`) in the same container/netns. Linux's `AF_PACKET` capture sees both incoming *and* locally-generated outgoing traffic on an interface by default, so Zeek captures tcpreplay's replayed frames with no bridge/veth hand-off needed between them, and no dependency on the pcap's IPs existing anywhere in the Docker network (capture is passive L2, not IP-routed). tcpreplay has no production analog — a real sensor's interface is fed by a physical/virtual SPAN/mirror port instead, and `zeek -i eth0 local` doesn't change either way.
  - `backend` = Kafka (KRaft) + the Python inference consumer bundled in one container, purely for local dev container-count convenience. This is *not* how it should be split in a real deployment — Kafka and inference have independent scaling/resource needs and Kafka usually fans in from multiple sensors — but neither process is capture-latency-sensitive, so co-locating them (unlike co-locating Kafka with the sensor) doesn't risk dropped packets.
  - Known rough edge: bundling two long-running processes (Kafka, a JVM process; the Python consumer) in one container means container-level graceful shutdown of Kafka is best-effort (see Task 14's `docker/backend/entrypoint.sh`), and restarting the inference process also restarts Kafka. Acceptable for local functional testing; revisit before any real deployment.

---

## File Structure

```
inference-ids/
  pyproject.toml
  Makefile
  docker-compose.yml
  README.md
  config/
    default.yaml
  pcaps/                          # gitignored, mount point for tcpreplay input
  models/                         # gitignored, mount point for model weights
  docker/
    sensor/
      Dockerfile
      entrypoint.sh
      local.zeek
    backend/
      Dockerfile
      entrypoint.sh
      server.properties
  scripts/
    replay.sh
  src/
    inference_ids/
      __init__.py
      __main__.py
      config.py
      factories.py
      bootstrap.py
      reference_model.py
      domain/
        __init__.py
        models.py
        ports.py
      application/
        __init__.py
        pipeline.py
      adapters/
        __init__.py
        _zeek_coercion.py
        tsv_parser.py
        json_parser.py
        feature_extractor_stub.py
        pytorch_inference.py
        log_sink.py
        kafka_source.py
  tests/
    __init__.py
    unit/
      __init__.py
      test_coercion.py
      test_tsv_parser.py
      test_json_parser.py
      test_feature_extractor_stub.py
      test_pytorch_inference.py
      test_log_sink.py
      test_pipeline.py
      test_config.py
      test_kafka_source.py
    fixtures/
      conn.log
      conn.json
  docs/
    VERIFICATION.md
```

---

### Task 1: Project scaffolding

**Files:**
- Create: `pyproject.toml`
- Create: `src/inference_ids/__init__.py`
- Create: `src/inference_ids/domain/__init__.py`
- Create: `src/inference_ids/application/__init__.py`
- Create: `src/inference_ids/adapters/__init__.py`
- Create: `tests/__init__.py`
- Create: `tests/unit/__init__.py`
- Create: `.gitignore`

**Interfaces:**
- Produces: an importable `inference_ids` package under `src/`, and a working `uv run pytest` command later tasks rely on.

- [ ] **Step 1: Write `pyproject.toml`**

```toml
[project]
name = "inference-ids"
version = "0.1.0"
description = "Live network traffic classification pipeline (Zeek -> Kafka -> PyTorch)"
requires-python = ">=3.12"
dependencies = [
    "confluent-kafka>=2.3.0",
    "numpy>=1.26",
    "pyyaml>=6.0",
    "torch>=2.2",
]

[dependency-groups]
dev = [
    "pytest>=8.0",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/inference_ids"]

[tool.pytest.ini_options]
pythonpath = ["src"]
testpaths = ["tests"]
```

- [ ] **Step 2: Create empty package `__init__.py` files**

```python
# src/inference_ids/__init__.py
# src/inference_ids/domain/__init__.py
# src/inference_ids/application/__init__.py
# src/inference_ids/adapters/__init__.py
# tests/__init__.py
# tests/unit/__init__.py
```

All six files are empty (0 bytes).

- [ ] **Step 3: Write `.gitignore`**

```
__pycache__/
*.pyc
.venv/
.pytest_cache/
uv.lock
pcaps/*
!pcaps/.gitkeep
models/*
!models/.gitkeep
```

- [ ] **Step 4: Create gitkeep placeholders**

```bash
mkdir -p pcaps models
touch pcaps/.gitkeep models/.gitkeep
```

- [ ] **Step 5: Install and verify**

Run: `uv sync`
Expected: creates `.venv/` and `uv.lock`, no errors.

Run: `uv run pytest`
Expected: `no tests ran` (exit code 5) — confirms pytest picks up the package via `pythonpath = ["src"]`.

- [ ] **Step 6: Commit**

```bash
git init
git add pyproject.toml src tests .gitignore pcaps/.gitkeep models/.gitkeep
git commit -m "chore: scaffold inference-ids package"
```

---

### Task 2: Domain models and ports

**Files:**
- Create: `src/inference_ids/domain/models.py`
- Create: `src/inference_ids/domain/ports.py`

**Interfaces:**
- Produces: `FlowRecord` (frozen dataclass, 20 fields — see below), `Prediction` (`label: str`, `confidence: float`, `logits: list[float]`), and five `Protocol` classes: `FlowSource.poll(timeout_seconds: float) -> dict | None` / `.close() -> None`; `FlowParser.parse(raw: dict) -> FlowRecord`; `FeatureExtractor.extract(records: list[FlowRecord]) -> np.ndarray`; `InferenceEngine.predict(features: np.ndarray) -> list[Prediction]`; `ResultSink.emit(record: FlowRecord, prediction: Prediction) -> None`. All later tasks import from here.

- [ ] **Step 1: Write `domain/models.py`**

```python
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class FlowRecord:
    """Canonical representation of one Zeek conn.log entry, independent of TSV/JSON source format."""

    uid: str
    ts: float
    duration: float
    orig_h: str
    orig_p: int
    resp_h: str
    resp_p: int
    proto: str
    service: str
    conn_state: str
    history: str
    missed_bytes: int
    orig_pkts: int
    orig_ip_bytes: int
    resp_pkts: int
    resp_ip_bytes: int
    orig_bytes: int
    resp_bytes: int
    local_orig: bool
    local_resp: bool


@dataclass(frozen=True)
class Prediction:
    label: str
    confidence: float
    logits: list[float] = field(default_factory=list)
```

- [ ] **Step 2: Write `domain/ports.py`**

```python
from __future__ import annotations

from typing import Protocol, runtime_checkable

import numpy as np

from inference_ids.domain.models import FlowRecord, Prediction


@runtime_checkable
class FlowSource(Protocol):
    def poll(self, timeout_seconds: float) -> dict | None:
        """Return the next raw flow message as a dict, or None if none arrived within timeout_seconds."""
        ...

    def close(self) -> None:
        ...


@runtime_checkable
class FlowParser(Protocol):
    def parse(self, raw: dict) -> FlowRecord:
        ...


@runtime_checkable
class FeatureExtractor(Protocol):
    def extract(self, records: list[FlowRecord]) -> np.ndarray:
        """Return a 2D float32 array of shape (len(records), n_features)."""
        ...


@runtime_checkable
class InferenceEngine(Protocol):
    def predict(self, features: np.ndarray) -> list[Prediction]:
        ...


@runtime_checkable
class ResultSink(Protocol):
    def emit(self, record: FlowRecord, prediction: Prediction) -> None:
        ...
```

- [ ] **Step 3: Verify it imports cleanly**

Run: `uv run python -c "from inference_ids.domain.ports import FlowSource, FlowParser, FeatureExtractor, InferenceEngine, ResultSink; from inference_ids.domain.models import FlowRecord, Prediction; print('ok')"`
Expected: `ok`

- [ ] **Step 4: Commit**

```bash
git add src/inference_ids/domain
git commit -m "feat: add domain models and hexagonal ports"
```

---

### Task 3: Shared Zeek value coercion helpers

**Files:**
- Create: `src/inference_ids/adapters/_zeek_coercion.py`
- Test: `tests/unit/test_coercion.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `coerce_float(value: object) -> float`, `coerce_int(value: object) -> int`, `coerce_bool(value: object) -> bool`, `coerce_str(value: object) -> str`. TSV and JSON parser adapters (Task 4, Task 5) both call these with `dict.get(field)` (which returns `None` for a missing key) so a missing JSON key and an explicit TSV `-` normalize to the same value.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_coercion.py
from inference_ids.adapters._zeek_coercion import coerce_bool, coerce_float, coerce_int, coerce_str


def test_coerce_float_handles_missing_and_unset():
    assert coerce_float(None) == 0.0
    assert coerce_float("-") == 0.0
    assert coerce_float("") == 0.0
    assert coerce_float("1.245") == 1.245
    assert coerce_float(1.245) == 1.245


def test_coerce_int_handles_missing_and_unset():
    assert coerce_int(None) == 0
    assert coerce_int("-") == 0
    assert coerce_int("350") == 350
    assert coerce_int(350) == 350


def test_coerce_bool_handles_zeek_tf_and_json_native():
    assert coerce_bool(None) is False
    assert coerce_bool("T") is True
    assert coerce_bool("F") is False
    assert coerce_bool(True) is True
    assert coerce_bool(False) is False


def test_coerce_str_handles_missing_and_unset():
    assert coerce_str(None) == ""
    assert coerce_str("-") == ""
    assert coerce_str("(empty)") == ""
    assert coerce_str("http") == "http"


def test_missing_key_and_unset_marker_are_identical():
    """The known JSON/TSV divergence from spec section 4: a missing key must
    map to the same internal value as an explicit unset marker."""
    missing = {}
    unset = {"service": "-"}
    assert coerce_str(missing.get("service")) == coerce_str(unset.get("service"))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_coercion.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'inference_ids.adapters._zeek_coercion'`

- [ ] **Step 3: Write `adapters/_zeek_coercion.py`**

```python
from __future__ import annotations

_UNSET_STRINGS = {"", "-", "(empty)"}


def coerce_float(value: object) -> float:
    if value is None:
        return 0.0
    if isinstance(value, str) and value in _UNSET_STRINGS:
        return 0.0
    return float(value)


def coerce_int(value: object) -> int:
    if value is None:
        return 0
    if isinstance(value, str) and value in _UNSET_STRINGS:
        return 0
    return int(float(value))


def coerce_bool(value: object) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return value == "T"
    return bool(value)


def coerce_str(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, str) and value in _UNSET_STRINGS:
        return ""
    return str(value)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_coercion.py -v`
Expected: 5 passed

- [ ] **Step 5: Commit**

```bash
git add src/inference_ids/adapters/_zeek_coercion.py tests/unit/test_coercion.py
git commit -m "feat: add shared Zeek value coercion helpers"
```

---

### Task 4: TSV (ASCII conn.log) parser adapter

**Files:**
- Create: `src/inference_ids/adapters/tsv_parser.py`
- Create: `tests/fixtures/conn.log`
- Test: `tests/unit/test_tsv_parser.py`

**Interfaces:**
- Consumes: `coerce_float`, `coerce_int`, `coerce_bool`, `coerce_str` from `inference_ids.adapters._zeek_coercion` (Task 3); `FlowRecord` from `inference_ids.domain.models` (Task 2).
- Produces: `TSVFlowParser` implementing `FlowParser.parse(raw: dict[str, str]) -> FlowRecord`; `iter_ascii_log_rows(path: Path) -> list[dict[str, str]]` — reads a Zeek ASCII log file's `#fields` header and rows into dicts. Task 6 (equivalence test) uses `iter_ascii_log_rows` + `TSVFlowParser` together.

- [ ] **Step 1: Write fixture `tests/fixtures/conn.log`**

```
#separator \x09
#set_separator	,
#empty_field	(empty)
#unset_field	-
#path	conn
#open	2026-07-18-00-00-00
#fields	ts	uid	id.orig_h	id.orig_p	id.resp_h	id.resp_p	proto	service	duration	orig_bytes	resp_bytes	conn_state	local_orig	local_resp	missed_bytes	history	orig_pkts	orig_ip_bytes	resp_pkts	resp_ip_bytes
#types	time	string	addr	port	addr	port	enum	string	interval	count	count	string	bool	bool	count	string	count	count	count	count
1713904200.123456	CXk4tK1AbAA1B2C3D4	192.168.1.10	51514	93.184.216.34	80	tcp	http	1.245	350	1200	SF	T	F	0	ShADadfF	6	740	8	1620
1713904300.654321	CYk4tK1AbAA1B2C3D5	10.0.0.5	40000	10.0.0.6	53	udp	dns	0.002	40	80	SF	F	F	0	Dd	1	68	1	108
#close	2026-07-18-00-05-00
```

- [ ] **Step 2: Write the failing test**

```python
# tests/unit/test_tsv_parser.py
from pathlib import Path

from inference_ids.adapters.tsv_parser import TSVFlowParser, iter_ascii_log_rows

FIXTURE = Path(__file__).parent.parent / "fixtures" / "conn.log"


def test_iter_ascii_log_rows_reads_fields_header():
    rows = iter_ascii_log_rows(FIXTURE)
    assert len(rows) == 2
    assert rows[0]["proto"] == "tcp"
    assert rows[1]["proto"] == "udp"


def test_tsv_parser_maps_row_to_flow_record():
    rows = iter_ascii_log_rows(FIXTURE)
    record = TSVFlowParser().parse(rows[0])

    assert record.uid == "CXk4tK1AbAA1B2C3D4"
    assert record.orig_h == "192.168.1.10"
    assert record.orig_p == 51514
    assert record.resp_h == "93.184.216.34"
    assert record.resp_p == 80
    assert record.proto == "tcp"
    assert record.service == "http"
    assert record.duration == 1.245
    assert record.orig_bytes == 350
    assert record.resp_bytes == 1200
    assert record.conn_state == "SF"
    assert record.local_orig is True
    assert record.local_resp is False
    assert record.missed_bytes == 0
    assert record.history == "ShADadfF"
    assert record.orig_pkts == 6
    assert record.orig_ip_bytes == 740
    assert record.resp_pkts == 8
    assert record.resp_ip_bytes == 1620


def test_tsv_parser_maps_unset_marker_to_empty_string():
    row = {"service": "-", "proto": "tcp"}
    record = TSVFlowParser().parse(row)
    assert record.service == ""
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_tsv_parser.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'inference_ids.adapters.tsv_parser'`

- [ ] **Step 4: Write `adapters/tsv_parser.py`**

```python
from __future__ import annotations

from pathlib import Path

from inference_ids.adapters._zeek_coercion import coerce_bool, coerce_float, coerce_int, coerce_str
from inference_ids.domain.models import FlowRecord


def _decode_separator(raw_value: str) -> str:
    if raw_value.startswith("\\x") and len(raw_value) == 4:
        return bytes.fromhex(raw_value[2:]).decode("utf-8")
    return raw_value


def iter_ascii_log_rows(path: Path) -> list[dict[str, str]]:
    """Read a Zeek ASCII log's #fields header and body rows into a list of field-name -> raw-string dicts."""
    separator = "\t"
    fields: list[str] = []
    rows: list[dict[str, str]] = []

    with Path(path).open("r", encoding="utf-8", newline="") as handle:
        for raw_line in handle:
            line = raw_line.rstrip("\n")
            if not line:
                continue

            if line.startswith("#separator"):
                _, raw_separator = line.split(" ", 1)
                separator = _decode_separator(raw_separator.strip())
                continue

            if line.startswith("#fields"):
                fields = line.split(separator)[1:]
                continue

            if line.startswith("#"):
                continue

            if not fields:
                raise ValueError(f"Could not parse {path}. Missing Zeek #fields header.")

            values = line.split(separator)
            if len(values) != len(fields):
                raise ValueError(f"Malformed row in {path}: expected {len(fields)} fields, got {len(values)}.")
            rows.append(dict(zip(fields, values, strict=True)))

    return rows


class TSVFlowParser:
    """Parses a single Zeek ASCII conn.log row (field-name -> raw-string dict) into a FlowRecord.

    Used for the Kafka JSON / TSV equivalence test and for offline batch processing.
    """

    def parse(self, raw: dict) -> FlowRecord:
        return FlowRecord(
            uid=coerce_str(raw.get("uid")),
            ts=coerce_float(raw.get("ts")),
            duration=coerce_float(raw.get("duration")),
            orig_h=coerce_str(raw.get("id.orig_h")),
            orig_p=coerce_int(raw.get("id.orig_p")),
            resp_h=coerce_str(raw.get("id.resp_h")),
            resp_p=coerce_int(raw.get("id.resp_p")),
            proto=coerce_str(raw.get("proto")),
            service=coerce_str(raw.get("service")),
            conn_state=coerce_str(raw.get("conn_state")),
            history=coerce_str(raw.get("history")),
            missed_bytes=coerce_int(raw.get("missed_bytes")),
            orig_pkts=coerce_int(raw.get("orig_pkts")),
            orig_ip_bytes=coerce_int(raw.get("orig_ip_bytes")),
            resp_pkts=coerce_int(raw.get("resp_pkts")),
            resp_ip_bytes=coerce_int(raw.get("resp_ip_bytes")),
            orig_bytes=coerce_int(raw.get("orig_bytes")),
            resp_bytes=coerce_int(raw.get("resp_bytes")),
            local_orig=coerce_bool(raw.get("local_orig")),
            local_resp=coerce_bool(raw.get("local_resp")),
        )
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_tsv_parser.py -v`
Expected: 3 passed

- [ ] **Step 6: Commit**

```bash
git add src/inference_ids/adapters/tsv_parser.py tests/fixtures/conn.log tests/unit/test_tsv_parser.py
git commit -m "feat: add TSV conn.log parser adapter"
```

---

### Task 5: JSON (zeek-kafka) parser adapter

**Files:**
- Create: `src/inference_ids/adapters/json_parser.py`
- Create: `tests/fixtures/conn.json`
- Test: `tests/unit/test_json_parser.py`

**Interfaces:**
- Consumes: coercion helpers (Task 3); `FlowRecord` (Task 2).
- Produces: `JSONFlowParser` implementing `FlowParser.parse(raw: dict) -> FlowRecord`. `raw` is the already-JSON-decoded Kafka message value, still wrapped as `{"conn": {...}}` (the `tag_json=T` shape) — this adapter is where the unwrap happens, per spec §3.2's note that "the consumer must unwrap this key." `KafkaFlowSource` (Task 11) stays transport-only and does not know about the `conn` wrapper.

- [ ] **Step 1: Write fixture `tests/fixtures/conn.json`**

One line per record, matching the same two flows as `tests/fixtures/conn.log`, wrapped in `{"conn": {...}}` and with the second record's `service` key *omitted* (simulating the JSON writer dropping unset fields — spec §4's known divergence):

```json
{"conn":{"ts":1713904200.123456,"uid":"CXk4tK1AbAA1B2C3D4","id.orig_h":"192.168.1.10","id.orig_p":51514,"id.resp_h":"93.184.216.34","id.resp_p":80,"proto":"tcp","service":"http","duration":1.245,"orig_bytes":350,"resp_bytes":1200,"conn_state":"SF","local_orig":true,"local_resp":false,"missed_bytes":0,"history":"ShADadfF","orig_pkts":6,"orig_ip_bytes":740,"resp_pkts":8,"resp_ip_bytes":1620}}
{"conn":{"ts":1713904300.654321,"uid":"CYk4tK1AbAA1B2C3D5","id.orig_h":"10.0.0.5","id.orig_p":40000,"id.resp_h":"10.0.0.6","id.resp_p":53,"proto":"udp","duration":0.002,"orig_bytes":40,"resp_bytes":80,"conn_state":"SF","local_orig":false,"local_resp":false,"missed_bytes":0,"history":"Dd","orig_pkts":1,"orig_ip_bytes":68,"resp_pkts":1,"resp_ip_bytes":108}}
```

- [ ] **Step 2: Write the failing test**

```python
# tests/unit/test_json_parser.py
import json
from pathlib import Path

from inference_ids.adapters.json_parser import JSONFlowParser

FIXTURE = Path(__file__).parent.parent / "fixtures" / "conn.json"


def _load_records():
    with FIXTURE.open() as handle:
        return [json.loads(line) for line in handle if line.strip()]


def test_json_parser_unwraps_conn_key_and_maps_fields():
    raw_messages = _load_records()
    record = JSONFlowParser().parse(raw_messages[0])

    assert record.uid == "CXk4tK1AbAA1B2C3D4"
    assert record.orig_h == "192.168.1.10"
    assert record.orig_p == 51514
    assert record.proto == "tcp"
    assert record.service == "http"
    assert record.duration == 1.245
    assert record.orig_bytes == 350
    assert record.resp_bytes == 1200
    assert record.local_orig is True
    assert record.local_resp is False
    assert record.history == "ShADadfF"


def test_json_parser_missing_key_maps_to_same_value_as_tsv_unset_marker():
    """spec section 4: the JSON writer omits keys for unset fields entirely. A
    missing 'service' key must parse to the same empty string as TSV's '-'."""
    raw_messages = _load_records()
    record = JSONFlowParser().parse(raw_messages[1])  # 'service' key is absent
    assert record.service == ""


def test_json_parser_handles_unwrapped_dict_too():
    """Defensive: if logs_to_send / tag_json config changes upstream, don't hard-fail on an
    already-flat dict."""
    flat = {"uid": "C1", "proto": "tcp"}
    record = JSONFlowParser().parse(flat)
    assert record.uid == "C1"
    assert record.proto == "tcp"
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_json_parser.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'inference_ids.adapters.json_parser'`

- [ ] **Step 4: Write `adapters/json_parser.py`**

```python
from __future__ import annotations

from inference_ids.adapters._zeek_coercion import coerce_bool, coerce_float, coerce_int, coerce_str
from inference_ids.domain.models import FlowRecord


class JSONFlowParser:
    """Parses a zeek-kafka JSON message into a FlowRecord.

    `Kafka::tag_json = T` wraps each record as {"conn": {...}}; this adapter unwraps
    that key so the Kafka transport adapter can stay agnostic of the log shape.
    """

    def parse(self, raw: dict) -> FlowRecord:
        conn = raw.get("conn", raw)
        return FlowRecord(
            uid=coerce_str(conn.get("uid")),
            ts=coerce_float(conn.get("ts")),
            duration=coerce_float(conn.get("duration")),
            orig_h=coerce_str(conn.get("id.orig_h")),
            orig_p=coerce_int(conn.get("id.orig_p")),
            resp_h=coerce_str(conn.get("id.resp_h")),
            resp_p=coerce_int(conn.get("id.resp_p")),
            proto=coerce_str(conn.get("proto")),
            service=coerce_str(conn.get("service")),
            conn_state=coerce_str(conn.get("conn_state")),
            history=coerce_str(conn.get("history")),
            missed_bytes=coerce_int(conn.get("missed_bytes")),
            orig_pkts=coerce_int(conn.get("orig_pkts")),
            orig_ip_bytes=coerce_int(conn.get("orig_ip_bytes")),
            resp_pkts=coerce_int(conn.get("resp_pkts")),
            resp_ip_bytes=coerce_int(conn.get("resp_ip_bytes")),
            orig_bytes=coerce_int(conn.get("orig_bytes")),
            resp_bytes=coerce_int(conn.get("resp_bytes")),
            local_orig=coerce_bool(conn.get("local_orig")),
            local_resp=coerce_bool(conn.get("local_resp")),
        )
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_json_parser.py -v`
Expected: 3 passed

- [ ] **Step 6: Commit**

```bash
git add src/inference_ids/adapters/json_parser.py tests/fixtures/conn.json tests/unit/test_json_parser.py
git commit -m "feat: add JSON (zeek-kafka) parser adapter"
```

---

### Task 6: Parser equivalence test (spec §4 acceptance test)

**Files:**
- Test: `tests/unit/test_parser_equivalence.py`

**Interfaces:**
- Consumes: `iter_ascii_log_rows` + `TSVFlowParser` (Task 4), `JSONFlowParser` (Task 5), the two fixtures from Tasks 4–5 (which encode the *same* two flows).

This is the fixture-level version of the spec's acceptance test ("run the same pcap through Zeek twice ... assert the resulting feature vectors are element-wise identical"). It proves the parser migration has no format-driven drift using fixed inputs. The full pcap-driven version (actually invoking Zeek twice) is a manual step in `docs/VERIFICATION.md` (Task 15) because it needs a running Zeek binary, not just this Python package.

- [ ] **Step 1: Write the test**

```python
# tests/unit/test_parser_equivalence.py
import json
from pathlib import Path

from inference_ids.adapters.json_parser import JSONFlowParser
from inference_ids.adapters.tsv_parser import TSVFlowParser, iter_ascii_log_rows

TSV_FIXTURE = Path(__file__).parent.parent / "fixtures" / "conn.log"
JSON_FIXTURE = Path(__file__).parent.parent / "fixtures" / "conn.json"


def test_tsv_and_json_parsers_produce_identical_flow_records():
    tsv_rows = iter_ascii_log_rows(TSV_FIXTURE)
    tsv_records = [TSVFlowParser().parse(row) for row in tsv_rows]

    with JSON_FIXTURE.open() as handle:
        json_records = [JSONFlowParser().parse(json.loads(line)) for line in handle if line.strip()]

    assert len(tsv_records) == len(json_records)
    for tsv_record, json_record in zip(tsv_records, json_records):
        assert tsv_record == json_record
```

- [ ] **Step 2: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_parser_equivalence.py -v`
Expected: 1 passed. If it fails, the two fixtures have drifted apart (they must encode literally the same flows) or a coercion mismatch exists between the two adapters — do not proceed to Task 7 until this is green.

- [ ] **Step 3: Commit**

```bash
git add tests/unit/test_parser_equivalence.py
git commit -m "test: add TSV/JSON parser equivalence test (spec section 4)"
```

---

### Task 7: Placeholder feature extractor adapter

**Files:**
- Create: `src/inference_ids/adapters/feature_extractor_stub.py`
- Test: `tests/unit/test_feature_extractor_stub.py`

**Interfaces:**
- Consumes: `FlowRecord` (Task 2).
- Produces: `StubFeatureExtractor` implementing `FeatureExtractor.extract(records: list[FlowRecord]) -> np.ndarray` (shape `(len(records), 11)`, dtype `float32`), and `StubFeatureExtractor.feature_count == 11`. Task 8's reference model and Task 12's default config both use `feature_count`/`11` as `input_features`.

This is the fake adapter agreed with the user: the real 78-feature-vs-Zeek-feature question (spec §5) is owned by a separate team's model-training pipeline. This stub is explicitly not that — it exists only so the rest of the pipeline is exercisable end to end.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_feature_extractor_stub.py
import numpy as np

from inference_ids.adapters.feature_extractor_stub import StubFeatureExtractor
from inference_ids.domain.models import FlowRecord


def _record(**overrides) -> FlowRecord:
    base = dict(
        uid="C1", ts=0.0, duration=1.5, orig_h="10.0.0.1", orig_p=1,
        resp_h="10.0.0.2", resp_p=2, proto="tcp", service="http",
        conn_state="SF", history="ShADadfF", missed_bytes=0,
        orig_pkts=6, orig_ip_bytes=740, resp_pkts=8, resp_ip_bytes=1620,
        orig_bytes=350, resp_bytes=1200, local_orig=True, local_resp=False,
    )
    base.update(overrides)
    return FlowRecord(**base)


def test_extract_returns_correct_shape_and_dtype():
    extractor = StubFeatureExtractor()
    features = extractor.extract([_record(), _record()])

    assert features.shape == (2, extractor.feature_count)
    assert features.dtype == np.float32


def test_extract_empty_list_returns_empty_array():
    extractor = StubFeatureExtractor()
    features = extractor.extract([])
    assert features.shape == (0, extractor.feature_count)


def test_byte_ratio_avoids_division_by_zero():
    extractor = StubFeatureExtractor()
    record = _record(orig_bytes=100, resp_bytes=0)
    features = extractor.extract([record])
    assert np.isfinite(features).all()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_feature_extractor_stub.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Write `adapters/feature_extractor_stub.py`**

```python
from __future__ import annotations

import numpy as np

from inference_ids.domain.models import FlowRecord

_PROTO_INDEX = {"tcp": 0, "udp": 1, "icmp": 2}


class StubFeatureExtractor:
    """
    PLACEHOLDER feature extractor.

    The real feature contract (exact feature list, order, units) for the production
    model is owned by a separate model-training pipeline and does not live in this
    repo. This stub derives a small feature vector directly from Zeek conn.log fields
    so Kafka -> parse -> batch -> inference -> sink is runnable and testable end to end.

    Do not treat this as a production feature set. Replace with an adapter matching
    the trained model's real input contract before scoring real traffic.
    """

    feature_count = 11

    def extract(self, records: list[FlowRecord]) -> np.ndarray:
        if not records:
            return np.empty((0, self.feature_count), dtype=np.float32)
        return np.array([self._extract_one(record) for record in records], dtype=np.float32)

    @staticmethod
    def _extract_one(record: FlowRecord) -> list[float]:
        total_bytes = record.orig_bytes + record.resp_bytes
        total_pkts = record.orig_pkts + record.resp_pkts
        byte_ratio = record.orig_bytes / record.resp_bytes if record.resp_bytes else float(record.orig_bytes)
        return [
            record.duration,
            float(_PROTO_INDEX.get(record.proto, -1)),
            float(record.orig_bytes),
            float(record.resp_bytes),
            float(record.orig_pkts),
            float(record.resp_pkts),
            float(total_bytes),
            float(total_pkts),
            byte_ratio,
            float(len(record.history)),
            float(record.missed_bytes),
        ]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_feature_extractor_stub.py -v`
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add src/inference_ids/adapters/feature_extractor_stub.py tests/unit/test_feature_extractor_stub.py
git commit -m "feat: add placeholder Zeek-field feature extractor adapter"
```

---

### Task 8: Reference model + PyTorch inference engine adapter

**Files:**
- Create: `src/inference_ids/reference_model.py`
- Create: `src/inference_ids/adapters/pytorch_inference.py`
- Test: `tests/unit/test_pytorch_inference.py`

**Interfaces:**
- Consumes: `Prediction` (Task 2).
- Produces: `IDSModel(input_features: int, num_classes: int)` (a small placeholder `nn.Module`, architecture mirrors the sibling `DL4IDS` project's model so it's a realistic stand-in); `PyTorchInferenceEngine(model_module, model_class, state_dict_path, init_kwargs, class_names, device="cpu", precision="fp32")` implementing `InferenceEngine.predict(features: np.ndarray) -> list[Prediction]`. Task 12's config/factories reference `model_module="inference_ids.reference_model"`, `model_class="IDSModel"` as the default.

`IDSModel` here is a placeholder architecture for exercising the adapter and the pipeline, not the production model — the real model class and trained weights are supplied later (by the team that owns training) via config, without touching this adapter's code.

- [ ] **Step 1: Write `reference_model.py`**

```python
from __future__ import annotations

import torch.nn as nn


class IDSModel(nn.Module):
    """Placeholder classifier used to exercise the inference pipeline before a real
    trained model + weights are supplied via config. Not fit on real traffic."""

    def __init__(self, input_features: int, num_classes: int) -> None:
        super().__init__()
        self.fc1 = nn.Linear(input_features, 256)
        self.batch_norm1 = nn.BatchNorm1d(256)
        self.activation1 = nn.GELU()
        self.dropout1 = nn.Dropout(0.2)
        self.fc2 = nn.Linear(256, 128)
        self.batch_norm2 = nn.BatchNorm1d(128)
        self.activation2 = nn.GELU()
        self.dropout2 = nn.Dropout(0.2)
        self.fc3 = nn.Linear(128, num_classes)

    def forward(self, x):
        x = self.fc1(x)
        x = self.batch_norm1(x)
        x = self.activation1(x)
        x = self.dropout1(x)
        x = self.fc2(x)
        x = self.batch_norm2(x)
        x = self.activation2(x)
        x = self.dropout2(x)
        return self.fc3(x)
```

- [ ] **Step 2: Write the failing test**

```python
# tests/unit/test_pytorch_inference.py
import numpy as np
import torch

from inference_ids.adapters.pytorch_inference import PyTorchInferenceEngine
from inference_ids.reference_model import IDSModel


def test_predict_returns_one_prediction_per_row(tmp_path):
    model = IDSModel(input_features=4, num_classes=3)
    model.eval()
    state_dict_path = tmp_path / "model.pth"
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

    assert len(predictions) == 5
    for prediction in predictions:
        assert prediction.label in {"benign", "scan", "dos"}
        assert 0.0 <= prediction.confidence <= 1.0
        assert len(prediction.logits) == 3


def test_predict_confidence_matches_argmax_probability(tmp_path):
    model = IDSModel(input_features=2, num_classes=2)
    model.eval()
    state_dict_path = tmp_path / "model.pth"
    torch.save(model.state_dict(), state_dict_path)

    engine = PyTorchInferenceEngine(
        model_module="inference_ids.reference_model",
        model_class="IDSModel",
        state_dict_path=str(state_dict_path),
        init_kwargs={"input_features": 2, "num_classes": 2},
        class_names=["a", "b"],
        device="cpu",
        precision="fp32",
    )

    features = np.array([[0.1, 0.2]], dtype=np.float32)
    [prediction] = engine.predict(features)

    probabilities = torch.softmax(torch.tensor(prediction.logits), dim=0)
    expected_confidence = probabilities.max().item()
    assert prediction.confidence == expected_confidence
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_pytorch_inference.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'inference_ids.adapters.pytorch_inference'`

- [ ] **Step 4: Write `adapters/pytorch_inference.py`**

```python
from __future__ import annotations

import importlib

import numpy as np
import torch

from inference_ids.domain.models import Prediction


class PyTorchInferenceEngine:
    def __init__(
        self,
        model_module: str,
        model_class: str,
        state_dict_path: str,
        init_kwargs: dict,
        class_names: list[str],
        device: str = "cpu",
        precision: str = "fp32",
    ) -> None:
        module = importlib.import_module(model_module)
        model_cls = getattr(module, model_class)

        self._device = torch.device(device)
        self._dtype = torch.float16 if precision == "fp16" else torch.float32
        self._class_names = class_names

        model = model_cls(**init_kwargs)
        state_dict = torch.load(state_dict_path, map_location=self._device)
        model.load_state_dict(state_dict)
        model.to(self._device, dtype=self._dtype)
        model.eval()
        self._model = model

    @torch.inference_mode()
    def predict(self, features: np.ndarray) -> list[Prediction]:
        tensor = torch.from_numpy(features).to(self._device, dtype=self._dtype)
        logits = self._model(tensor)
        probabilities = torch.softmax(logits, dim=1)
        confidences, indices = torch.max(probabilities, dim=1)

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

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_pytorch_inference.py -v`
Expected: 2 passed

- [ ] **Step 6: Commit**

```bash
git add src/inference_ids/reference_model.py src/inference_ids/adapters/pytorch_inference.py tests/unit/test_pytorch_inference.py
git commit -m "feat: add reference model and PyTorch inference engine adapter"
```

---

### Task 9: Logging result sink adapter

**Files:**
- Create: `src/inference_ids/adapters/log_sink.py`
- Test: `tests/unit/test_log_sink.py`

**Interfaces:**
- Consumes: `FlowRecord`, `Prediction` (Task 2).
- Produces: `LoggingResultSink` implementing `ResultSink.emit(record, prediction) -> None`, logging at `INFO` via `logging.getLogger("inference_ids.results")`.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_log_sink.py
import logging

from inference_ids.adapters.log_sink import LoggingResultSink
from inference_ids.domain.models import FlowRecord, Prediction


def _record() -> FlowRecord:
    return FlowRecord(
        uid="C1", ts=0.0, duration=1.5, orig_h="10.0.0.1", orig_p=1111,
        resp_h="10.0.0.2", resp_p=80, proto="tcp", service="http",
        conn_state="SF", history="ShADadfF", missed_bytes=0,
        orig_pkts=6, orig_ip_bytes=740, resp_pkts=8, resp_ip_bytes=1620,
        orig_bytes=350, resp_bytes=1200, local_orig=True, local_resp=False,
    )


def test_emit_logs_flow_and_prediction(caplog):
    sink = LoggingResultSink()
    prediction = Prediction(label="benign", confidence=0.987, logits=[2.1, -0.3])

    with caplog.at_level(logging.INFO, logger="inference_ids.results"):
        sink.emit(_record(), prediction)

    assert len(caplog.records) == 1
    message = caplog.records[0].getMessage()
    assert "C1" in message
    assert "10.0.0.1" in message
    assert "10.0.0.2" in message
    assert "benign" in message
    assert "0.987" in message
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_log_sink.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Write `adapters/log_sink.py`**

```python
from __future__ import annotations

import logging

from inference_ids.domain.models import FlowRecord, Prediction

logger = logging.getLogger("inference_ids.results")


class LoggingResultSink:
    def emit(self, record: FlowRecord, prediction: Prediction) -> None:
        logger.info(
            "flow=%s %s:%s -> %s:%s proto=%s label=%s confidence=%.3f",
            record.uid,
            record.orig_h,
            record.orig_p,
            record.resp_h,
            record.resp_p,
            record.proto,
            prediction.label,
            prediction.confidence,
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_log_sink.py -v`
Expected: 1 passed

- [ ] **Step 5: Commit**

```bash
git add src/inference_ids/adapters/log_sink.py tests/unit/test_log_sink.py
git commit -m "feat: add logging result sink adapter"
```

---

### Task 10: Application pipeline (orchestration + batching)

**Files:**
- Create: `src/inference_ids/application/pipeline.py`
- Test: `tests/unit/test_pipeline.py`

**Interfaces:**
- Consumes: the five ports from `inference_ids.domain.ports` (Task 2); `FlowRecord` (Task 2).
- Produces: `InferencePipeline(source, parser, feature_extractor, inference_engine, sink, batch_window_ms=100, max_batch_size=256, poll_timeout_seconds=0.05, clock=time.monotonic)` with `run_one_window() -> int` (returns records processed, for testability) and `run_forever() -> None` (calls `run_one_window` in a loop; used by Task 13's CLI entrypoint). `clock` is injectable so tests don't depend on real wall-clock time.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_pipeline.py
from inference_ids.application.pipeline import InferencePipeline
from inference_ids.domain.models import FlowRecord, Prediction


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class FakeSource:
    def __init__(self, raw_messages, clock: FakeClock, seconds_per_poll: float = 0.01):
        self._queue = list(raw_messages)
        self._clock = clock
        self._seconds_per_poll = seconds_per_poll
        self.closed = False

    def poll(self, timeout_seconds: float):
        self._clock.advance(self._seconds_per_poll)
        if self._queue:
            return self._queue.pop(0)
        return None

    def close(self) -> None:
        self.closed = True


class FakeParser:
    def parse(self, raw: dict) -> FlowRecord:
        return FlowRecord(
            uid=raw["uid"], ts=0.0, duration=0.0, orig_h="a", orig_p=1, resp_h="b", resp_p=2,
            proto="tcp", service="", conn_state="SF", history="", missed_bytes=0,
            orig_pkts=0, orig_ip_bytes=0, resp_pkts=0, resp_ip_bytes=0,
            orig_bytes=0, resp_bytes=0, local_orig=False, local_resp=False,
        )


class FakeFeatureExtractor:
    def extract(self, records):
        return [[float(len(records))] for _ in records]


class FakeInferenceEngine:
    def predict(self, features):
        return [Prediction(label="benign", confidence=1.0, logits=[1.0]) for _ in features]


class FakeSink:
    def __init__(self):
        self.emitted = []

    def emit(self, record, prediction) -> None:
        self.emitted.append((record, prediction))


def _build_pipeline(raw_messages, max_batch_size=256, batch_window_ms=50):
    clock = FakeClock()
    source = FakeSource(raw_messages, clock)
    sink = FakeSink()
    pipeline = InferencePipeline(
        source=source,
        parser=FakeParser(),
        feature_extractor=FakeFeatureExtractor(),
        inference_engine=FakeInferenceEngine(),
        sink=sink,
        batch_window_ms=batch_window_ms,
        max_batch_size=max_batch_size,
        poll_timeout_seconds=0.01,
        clock=clock,
    )
    return pipeline, sink


def test_run_one_window_batches_all_messages_within_window():
    raw_messages = [{"uid": f"C{i}"} for i in range(3)]
    pipeline, sink = _build_pipeline(raw_messages, max_batch_size=256, batch_window_ms=50)

    processed = pipeline.run_one_window()

    assert processed == 3
    assert len(sink.emitted) == 3


def test_run_one_window_respects_max_batch_size():
    raw_messages = [{"uid": f"C{i}"} for i in range(10)]
    pipeline, sink = _build_pipeline(raw_messages, max_batch_size=1, batch_window_ms=1000)

    processed = pipeline.run_one_window()

    assert processed == 1
    assert len(sink.emitted) == 1


def test_run_one_window_with_no_messages_emits_nothing():
    pipeline, sink = _build_pipeline([], max_batch_size=256, batch_window_ms=10)

    processed = pipeline.run_one_window()

    assert processed == 0
    assert sink.emitted == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_pipeline.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'inference_ids.application.pipeline'`

- [ ] **Step 3: Write `application/pipeline.py`**

```python
from __future__ import annotations

import time
from typing import Callable

from inference_ids.domain.models import FlowRecord
from inference_ids.domain.ports import FeatureExtractor, FlowParser, FlowSource, InferenceEngine, ResultSink


class InferencePipeline:
    def __init__(
        self,
        source: FlowSource,
        parser: FlowParser,
        feature_extractor: FeatureExtractor,
        inference_engine: InferenceEngine,
        sink: ResultSink,
        batch_window_ms: int = 100,
        max_batch_size: int = 256,
        poll_timeout_seconds: float = 0.05,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._source = source
        self._parser = parser
        self._feature_extractor = feature_extractor
        self._inference_engine = inference_engine
        self._sink = sink
        self._batch_window_seconds = batch_window_ms / 1000
        self._max_batch_size = max_batch_size
        self._poll_timeout_seconds = poll_timeout_seconds
        self._clock = clock

    def run_forever(self) -> None:
        while True:
            self.run_one_window()

    def run_one_window(self) -> int:
        records: list[FlowRecord] = []
        window_deadline = self._clock() + self._batch_window_seconds

        while self._clock() < window_deadline and len(records) < self._max_batch_size:
            raw = self._source.poll(self._poll_timeout_seconds)
            if raw is not None:
                records.append(self._parser.parse(raw))

        if records:
            self._score_and_emit(records)
        return len(records)

    def _score_and_emit(self, records: list[FlowRecord]) -> None:
        features = self._feature_extractor.extract(records)
        predictions = self._inference_engine.predict(features)
        for record, prediction in zip(records, predictions):
            self._sink.emit(record, prediction)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_pipeline.py -v`
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add src/inference_ids/application/pipeline.py tests/unit/test_pipeline.py
git commit -m "feat: add InferencePipeline application service with windowed batching"
```

---

### Task 11: Kafka flow source adapter

**Files:**
- Create: `src/inference_ids/adapters/kafka_source.py`
- Test: `tests/unit/test_kafka_source.py`

**Interfaces:**
- Consumes: nothing from earlier tasks besides the `FlowSource` port shape (Task 2) it must satisfy structurally.
- Produces: `KafkaFlowSource(bootstrap_servers, topic, group_id, auto_offset_reset="latest")` implementing `FlowSource.poll(timeout_seconds) -> dict | None` / `.close() -> None`. Returns the JSON-decoded message value as-is (still `{"conn": {...}}`-wrapped) — unwrapping is `JSONFlowParser`'s job (Task 5), keeping this adapter transport-only.

This adapter is tested against a fake `confluent_kafka.Consumer` (monkeypatched) since a real broker isn't available in unit tests; the real broker path is exercised manually per `docs/VERIFICATION.md` step 2 (Task 15).

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_kafka_source.py
import json

import pytest

from inference_ids.adapters import kafka_source


class FakeMessage:
    def __init__(self, value: bytes, error=None):
        self._value = value
        self._error = error

    def value(self) -> bytes:
        return self._value

    def error(self):
        return self._error


class FakeConsumer:
    instances = []

    def __init__(self, conf: dict):
        self.conf = conf
        self.subscribed_topics = []
        self.closed = False
        self._messages = []
        FakeConsumer.instances.append(self)

    def subscribe(self, topics):
        self.subscribed_topics = topics

    def poll(self, timeout):
        if self._messages:
            return self._messages.pop(0)
        return None

    def close(self):
        self.closed = True


@pytest.fixture(autouse=True)
def patch_consumer(monkeypatch):
    FakeConsumer.instances.clear()
    monkeypatch.setattr(kafka_source, "Consumer", FakeConsumer)
    yield


def test_source_subscribes_to_configured_topic():
    kafka_source.KafkaFlowSource(
        bootstrap_servers="kafka:9092", topic="zeek-flows", group_id="inference-ids"
    )

    consumer = FakeConsumer.instances[0]
    assert consumer.subscribed_topics == ["zeek-flows"]
    assert consumer.conf["bootstrap.servers"] == "kafka:9092"
    assert consumer.conf["group.id"] == "inference-ids"
    assert consumer.conf["auto.offset.reset"] == "latest"


def test_poll_returns_none_when_no_message():
    source = kafka_source.KafkaFlowSource(
        bootstrap_servers="kafka:9092", topic="zeek-flows", group_id="inference-ids"
    )
    assert source.poll(0.01) is None


def test_poll_decodes_json_message_value():
    source = kafka_source.KafkaFlowSource(
        bootstrap_servers="kafka:9092", topic="zeek-flows", group_id="inference-ids"
    )
    consumer = FakeConsumer.instances[0]
    payload = json.dumps({"conn": {"uid": "C1"}}).encode("utf-8")
    consumer._messages.append(FakeMessage(value=payload))

    result = source.poll(0.01)
    assert result == {"conn": {"uid": "C1"}}


def test_poll_raises_on_kafka_error():
    source = kafka_source.KafkaFlowSource(
        bootstrap_servers="kafka:9092", topic="zeek-flows", group_id="inference-ids"
    )
    consumer = FakeConsumer.instances[0]
    consumer._messages.append(FakeMessage(value=b"", error="broker unavailable"))

    with pytest.raises(RuntimeError, match="broker unavailable"):
        source.poll(0.01)


def test_close_closes_underlying_consumer():
    source = kafka_source.KafkaFlowSource(
        bootstrap_servers="kafka:9092", topic="zeek-flows", group_id="inference-ids"
    )
    source.close()
    assert FakeConsumer.instances[0].closed is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_kafka_source.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'inference_ids.adapters.kafka_source'`

- [ ] **Step 3: Write `adapters/kafka_source.py`**

```python
from __future__ import annotations

import json

from confluent_kafka import Consumer


class KafkaFlowSource:
    """Transport-only adapter: returns the JSON-decoded message value unmodified.
    Unwrapping zeek-kafka's {"conn": {...}} shape is JSONFlowParser's responsibility.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        auto_offset_reset: str = "latest",
    ) -> None:
        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": auto_offset_reset,
            }
        )
        self._consumer.subscribe([topic])

    def poll(self, timeout_seconds: float) -> dict | None:
        message = self._consumer.poll(timeout_seconds)
        if message is None:
            return None
        if message.error():
            raise RuntimeError(f"Kafka error: {message.error()}")
        return json.loads(message.value())

    def close(self) -> None:
        self._consumer.close()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_kafka_source.py -v`
Expected: 5 passed

- [ ] **Step 5: Commit**

```bash
git add src/inference_ids/adapters/kafka_source.py tests/unit/test_kafka_source.py
git commit -m "feat: add Kafka flow source adapter"
```

---

### Task 12: Config loader and adapter factories

**Files:**
- Create: `src/inference_ids/config.py`
- Create: `src/inference_ids/factories.py`
- Create: `config/default.yaml`
- Test: `tests/unit/test_config.py`

**Interfaces:**
- Consumes: all adapter classes from Tasks 4, 5, 7, 8, 9, 11.
- Produces: `load_config(path: str | Path) -> AppConfig` (dataclass tree: `AppConfig.source`, `.parser`, `.feature_extractor`, `.inference_engine`, `.sink`, `.pipeline`, each with a `type: str` field selecting the adapter); `create_flow_source(config) -> FlowSource`, `create_flow_parser(config) -> FlowParser`, `create_feature_extractor(config) -> FeatureExtractor`, `create_inference_engine(config) -> InferenceEngine`, `create_result_sink(config) -> ResultSink` — one factory function per port, each dispatching on that port's `type` field. This `type`-keyed dispatch is the actual swap point: adding a new adapter means adding one `elif` branch here, not touching `application/pipeline.py` or `bootstrap.py` (Task 13).

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_config.py
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_config.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'inference_ids.config'`

- [ ] **Step 3: Write `config.py`**

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
```

- [ ] **Step 4: Write `factories.py`**

```python
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
```

- [ ] **Step 5: Write `config/default.yaml`**

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
  type: log

pipeline:
  batch_window_ms: 100
  max_batch_size: 256
  poll_timeout_seconds: 0.05
```

`bootstrap_servers` is `localhost:9092` because, per Task 14, the inference process runs in the *same* container as the Kafka broker it consumes from — no cross-container hop needed for this connection either. `inference_engine.pytorch.state_dict_path` points at a placeholder path; Task 14's `docker-compose.yml` mounts `./models` there. Before running the stack, save a checkpoint that matches this config's `init_kwargs`, e.g.:

```bash
uv run python -c "
import torch
from inference_ids.reference_model import IDSModel
model = IDSModel(input_features=11, num_classes=3)
torch.save(model.state_dict(), 'models/reference_ids_model.pth')
"
```

- [ ] **Step 6: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_config.py -v`
Expected: 6 passed

- [ ] **Step 7: Commit**

```bash
git add src/inference_ids/config.py src/inference_ids/factories.py config/default.yaml tests/unit/test_config.py
git commit -m "feat: add config loader and type-dispatched adapter factories"
```

---

### Task 13: Bootstrap wiring and CLI entrypoint

**Files:**
- Create: `src/inference_ids/bootstrap.py`
- Create: `src/inference_ids/__main__.py`

**Interfaces:**
- Consumes: `load_config` (Task 12), all `create_*` factories (Task 12), `InferencePipeline` (Task 10).
- Produces: `build_pipeline(config: AppConfig) -> InferencePipeline`; running `python -m inference_ids --config <path>` starts `pipeline.run_forever()`.

- [ ] **Step 1: Write `bootstrap.py`**

```python
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
```

- [ ] **Step 2: Write `__main__.py`**

```python
from __future__ import annotations

import argparse
import logging

from inference_ids.bootstrap import build_pipeline
from inference_ids.config import load_config


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the live NTC inference pipeline.")
    parser.add_argument("--config", default="config/default.yaml")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    config = load_config(args.config)
    pipeline = build_pipeline(config)
    pipeline.run_forever()


if __name__ == "__main__":
    main()
```

- [ ] **Step 3: Verify wiring end to end with a saved reference checkpoint**

```bash
uv run python -c "
import torch
from inference_ids.reference_model import IDSModel
model = IDSModel(input_features=11, num_classes=3)
torch.save(model.state_dict(), 'models/reference_ids_model.pth')
"
```

Run: `uv run python -c "from inference_ids.bootstrap import build_pipeline; from inference_ids.config import load_config; c = load_config('config/default.yaml'); print(type(build_pipeline(c)))"`
Expected: fails on `KafkaFlowSource` construction only if no broker is reachable at `kafka:9092` — that's fine at this stage (no Kafka running yet); confirm the failure is a connection/DNS issue from `confluent_kafka`, not an `ImportError`/`AttributeError`/config error. If you have a broker reachable at `localhost:9092`, temporarily point `bootstrap_servers` there to confirm a clean construction with no exceptions at all.

- [ ] **Step 4: Commit**

```bash
git add src/inference_ids/bootstrap.py src/inference_ids/__main__.py
git commit -m "feat: wire adapters via config-driven bootstrap and add CLI entrypoint"
```

---

### Task 14: Docker services and compose stack (sensor + backend)

**Files:**
- Create: `docker/sensor/Dockerfile`
- Create: `docker/sensor/entrypoint.sh`
- Create: `docker/sensor/local.zeek`
- Create: `docker/backend/Dockerfile`
- Create: `docker/backend/entrypoint.sh`
- Create: `docker/backend/server.properties`
- Create: `docker-compose.yml`
- Create: `scripts/replay.sh`
- Create: `Makefile`

**Interfaces:**
- Consumes: `pyproject.toml` (Task 1), `src/` (all prior tasks), `config/default.yaml` (Task 12).
- Produces: `docker compose build` succeeds for both services; `make replay PCAP=<path>` replays a pcap into the running `sensor` container's own `eth0`.

**Design:** `sensor` runs tcpreplay and Zeek sharing one interface — tcpreplay replays onto `eth0`, and Zeek (`zeek -i eth0 local`) captures on that same `eth0` in the same network namespace, seeing the replayed frames via Linux `AF_PACKET`'s default both-directions capture (no bridge/veth hand-off between the two processes, no dependency on the pcap's IPs existing in the Docker network). `backend` runs a single-node KRaft Kafka broker and the Python inference consumer in one container, with the consumer connecting to `localhost:9092`.

- [ ] **Step 1: Write `docker/sensor/local.zeek`**

```zeek
@load packages/zeek-kafka

redef Kafka::topic_name = "zeek-flows";
redef Kafka::tag_json = T;
redef use_conn_size_analyzer = T;
redef Kafka::kafka_conf = table(
	["metadata.broker.list"] = "backend:9092"
);

hook Conn::log_policy(rec: Conn::Info, id: Log::ID, filter: Log::Filter)
	{
	if ( rec$id$resp_p == 9092/tcp || rec$id$orig_p == 9092/tcp )
		break;
	}

event zeek_init() &priority=-10
	{
	Log::add_filter(Conn::LOG, [
		$name = "kafka-conn",
		$writer = Log::WRITER_KAFKAWRITER
	]);
	}
```

`use_conn_size_analyzer = T` is required for `orig_pkts`/`resp_pkts`/`orig_ip_bytes`/`resp_ip_bytes` to populate (confirmed against the sibling `zeek_pilot` project's notes) — without it those fields stay zero and the feature extractor's packet-count features are meaningless. `metadata.broker.list` points at the `backend` service name — this producer connection is the one remaining cross-container hop in the whole stack.

**Verified against a real build (post-implementation note) — this config differs substantially from the version originally written here, for reasons only discoverable by actually running the stack:**
1. `redef Log::default_writer = Log::WRITER_KAFKAWRITER` sends *every* log stream to Kafka, not just the ones named in `logs_to_send` — confirmed by seeing `packet_filter`, `reporter`, and `weird` records on the topic alongside `conn`. It also collides with the `Log::add_filter` mechanism's own "conn" path, and Zeek silently renames the stream to "conn-2" to resolve the clash, which breaks `JSONFlowParser`'s assumption that records are wrapped under a literal `"conn"` key. `logs_to_send` is also documented as mutually exclusive with per-filter predicates. The fix: use `Log::add_filter(Conn::LOG, ...)` directly, matching zeek-kafka's own README examples, and drop `Log::default_writer` and `Kafka::logs_to_send` entirely.
2. Zeek's AF_PACKET capture on `eth0` — the same mechanism that lets tcpreplay and Zeek share one interface — also captures the sensor's *own* librdkafka producer connection to `backend:9092`, which would otherwise get logged and republished as if it were monitored traffic. `Conn::log_policy` (Zeek 6.x's stream-filtering hook; the older `$pred` field on `Log::Filter` shown in some zeek-kafka examples no longer exists on this Zeek version's `Log::Filter` record) vetoes it. Both `orig_p` and `resp_p` must be checked: for connections where Zeek's capture starts mid-stream, it guesses direction heuristically and sometimes assigns the broker's `:9092` side as the "originator" instead of the "responder."
3. The Kafka writer reads broker connection settings from the global `Kafka::kafka_conf`, not from a filter's own `$config` table — with only a per-filter `$config` set, librdkafka fell back to a `localhost:9092` default and every produce attempt failed with "Connection refused."

- [ ] **Step 2: Write `docker/sensor/entrypoint.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail

# Zeek is the long-running foreground process for this container. tcpreplay is
# invoked on demand against this same container's eth0 via `make replay`
# (docker compose exec sensor tcpreplay ...) - it is not started here.
exec zeek -i eth0 local
```

```bash
chmod +x docker/sensor/entrypoint.sh
```

- [ ] **Step 3: Write `docker/sensor/Dockerfile`**

```dockerfile
FROM zeek/zeek:6.0.4

ARG LIBRDKAFKA_VERSION=2.3.0
ARG ZEEK_KAFKA_VERSION=v1.2.0
ARG TCPREPLAY_VERSION=4.4.3

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        cmake \
        git \
        libssl-dev \
        zlib1g-dev \
        libpcap-dev \
        libnet1-dev \
        wget \
        iproute2 \
    && rm -rf /var/lib/apt/lists/*

RUN wget -q "https://github.com/confluentinc/librdkafka/archive/refs/tags/v${LIBRDKAFKA_VERSION}.tar.gz" \
    && tar xzf "v${LIBRDKAFKA_VERSION}.tar.gz" \
    && cd "librdkafka-${LIBRDKAFKA_VERSION}" \
    && ./configure --prefix=/usr/local \
    && make -j"$(nproc)" \
    && make install \
    && ldconfig \
    && cd .. && rm -rf "librdkafka-${LIBRDKAFKA_VERSION}" "v${LIBRDKAFKA_VERSION}.tar.gz"

ENV LIBRDKAFKA_ROOT=/usr/local

# Debian's `apt-get install tcpreplay` ships a build with "Packet editing: disabled"
# (no tcpedit/checksum support), which --fixcsum requires. Build from source instead;
# the same source tree also produces tcpreplay-edit, the packet-editing-enabled binary.
RUN wget -q "https://github.com/appneta/tcpreplay/releases/download/v${TCPREPLAY_VERSION}/tcpreplay-${TCPREPLAY_VERSION}.tar.gz" \
    && tar xzf "tcpreplay-${TCPREPLAY_VERSION}.tar.gz" \
    && cd "tcpreplay-${TCPREPLAY_VERSION}" \
    && ./configure --prefix=/usr/local \
    && make -j"$(nproc)" \
    && make install \
    && cd .. && rm -rf "tcpreplay-${TCPREPLAY_VERSION}" "tcpreplay-${TCPREPLAY_VERSION}.tar.gz"

RUN zkg install --force --version "${ZEEK_KAFKA_VERSION}" https://github.com/SeisoLLC/zeek-kafka

COPY local.zeek /usr/local/zeek/share/zeek/site/local.zeek
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# /pcaps is a read-only bind mount (tcpreplay's input); Zeek needs a writable cwd
# for ad-hoc ASCII-writer debugging (see docs/VERIFICATION.md step 1) even though
# the default Kafka-writer config never writes local log files at all.
WORKDIR /var/log/zeek
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
```

`ZEEK_KAFKA_VERSION` and `LIBRDKAFKA_VERSION` are pinned per the spec's "do not float on master" requirement — check `https://github.com/SeisoLLC/zeek-kafka/releases` for the latest tag compatible with `zeek/zeek:6.0.4` before first build and update the ARG if needed. The `zkg install` step is the most likely first-build failure point (spec §8 step 1 note: "if step 1 fails, the problem is capabilities or the interface name — not Zeek or Kafka" — that's about capture; a `zkg`/librdkafka build failure is a separate, earlier failure mode to expect and debug on the first `docker compose build sensor`).

**Verified against a real build (post-implementation note):** several issues surfaced only once this was actually built and run against Docker Desktop, after this plan was written and the code implemented from it. All are already fixed in the shipped `docker/sensor/Dockerfile`, and the code block above has been updated to match:
1. zkg's version pin is a separate `--version VERSION` CLI flag, not an `@version` suffix on the package name/URL — the original `"seisollc/zeek-kafka@${ZEEK_KAFKA_VERSION}"` form errors with "package name not found in sources and also not a usable git URL"; it needs `--version "${ZEEK_KAFKA_VERSION}" https://github.com/SeisoLLC/zeek-kafka` instead.
2. the zeek-kafka plugin's C++ build needs `pcap.h` (from `libpcap-dev`), which wasn't in the original apt-get list and fails with `fatal error: pcap.h: No such file or directory` inside `Packet.h` otherwise.
3. `--fix-checksums` was never a real tcpreplay flag — a planning error, not a real option. The real mechanism is `-C`/`--fixcsum` on `tcpreplay-edit`, a *separate binary* from plain `tcpreplay` built by the same upstream source tree; the packet-editing/checksum subsystem (tcpedit) isn't linked into `apt-get install tcpreplay`'s prebuilt binary on Debian at all ("Packet editing: disabled" in `tcpreplay --version`), so tcpreplay must be compiled from source to get `tcpreplay-edit`.
4. the original `WORKDIR /pcaps` is a read-only bind mount, so Zeek can't write `conn.log` there for the plain-ASCII-writer debugging step in `docs/VERIFICATION.md` step 1 — moved `WORKDIR` to `/var/log/zeek` instead. This never affected the default Kafka-writer config, which writes no local log files at all.

- [ ] **Step 4: Write `docker/backend/server.properties`**

```properties
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093

listeners=PLAINTEXT://:9092,CONTROLLER://:9093
advertised.listeners=PLAINTEXT://backend:9092
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT

log.dirs=/var/lib/kafka/data
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
auto.create.topics.enable=true
```

- [ ] **Step 5: Write `docker/backend/entrypoint.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail

KAFKA_CONFIG=/opt/kafka/config/kraft/server.properties
KAFKA_DATA_DIR=/var/lib/kafka/data

if [ ! -f "${KAFKA_DATA_DIR}/meta.properties" ]; then
    CLUSTER_ID=$(kafka-storage.sh random-uuid)
    kafka-storage.sh format -t "${CLUSTER_ID}" -c "${KAFKA_CONFIG}"
fi

kafka-server-start.sh "${KAFKA_CONFIG}" &
KAFKA_PID=$!
trap 'kill -TERM "${KAFKA_PID}" 2>/dev/null' EXIT

echo "Waiting for Kafka to accept connections on :9092..."
until nc -z localhost 9092; do
    sleep 1
done
echo "Kafka is up."

uv run python -m inference_ids --config config/default.yaml
```

Known rough edge (documented in Global Constraints): this trap-based shutdown is best-effort, not a proper process supervisor. If Kafka needs to keep running independently of the inference process's restarts, split this back into two containers.

```bash
chmod +x docker/backend/entrypoint.sh
```

- [ ] **Step 6: Write `docker/backend/Dockerfile`**

```dockerfile
FROM eclipse-temurin:17-jre-jammy

ARG KAFKA_VERSION=3.7.0
ARG SCALA_VERSION=2.13

RUN apt-get update && apt-get install -y --no-install-recommends \
        wget \
        netcat-openbsd \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN wget -q "https://downloads.apache.org/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" \
    && tar xzf "kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" -C /opt \
    && mv "/opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION}" /opt/kafka \
    && rm "kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"

ENV PATH="/opt/kafka/bin:/root/.local/bin:${PATH}"
RUN wget -qO- https://astral.sh/uv/install.sh | sh

WORKDIR /app
COPY pyproject.toml uv.lock* ./
RUN uv python install 3.12 && uv sync --frozen --no-dev --no-install-project

COPY src ./src
COPY config ./config
COPY docker/backend/server.properties /opt/kafka/config/kraft/server.properties
COPY docker/backend/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

ENV PYTHONPATH=/app/src

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
```

- [ ] **Step 7: Write `docker-compose.yml`**

```yaml
services:
  backend:
    build:
      context: .
      dockerfile: docker/backend/Dockerfile
    container_name: backend
    volumes:
      - ./config:/app/config:ro
      - ./models:/models:ro
      - kafka-data:/var/lib/kafka/data
    ports:
      - "9092:9092"
    networks:
      - ntc

  sensor:
    build:
      context: ./docker/sensor
    container_name: sensor
    cap_add:
      - NET_ADMIN
      - NET_RAW
    volumes:
      - ./pcaps:/pcaps:ro
    depends_on:
      - backend
    networks:
      - ntc

networks:
  ntc:
    driver: bridge

volumes:
  kafka-data:
```

`backend`'s `9092:9092` host port mapping is only for the manual `kafka-console-consumer` debugging step in `docs/VERIFICATION.md` (Task 15) — the `sensor` container reaches it internally via the `backend` service name, never through the host.

- [ ] **Step 8: Write `scripts/replay.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail

PCAP="${1:?Usage: scripts/replay.sh <path-to-pcap> [pps]}"
PPS="${2:-100}"

mkdir -p pcaps
cp "${PCAP}" pcaps/
FILENAME="$(basename "${PCAP}")"

docker compose exec sensor tcpreplay-edit --intf1=eth0 --pps="${PPS}" --fixcsum "/pcaps/${FILENAME}"
```

```bash
chmod +x scripts/replay.sh
```

- [ ] **Step 9: Write `Makefile`**

```makefile
.PHONY: build up down replay test

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

replay:
	./scripts/replay.sh $(PCAP) $(PPS)

test:
	uv run pytest
```

- [ ] **Step 10: Verify compose config is well-formed**

Run: `docker compose config --quiet`
Expected: no output, exit code 0 (validates YAML + build contexts resolve; does not build images).

- [ ] **Step 11: Commit**

```bash
git add docker docker-compose.yml scripts/replay.sh Makefile
git commit -m "feat: add two-container Docker Compose stack (sensor, backend)"
```

---

### Task 15: Manual verification runbook

**Files:**
- Create: `docs/VERIFICATION.md`
- Create: `README.md`

**Interfaces:**
- Consumes: the full stack from Tasks 1–14.
- Produces: a written runbook of the spec §8 staged verification order — these steps require a real Docker/Zeek/Kafka runtime and are not automated by `pytest`; they must be run manually after the codebase is in place.

- [ ] **Step 1: Write `docs/VERIFICATION.md`**

```markdown
# Manual Verification Runbook

Run these in order. Do not skip ahead — each stage isolates a different failure mode
(see the note at the end of each step).

## 0. Prerequisites

- A test pcap in `pcaps/` (e.g. `zeek_pilot/quickstart.pcap` from the sibling project, or any small pcap).
- A saved reference checkpoint at `models/reference_ids_model.pth` matching `config/default.yaml`'s
  `inference_engine.pytorch.init_kwargs` (see Task 12/13 for the one-liner to generate it).

## 1. Capture on the sensor's own interface (ASCII writer, no Kafka)

Temporarily run Zeek with the plain ASCII writer (skip `local.zeek`'s Kafka redef) to confirm capture works
at all, independent of Kafka:

```bash
docker compose build sensor
docker compose run --rm --entrypoint "zeek -i eth0" sensor
# in another terminal, replay into that same running container:
docker compose exec sensor tcpreplay-edit --intf1=eth0 --pps=100 --fixcsum /pcaps/<your>.pcap
```

Check `conn.log` is created inside the sensor container and has rows matching the replayed pcap:

```bash
docker compose exec sensor cat conn.log
```

**If this fails:** the problem is capabilities (`NET_ADMIN`/`NET_RAW`) or the interface name (`eth0`), not
Zeek or Kafka. Check `docker compose exec sensor ip link show eth0` for MTU and link state. Since tcpreplay
and Zeek share this container's network namespace, this also isolates whether Zeek's default bidirectional
`AF_PACKET` capture is actually picking up tcpreplay's self-generated egress traffic — if `conn.log` stays
empty despite tcpreplay reporting successful transmission, that local-capture assumption is the first thing
to re-check.

## 2. Kafka writer

Bring up the full stack (this uses the default entrypoint — Zeek with `local.zeek`'s Kafka writer):

```bash
docker compose up -d backend
docker compose up -d sensor
./scripts/replay.sh pcaps/<your>.pcap
```

Confirm records land on the topic (using the `backend` container's own `kafka-console-consumer.sh`, or the
host-mapped port 9092 from outside Docker):

```bash
docker compose exec backend kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic zeek-flows --from-beginning --max-messages 5
```

**If this fails:** re-check the zeek-kafka plugin build (`docker compose build sensor` logs) and
`Kafka::kafka_conf`'s `metadata.broker.list` in `docker/sensor/local.zeek` (must be `backend:9092`). Also
confirm `backend`'s entrypoint script actually got Kafka listening before `sensor` started producing —
check `docker compose logs backend` for "Kafka is up."

## 3. Parser equivalence

Already automated — `uv run pytest tests/unit/test_parser_equivalence.py`. For an end-to-end version using
this pcap specifically: run the pcap through Zeek twice (ASCII writer to file per step 1, and Kafka JSON per
step 2), pull both outputs, parse each with `TSVFlowParser`/`JSONFlowParser`, and diff the resulting
`FlowRecord` lists by `uid`.

## 4. Feature provenance — STOP AND CONFIRM

`StubFeatureExtractor` (`src/inference_ids/adapters/feature_extractor_stub.py`) is a placeholder. Before
scoring real traffic, confirm with the model-training team:
- the exact feature list, order, and units their model expects,
- whether every feature is derivable from `conn.log` (with `use_conn_size_analyzer = T`) or needs a custom
  `.zeek` script,
- that `conn.log`'s termination-triggered write timing (see spec section 5 — up to ~1 hour delay for
  long-lived flows) is acceptable for the intended detection latency.

Do not swap in a production `FeatureExtractor` until this is answered.

## 5. Single-sample inference

```bash
docker compose up -d backend
docker compose logs -f backend
```

With `pipeline.max_batch_size: 1` in `config/default.yaml`, confirm one log line per replayed flow appears
via `LoggingResultSink` in the `backend` container's logs (the inference process and the Kafka broker share
this container, so both Kafka's and the consumer's log lines are interleaved — that's expected here).

## 6. Batching

Raise `pipeline.max_batch_size` and `pipeline.batch_window_ms` in `config/default.yaml`, restart the
`backend` service, and confirm throughput improves without dropped messages (compare Kafka consumer lag via
`docker compose exec backend kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group inference-ids`).
Note that restarting `backend` also restarts Kafka (see the bundling tradeoff in the plan's Global
Constraints) — this loses any in-flight but un-consumed messages on restart, which is fine for functional
testing but worth knowing about.

## Known environment limits (WSL2 / Docker Desktop)

- MTU on the internal bridge can be sub-1500 — check `docker compose exec sensor ip link show eth0` if a
  pcap with large frames shows unexplained truncation.
- Throughput here is not representative of bare-metal or Jetson numbers (host -> Hyper-V -> WSL2 VM ->
  nested bridge). Do not use timings from this environment in any performance write-up.
- The `sensor`/`backend` split is a local-dev convenience, not a deployment topology: a real deployment
  drops tcpreplay entirely (Zeek listens on a real mirrored interface instead) and very likely puts Kafka
  and the inference consumer on separate, independently-scaled infrastructure rather than one container.
```

- [ ] **Step 2: Write `README.md`**

```markdown
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
docker compose build
make replay PCAP=pcaps/quickstart.pcap
```
```

- [ ] **Step 3: Run the full test suite one last time**

Run: `uv run pytest -v`
Expected: all tests from Tasks 1–13 pass.

- [ ] **Step 4: Commit**

```bash
git add docs/VERIFICATION.md README.md
git commit -m "docs: add manual verification runbook and README"
```

---

## Self-Review Notes

- **Spec coverage:** tcpreplay/Zeek/Kafka/inference services (Task 14), `zeek-kafka` pinned build + `local.zeek` (Task 14), TSV/JSON parser migration with shared coercion for the missing-key-vs-unset divergence (Tasks 3–5), parser equivalence acceptance test (Task 6, plus manual pcap-driven version in Task 15), feature-provenance open item surfaced rather than approximated (Task 7's stub + Task 15 step 4 stop-and-confirm gate), config-driven model loading/device/precision (Task 8/12), configurable batch window down to a single sample (Task 10/12), `make replay` entrypoint (Task 14), staged verification order (Task 15). Hexagonal requirement: every cross-boundary dependency is a `Protocol` (Task 2) with type-dispatched factories (Task 12) — no adapter is imported by name in `application/pipeline.py` or `bootstrap.py`.
- **Placeholder scan:** no TBD/TODO markers; the one intentional placeholder (`StubFeatureExtractor`) is explicitly labeled as such per the user's own instruction, not left vague.
- **Type consistency:** `FlowRecord`'s 20 fields are identical across `domain/models.py`, `tsv_parser.py`, and `json_parser.py`. `FeatureExtractor.extract` returns `(n, 11)` consistently between the stub (Task 7) and the reference model's `init_kwargs.input_features: 11` (Task 12's `config/default.yaml`). `InferenceEngine.predict` returns `list[Prediction]` consistently between Task 8's adapter and Task 10's pipeline/tests.
