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
