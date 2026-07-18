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
