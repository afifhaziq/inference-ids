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
