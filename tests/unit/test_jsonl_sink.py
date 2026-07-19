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
