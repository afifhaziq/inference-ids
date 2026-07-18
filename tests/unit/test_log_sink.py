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
