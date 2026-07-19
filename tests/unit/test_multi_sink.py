from inference_ids.adapters.multi_sink import MultiResultSink
from inference_ids.domain.models import FlowRecord, Prediction


def _record(uid: str) -> FlowRecord:
    return FlowRecord(
        uid=uid, ts=0.0, duration=1.5, orig_h="10.0.0.1", orig_p=1111,
        resp_h="10.0.0.2", resp_p=80, proto="tcp", service="http",
        conn_state="SF", history="ShADadfF", missed_bytes=0,
        orig_pkts=6, orig_ip_bytes=740, resp_pkts=8, resp_ip_bytes=1620,
        orig_bytes=350, resp_bytes=1200, local_orig=True, local_resp=False,
    )


class FakeSink:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def emit(self, record: FlowRecord, prediction: Prediction) -> None:
        self.calls.append((record.uid, prediction.label))


def test_emit_calls_every_wrapped_sink_in_order():
    sink_a = FakeSink()
    sink_b = FakeSink()
    multi = MultiResultSink(sinks=[sink_a, sink_b])
    prediction = Prediction(label="benign", confidence=0.9, logits=[1.0], class_index=0)

    multi.emit(_record("C1"), prediction)

    assert sink_a.calls == [("C1", "benign")]
    assert sink_b.calls == [("C1", "benign")]


def test_emit_with_no_sinks_does_nothing():
    multi = MultiResultSink(sinks=[])
    prediction = Prediction(label="benign", confidence=0.9, logits=[1.0], class_index=0)

    multi.emit(_record("C1"), prediction)  # must not raise
