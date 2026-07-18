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
