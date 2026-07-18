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
