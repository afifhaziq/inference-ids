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
