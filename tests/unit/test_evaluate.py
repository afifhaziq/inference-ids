import json

from inference_ids.evaluate import evaluate, load_jsonl, load_labels


def _write_jsonl(path, rows: list[dict]) -> None:
    path.write_text("\n".join(json.dumps(r) for r in rows) + "\n")


def test_load_jsonl_parses_happy_path(tmp_path):
    path = tmp_path / "rows.jsonl"
    _write_jsonl(path, [{"uid": "C1", "x": 1}, {"uid": "C2", "x": 2}])

    rows = load_jsonl(str(path))

    assert rows == {"C1": {"uid": "C1", "x": 1}, "C2": {"uid": "C2", "x": 2}}


def test_load_jsonl_skips_malformed_line(tmp_path, capsys):
    path = tmp_path / "rows.jsonl"
    path.write_text('{"uid": "C1", "x": 1}\nnot json\n{"uid": "C2", "x": 2}\n')

    rows = load_jsonl(str(path))

    assert set(rows.keys()) == {"C1", "C2"}
    assert "malformed" in capsys.readouterr().err


def test_load_labels_extracts_label_field(tmp_path):
    path = tmp_path / "labels.jsonl"
    _write_jsonl(path, [{"uid": "C1", "label": 0}, {"uid": "C2", "label": 2}])

    labels = load_labels(str(path))

    assert labels == {"C1": 0, "C2": 2}


def test_evaluate_computes_matched_and_unmatched_counts():
    predictions = {
        "C1": {"predicted_index": 0},
        "C2": {"predicted_index": 1},
        "C3": {"predicted_index": 0},  # no label -- unmatched prediction
    }
    labels = {
        "C1": 0,
        "C2": 1,
        "C4": 2,  # no prediction -- unmatched label
    }

    result = evaluate(predictions, labels, label_map={0: 0, 1: 1, 2: 2}, class_names=["benign", "scan", "dos"])

    assert result.matched == 2
    assert result.unmatched_predictions == 1
    assert result.unmatched_labels == 1
    assert "benign" in result.report_text
    assert result.confusion == [[1, 0, 0], [0, 1, 0], [0, 0, 0]]


def test_evaluate_applies_non_identity_label_map():
    predictions = {"C1": {"predicted_index": 0}}
    labels = {"C1": 5}  # dataset uses 5 to mean "benign"

    result = evaluate(predictions, labels, label_map={5: 0}, class_names=["benign", "scan", "dos"])

    assert result.matched == 1
    assert result.confusion == [[1, 0, 0], [0, 0, 0], [0, 0, 0]]


def test_evaluate_reports_zero_matched_without_raising_on_empty_overlap():
    predictions = {"C1": {"predicted_index": 0}}
    labels = {"C2": 1}

    result = evaluate(predictions, labels, label_map={0: 0, 1: 1, 2: 2}, class_names=["benign", "scan", "dos"])

    assert result.matched == 0
    assert result.unmatched_predictions == 1
    assert result.unmatched_labels == 1
    assert result.report_text == ""
