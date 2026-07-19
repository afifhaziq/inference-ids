from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass

from sklearn.metrics import classification_report, confusion_matrix

from inference_ids.config import load_config


@dataclass
class EvaluationResult:
    matched: int
    unmatched_predictions: int
    unmatched_labels: int
    report_text: str
    confusion: list[list[int]]
    class_names: list[str]


def load_jsonl(path: str) -> dict[str, dict]:
    rows: dict[str, dict] = {}
    with open(path, encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                print(f"warning: skipping malformed line {line_number} in {path}", file=sys.stderr)
                continue
            rows[row["uid"]] = row
    return rows


def load_labels(path: str) -> dict[str, int]:
    return {uid: row["label"] for uid, row in load_jsonl(path).items()}


def _default_label_map(class_names: list[str]) -> dict[int, int]:
    return {i: i for i in range(len(class_names))}


def evaluate(
    predictions: dict[str, dict],
    labels: dict[str, int],
    label_map: dict[int, int],
    class_names: list[str],
) -> EvaluationResult:
    matched_uids = predictions.keys() & labels.keys()
    unmatched_predictions = len(predictions.keys() - labels.keys())
    unmatched_labels = len(labels.keys() - predictions.keys())

    if not matched_uids:
        return EvaluationResult(
            matched=0,
            unmatched_predictions=unmatched_predictions,
            unmatched_labels=unmatched_labels,
            report_text="",
            confusion=[],
            class_names=class_names,
        )

    y_true = [label_map.get(labels[uid], labels[uid]) for uid in matched_uids]
    y_pred = [predictions[uid]["predicted_index"] for uid in matched_uids]
    known_indices = list(range(len(class_names)))

    report_text = classification_report(
        y_true, y_pred, labels=known_indices, target_names=class_names, zero_division=0
    )
    confusion = confusion_matrix(y_true, y_pred, labels=known_indices).tolist()

    return EvaluationResult(
        matched=len(matched_uids),
        unmatched_predictions=unmatched_predictions,
        unmatched_labels=unmatched_labels,
        report_text=report_text,
        confusion=confusion,
        class_names=class_names,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate persisted predictions against a labeled dataset.")
    parser.add_argument("--predictions", required=True, help="Path to JSONL predictions written by JSONLResultSink")
    parser.add_argument("--labels", required=True, help="Path to JSONL labels: one {uid, label} object per line")
    parser.add_argument("--config", default="config/default.yaml", help="Config to read class_names/label_map from")
    args = parser.parse_args()

    config = load_config(args.config)
    class_names = config.inference_engine.pytorch.class_names
    label_map = config.evaluation.label_map or _default_label_map(class_names)

    predictions = load_jsonl(args.predictions)
    labels = load_labels(args.labels)

    result = evaluate(predictions, labels, label_map, class_names)

    print(f"Predictions: {len(predictions)}  Labels: {len(labels)}  Matched: {result.matched}")
    print(f"Unmatched predictions (no label): {result.unmatched_predictions}")
    print(f"Unmatched labels (no prediction): {result.unmatched_labels}")

    if result.matched == 0:
        print("\nNo overlapping uids between predictions and labels -- nothing to score.")
        return

    print("\n" + result.report_text)
    print("Confusion matrix (rows=true, cols=predicted):")
    header = "            " + "  ".join(f"{name:>8s}" for name in class_names)
    print(header)
    for name, row in zip(class_names, result.confusion):
        print(f"{name:>10s}  " + "  ".join(f"{v:8d}" for v in row))


if __name__ == "__main__":
    main()
