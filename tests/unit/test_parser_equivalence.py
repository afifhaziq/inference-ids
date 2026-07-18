import json
from pathlib import Path

from inference_ids.adapters.json_parser import JSONFlowParser
from inference_ids.adapters.tsv_parser import TSVFlowParser, iter_ascii_log_rows

TSV_FIXTURE = Path(__file__).parent.parent / "fixtures" / "conn.log"
JSON_FIXTURE = Path(__file__).parent.parent / "fixtures" / "conn.json"


def test_tsv_and_json_parsers_produce_identical_flow_records():
    tsv_rows = iter_ascii_log_rows(TSV_FIXTURE)
    tsv_records = [TSVFlowParser().parse(row) for row in tsv_rows]

    with JSON_FIXTURE.open() as handle:
        json_records = [JSONFlowParser().parse(json.loads(line)) for line in handle if line.strip()]

    assert len(tsv_records) == len(json_records)
    for tsv_record, json_record in zip(tsv_records, json_records):
        assert tsv_record == json_record
