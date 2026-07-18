from pathlib import Path

from inference_ids.adapters.tsv_parser import TSVFlowParser, iter_ascii_log_rows

FIXTURE = Path(__file__).parent.parent / "fixtures" / "conn.log"


def test_iter_ascii_log_rows_reads_fields_header():
    rows = iter_ascii_log_rows(FIXTURE)
    assert len(rows) == 2
    assert rows[0]["proto"] == "tcp"
    assert rows[1]["proto"] == "udp"


def test_tsv_parser_maps_row_to_flow_record():
    rows = iter_ascii_log_rows(FIXTURE)
    record = TSVFlowParser().parse(rows[0])

    assert record.uid == "CXk4tK1AbAA1B2C3D4"
    assert record.orig_h == "192.168.1.10"
    assert record.orig_p == 51514
    assert record.resp_h == "93.184.216.34"
    assert record.resp_p == 80
    assert record.proto == "tcp"
    assert record.service == "http"
    assert record.duration == 1.245
    assert record.orig_bytes == 350
    assert record.resp_bytes == 1200
    assert record.conn_state == "SF"
    assert record.local_orig is True
    assert record.local_resp is False
    assert record.missed_bytes == 0
    assert record.history == "ShADadfF"
    assert record.orig_pkts == 6
    assert record.orig_ip_bytes == 740
    assert record.resp_pkts == 8
    assert record.resp_ip_bytes == 1620


def test_tsv_parser_maps_unset_marker_to_empty_string():
    row = {"service": "-", "proto": "tcp"}
    record = TSVFlowParser().parse(row)
    assert record.service == ""
