from __future__ import annotations

from inference_ids.adapters._zeek_coercion import coerce_bool, coerce_float, coerce_int, coerce_str
from inference_ids.domain.models import FlowRecord


class JSONFlowParser:
    """Parses a zeek-kafka JSON message into a FlowRecord.

    `Kafka::tag_json = T` wraps each record as {"conn": {...}}; this adapter unwraps
    that key so the Kafka transport adapter can stay agnostic of the log shape.
    """

    def parse(self, raw: dict) -> FlowRecord:
        conn = raw.get("conn", raw)
        return FlowRecord(
            uid=coerce_str(conn.get("uid")),
            ts=coerce_float(conn.get("ts")),
            duration=coerce_float(conn.get("duration")),
            orig_h=coerce_str(conn.get("id.orig_h")),
            orig_p=coerce_int(conn.get("id.orig_p")),
            resp_h=coerce_str(conn.get("id.resp_h")),
            resp_p=coerce_int(conn.get("id.resp_p")),
            proto=coerce_str(conn.get("proto")),
            service=coerce_str(conn.get("service")),
            conn_state=coerce_str(conn.get("conn_state")),
            history=coerce_str(conn.get("history")),
            missed_bytes=coerce_int(conn.get("missed_bytes")),
            orig_pkts=coerce_int(conn.get("orig_pkts")),
            orig_ip_bytes=coerce_int(conn.get("orig_ip_bytes")),
            resp_pkts=coerce_int(conn.get("resp_pkts")),
            resp_ip_bytes=coerce_int(conn.get("resp_ip_bytes")),
            orig_bytes=coerce_int(conn.get("orig_bytes")),
            resp_bytes=coerce_int(conn.get("resp_bytes")),
            local_orig=coerce_bool(conn.get("local_orig")),
            local_resp=coerce_bool(conn.get("local_resp")),
        )
