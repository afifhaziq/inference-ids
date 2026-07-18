# Manual Verification Runbook

Run these in order. Do not skip ahead — each stage isolates a different failure mode
(see the note at the end of each step).

## 0. Prerequisites

- A test pcap in `pcaps/` (e.g. `zeek_pilot/quickstart.pcap` from the sibling project, or any small pcap).
- A saved reference checkpoint at `models/reference_ids_model.pth` matching `config/default.yaml`'s
  `inference_engine.pytorch.init_kwargs` (see Task 12/13 for the one-liner to generate it).

## 1. Capture on the sensor's own interface (ASCII writer, no Kafka)

Temporarily run Zeek with the plain ASCII writer (skip `local.zeek`'s Kafka redef) to confirm capture works
at all, independent of Kafka:

```bash
docker compose build sensor
docker compose run --rm --entrypoint "zeek -i eth0" sensor
# in another terminal, replay into that same running container:
docker compose exec sensor tcpreplay --intf1=eth0 --pps=100 --fix-checksums /pcaps/<your>.pcap
```

Check `conn.log` is created inside the sensor container and has rows matching the replayed pcap:

```bash
docker compose exec sensor cat conn.log
```

**If this fails:** the problem is capabilities (`NET_ADMIN`/`NET_RAW`) or the interface name (`eth0`), not
Zeek or Kafka. Check `docker compose exec sensor ip link show eth0` for MTU and link state. Since tcpreplay
and Zeek share this container's network namespace, this also isolates whether Zeek's default bidirectional
`AF_PACKET` capture is actually picking up tcpreplay's self-generated egress traffic — if `conn.log` stays
empty despite tcpreplay reporting successful transmission, that local-capture assumption is the first thing
to re-check.

## 2. Kafka writer

Bring up the full stack (this uses the default entrypoint — Zeek with `local.zeek`'s Kafka writer):

```bash
docker compose up -d backend
docker compose up -d sensor
./scripts/replay.sh pcaps/<your>.pcap
```

Confirm records land on the topic (using the `backend` container's own `kafka-console-consumer.sh`, or the
host-mapped port 9092 from outside Docker):

```bash
docker compose exec backend kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic zeek-flows --from-beginning --max-messages 5
```

**If this fails:** re-check the zeek-kafka plugin build (`docker compose build sensor` logs) and
`Kafka::kafka_conf`'s `metadata.broker.list` in `docker/sensor/local.zeek` (must be `backend:9092`). Also
confirm `backend`'s entrypoint script actually got Kafka listening before `sensor` started producing —
check `docker compose logs backend` for "Kafka is up."

## 3. Parser equivalence

Already automated — `uv run pytest tests/unit/test_parser_equivalence.py`. For an end-to-end version using
this pcap specifically: run the pcap through Zeek twice (ASCII writer to file per step 1, and Kafka JSON per
step 2), pull both outputs, parse each with `TSVFlowParser`/`JSONFlowParser`, and diff the resulting
`FlowRecord` lists by `uid`.

## 4. Feature provenance — STOP AND CONFIRM

`StubFeatureExtractor` (`src/inference_ids/adapters/feature_extractor_stub.py`) is a placeholder. Before
scoring real traffic, confirm with the model-training team:
- the exact feature list, order, and units their model expects,
- whether every feature is derivable from `conn.log` (with `use_conn_size_analyzer = T`) or needs a custom
  `.zeek` script,
- that `conn.log`'s termination-triggered write timing (see spec section 5 — up to ~1 hour delay for
  long-lived flows) is acceptable for the intended detection latency.

Do not swap in a production `FeatureExtractor` until this is answered.

## 5. Single-sample inference

```bash
docker compose up -d backend
docker compose logs -f backend
```

With `pipeline.max_batch_size: 1` in `config/default.yaml`, confirm one log line per replayed flow appears
via `LoggingResultSink` in the `backend` container's logs (the inference process and the Kafka broker share
this container, so both Kafka's and the consumer's log lines are interleaved — that's expected here).

## 6. Batching

Raise `pipeline.max_batch_size` and `pipeline.batch_window_ms` in `config/default.yaml`, restart the
`backend` service, and confirm throughput improves without dropped messages (compare Kafka consumer lag via
`docker compose exec backend kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group inference-ids`).
Note that restarting `backend` also restarts Kafka (see the bundling tradeoff in the plan's Global
Constraints) — this loses any in-flight but un-consumed messages on restart, which is fine for functional
testing but worth knowing about.

## Known environment limits (WSL2 / Docker Desktop)

- MTU on the internal bridge can be sub-1500 — check `docker compose exec sensor ip link show eth0` if a
  pcap with large frames shows unexplained truncation.
- Throughput here is not representative of bare-metal or Jetson numbers (host -> Hyper-V -> WSL2 VM ->
  nested bridge). Do not use timings from this environment in any performance write-up.
- The `sensor`/`backend` split is a local-dev convenience, not a deployment topology: a real deployment
  drops tcpreplay entirely (Zeek listens on a real mirrored interface instead) and very likely puts Kafka
  and the inference consumer on separate, independently-scaled infrastructure rather than one container.
