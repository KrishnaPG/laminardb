# Integrated workload observations

**Status/date: S12 preparation, 2026-09-21.** This extends the existing server process-soak
harness. It does not close S12, G3, G5 or the release security gate.

The supported measurement is **single-node Kafka → Kafka, at-least-once**, with one or four
independent projection pipelines. Each preserves the source `origin` field and performs
`id * 3 + 7` and `UPPER(payload)`. Uniform,
Zipf (exponent 1) and single-hot-key inputs are deterministic from the seed. The payload width,
partition count, rate and checkpoint cadence are recorded. Key distribution in this projection
workload exercises Kafka partition placement; it does not qualify keyed aggregate or join state.
The generated server configuration records the existing memory controls used by the run:
256 MiB DataFusion reservations, a 64 MiB shared source queue and 64 MiB per graph input port.
Kafka source polls cap batches at 1,000 records with an 8,192-record reader channel; the sink
uses 5 ms linger and 16,384-byte producer batches. Actual batches depend on arrivals.

Use Python 3.11 or later and run one workload at a time on a quiet host. A Linux server exposes
process RSS through the existing Prometheus process collector. Windows can run the harness and oracle tests, but a
declared RSS ceiling requires that collector and therefore fails without Linux RSS evidence.
The release build defaults to `--no-default-features --features cluster,kafka`; `cluster` enables
the existing test target while the actual server mode remains `single`. Add `jemalloc` explicitly
to `--features` when that allocator is part of the candidate being qualified. Features, toolchain,
lockfile, executable hashes, source files and dirty diff are archived with each run.
Install the Linux native build prerequisites used by
[`checkpoint-fault-soak.yml`](../../.github/workflows/checkpoint-fault-soak.yml), including
`libcurl4-openssl-dev`; the Kafka C library requires its headers even for this plaintext fixture.

For a local, **non-certifying** smoke run, start the repository's test broker and run:

```bash
docker compose -f tests/docker/compose.yml up -d --wait redpanda
export LAMINAR_SOAK_KAFKA_SOURCE_BROKERS=127.0.0.1:19092
python3 tools/run_workload_qualification.py \
  --spec tests/qualification/single-node-smoke.json \
  --output target/s12-workload/smoke
```

The output directory must be new. Brokers are real external Kafka boundaries; an optional
`LAMINAR_QUALIFICATION_SINK_BROKERS` selects a separate output broker. Use disposable test
brokers: topics have unique names and are retained for investigation. The harness stops its own
server processes. This fixture creates plaintext Kafka topics with replication factor one; it
does not qualify broker high availability. Each run retains
checkpoints, source acknowledgement offsets, all observed output
IDs/offsets/times, full Prometheus scrapes and logs. The final SHA-256 index detects subsequent
artifact changes; it is not a cryptographic signature or external attestation.

Copy the smoke JSON to declare a workload. `fault` accepts `none`, `process_kill` (after half
the configured run, once a committed cut exists), or `source_pause` (five seconds at halfway).
`hardware` must identify the target machine. `rps_per_pipeline` is the offered rate for **each**
pipeline. `seconds` includes warmup and an intentional source pause, but excludes final drain.
`warmup_seconds` only excludes early records from latency statistics; their delivery is still
verified. The bounded oracle permits at most 100 million input rows across all pipelines.

Leave `limits: null` for observations. To check declared numerical ceilings, replace it with an
object containing every field below, using workload-approved values:

| Field | Meaning |
|---|---|
| `visibility_ms` | Ordered array of p50, p95, p99, p99.9 ceilings, applied to each pipeline |
| `rss_bytes` | Maximum sampled server RSS |
| `rss_growth_bytes_per_second` | Maximum fitted RSS slope over the second half of offered load |
| `backlog_growth_rows_per_second` | Maximum fitted offered-minus-observed backlog slope over the same window |
| `checkpoint_p99_ms` | Maximum final cumulative checkpoint p99 upper bound across process generations |
| `recovery_ms` | Kill to a continuous externally visible prefix including a post-death scheduled input, plus a checkpoint completed by the restarted process |

Declared-limit runs require at least 3,600 seconds of offered load after warmup, at least
60 seconds of warmup, 100,000 measured records **per pipeline**, and 100 checkpoint observations
per process generation. Prometheus
histograms retain finite bucket upper bounds; overflow or unavailable evidence cannot satisfy
a limit. End-to-end visibility uses the existing Prometheus histogram implementation with
one-percent bucket spacing, a 1 μs minimum bucket and a finite range exceeding ten minutes.
The p99.9 floor provides at least 100 observations in the upper 0.1%; inspect the retained
distribution as well as the point estimate. Cycle and checkpoint-stall measurements remain
separate in `metrics.jsonl`, with the engine's coarser bucket resolution.
A steady run with 60 seconds of warmup therefore needs `seconds >= 3660`; the intentional
five-second source pause requires `seconds >= 3665` for the same offered-load duration.

Producer and independent output consumer share one monotonic observer clock. Latency begins at
the **scheduled** arrival and ends at the first verified external Kafka observation, including
producer delay and observer polling. A producer stall never resets the schedule. Offered,
enqueued, broker-acknowledged and externally observed counts expose backpressure. The observer
validates every output's origin, ID, key, arithmetic result and payload, counts ALO duplicates, and
requires all IDs followed by a drained, stable public Kafka boundary. A missing ID cannot be
hidden by later records or by another pipeline. Source origins make cross-routing detectable even
when different pipelines use the same IDs and payload. `backlog` is an end-to-end row backlog, not an
engine queue-byte measurement. RSS is sampled; spikes shorter than a scrape interval may be missed.

Use `--repetitions 3` for the same declared spec, then separately run the one/four-pipeline and
uniform/Zipf/hot-key combinations. The runner never emits `s12_qualified: true`. Even a
`run_limits_passed` report still needs the complete S12 matrix: slow/failed sinks, checkpoint
under load, corrupt cuts, expired replay, G9 Backpressure/Fail saturation/checkpoint/restart
external-ledger cases, and the intended mode's recovery/topology operations. Tables/MVs,
stateful SQL, cluster delivery, native cloud boundaries and exact compositions need their own
declared workloads and independent evidence. See [the execution plan](../../docs/production-hardening-plan.md#s12--qualify-the-integrated-workload-g5).
