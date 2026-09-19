# Production readiness review

**Status:** baseline review and backlog; current implementation progress is recorded in the execution plan.
**Date:** 2026-09-19. **Reviewed HEAD:** `b429d0dfd02a435219f1b5977a442da9972e0c3d` (`0.30.0`).

The [execution plan](production-hardening-plan.md) turns this backlog into bounded sessions,
dependencies, correctness tests and performance gates. Planning refinements below were checked
against the same HEAD. S1–S3, S4a/S4b/S4d dependency repairs, S4c CI/release enforcement and S5 Kafka
progress telemetry are implemented and verified locally. Remaining S4 findings now block CI/release.
Findings below describe the reviewed baseline. See the execution plan for validation and remaining work.

LaminarDB already has a substantial streaming execution, checkpoint, fencing, recovery, and
subscription implementation. The next investment should make its admitted workloads bounded,
operable, and demonstrably reliable. Replacing this architecture with a general distributed
stream-processing framework is not justified by this review.

Two P0s are conditional on configuration: HTTP administrative routes can be exposed on an
untrusted network without authentication, and opt-in `ShedOldest` can be combined with a durable
delivery label even though it drops buffered rows. The builder allows ALO; direct configuration
bypasses even its EO rejection. Default loopback HTTP and lossless backpressure avoid
those conditions. Other findings are hardening work or restrictions, not claims of demonstrated
checkpoint corruption. Passing unit tests is not production certification.

## Evidence and classification

Implementation at the SHA above takes precedence over READMEs and design documents. Inspection
covered all six workspace manifests, the execution and lifecycle paths below, connector contracts,
unit/integration/fault-soak tests, benchmark harnesses, workflows, and representative examples.
This is an architecture review with targeted path inspection, not a line-by-line security audit of
every dependency. Service-backed and performance evidence is distinguished from tests executed
locally. Links and line references refer to the reviewed SHA.

Severity: **P0** blocks a credible deployment in the stated scope; **P1** important hardening;
**P2** useful improvement; **P3** optional/future. An IMPLEMENTED entry's severity identifies the
importance of preserving the property, not an outstanding defect. Status vocabulary:
IMPLEMENTED, IMPLEMENTED-BUT-UNVERIFIED, PARTIAL, MISSING, DOCUMENTATION-ONLY,
STALE-DOCUMENTATION, EXPERIMENTAL, INTENTIONALLY-UNSUPPORTED.

### Validation performed

Host: Windows x86-64, Rust/Cargo 1.98.0, existing lockfile, offline dependency cache. No source
changes, broker provisioning, cloud mutations, or new performance claims were made.

| Command / evidence | Result and limits |
|---|---|
| `cargo test --workspace --lib --locked --offline` | Initial unconstrained run: connector suite 1,963 passed, three file lifecycle tests timed out. Execution stopped there. |
| Same command with `-- --test-threads=2` | Connectors 1,966/1,966 and core 959/959 passed; database test process hit `STATUS_STACK_OVERFLOW` in `coordinated_recovery::tests::evidence_only_worker_consumes_tombstoned_release_after_stopped_quorum`. |
| Same command, two threads and `RUST_MIN_STACK=4194304` | Passed: 5,659 tests, one ignored (connectors 1,966; core 959; DB 1,865; SQL 869; derive has no unit tests). This stack setting and concurrency follow the Windows resource settings in `.github/workflows/ci.yml:135–145`; the runner remains libtest, not nextest. |
| Targeted integration/server build | Initial parallel link exhausted Windows paging-file capacity (OS error 1455); stopped the remaining links and rebuilt with `-j 1`. |
| Targeted integration and server tests | Passed: CDC admission 2; checkpoint integration 3; incremental emission 13; recovery integration 3; shared-source isolation 2; server unit tests 331. Initial incremental run had 12 passes and one conditional-put probe cleanup timeout before the expected LDB-1300 rejection; the complete suite passed with one test thread. |
| `cargo clippy --workspace --all-features --all-targets --locked --offline -j 1 -- -D warnings` | Passed. Cargo reports a nonfatal future-Rust-compatibility warning for transitive `proc-macro-error2 2.0.1`. |
| `cargo clippy --workspace --no-default-features --locked --offline -j 1 -- -D warnings` | Passed. |
| `cargo +nightly fmt --all -- --check` | Passed. |
| `cargo run --offline --quiet --manifest-path tools/readability-check/Cargo.toml -- .` | Passed: 19 reviewed module exceptions, 195 function exceptions. |
| `python tools/check_analytical_dependencies.py` with `CARGO_NET_OFFLINE=true` | Passed: expected analytical generations, registry sources, no Git dependencies. |
| Criterion / native-provider / external-service fault soaks | Inspected; not executed in this session. No before/after benchmark is required for this document-only change. |

Local command logs are under ignored `target/production-readiness/`. Initial failures remain part
of this record even if a resource-matched rerun passes. They are not, by themselves, evidence of a
release-build lifecycle failure.

The targeted Cargo build selected `--workspace --bin laminardb` and tests `cdc_admission`,
`checkpoint_integration`, `incremental_emit`, `recovery_integration`, and `shared_source_isolation`.
Final serial reruns invoked the current-HEAD test executables emitted by that build with
`RUST_MIN_STACK=4194304` and `--test-threads=1`, preserving its feature graph and avoiding a new
build caused by narrowing Cargo's target selection. Total distinct selected tests passed: **6,013**;
one library test requiring an ONNX model download was ignored. Native cloud/broker certification,
cross-release upgrade execution, and production latency qualification remain outside these results.

An existing local Azure report under
`target/native-evidence/local-azure-certification-8c7a3b63/certification-summary.md` records a passing
three-node ALO process-kill soak at SHA `8c7a3b63b96a8f40bbdf8a3d28e4a8a39bed43e8`, 250 rows/s,
65,536 keys, two kills, and a 90-second recovery ceiling. The Git diff from that SHA to reviewed HEAD
changes only the native workflow and cloud-support documentation; engine source is unchanged.
This is relevant positive evidence for the same implementation, although this review did not
independently rerun it or establish an eligible exact-HEAD evidence bundle. Archived native CI
JSONs include passing store-contract tests and failed process-soak results. Neither those failures
nor the report alone establishes their root cause. HEAD removes the native workflow's schedule;
manual dispatch remains. See G5.

## 1. Current architecture

| Mode (P1 / IMPLEMENTED) | Persistence and execution boundary |
|---|---|
| Embedded | In-process handles/library; a run may be ephemeral. Persistent local checkpoints use an exclusive OS lock and the coordinated recovery lifecycle. |
| Single-node server | Same engine plus TOML, HTTP/WS, pgwire, telemetry and reload. Local durable checkpoints do not make an ephemeral source or sink replayable. |
| Cluster | Shared-object-store authority, process/leader leases, vnode assignment and fenced shuffle. Admission is deliberately narrower; whole-node local state cannot silently become distributed state. |

Evidence: [runtime mode](../crates/laminar-db/src/pipeline_lifecycle/runtime_launch.rs),
[DB configuration](../crates/laminar-db/src/config.rs),
[server configuration](../crates/laminar-server/src/config/mod.rs), and the admission paths below.

| Finding | Classification | What actually executes |
|---|---|---|
| A1 — compute ownership | P1 / IMPLEMENTED | One `StreamingCoordinator` task runs on a dedicated `laminar-compute` thread with a current-thread Tokio runtime. Connector tasks, control services, checkpoint persistence, and publication run on the main runtime. This is not a thread-per-core Reactor/WAL engine. [Runtime launch](../crates/laminar-db/src/pipeline_lifecycle/runtime_launch.rs), lines 91–134. |
| A2 — Arrow/SQL path | P1 / IMPLEMENTED | Bounded source messages feed a cycle that applies late-row/event-time decisions, routes by vnode, executes managed operators or cached DataFusion plans, publishes stream/MV/sink output, then settles processed cursors. Cached projection and aggregate paths exist. DataFusion target partitions is deliberately one because cached repartition plans cannot simply be reused between cycles. [Cycle](../crates/laminar-db/src/pipeline/streaming_coordinator/cycle.rs), [graph execution](../crates/laminar-db/src/operator_graph/mod.rs), [session configuration](../crates/laminar-sql/src/datafusion/mod.rs). |
| A3 — scheduling and backpressure | P1 / PARTIAL | Default input capacity 64 messages; bounded drains, per-port batch limits, an 8 ms query budget, and optional source isolation. Budget checks occur between operators, not during an individual operator's work. Default per-port byte cap is absent. Slow sink enqueue and a large operation can still delay siblings and barriers. Default Backpressure is lossless; the opt-in shedding policy has the separate P0 in G9. [Pipeline configuration](../crates/laminar-db/src/pipeline/config.rs), [execution](../crates/laminar-db/src/pipeline/streaming_coordinator/execution.rs), graph lines 3506–3570. G2/G5. |
| A4 — state ownership | P1 / IMPLEMENTED | Concrete per-vnode maps/buffers hold managed aggregate, window, and join state in RAM. The default managed-state limit is 256 MiB, with operator-specific growth preflights and terminal failure on rejected growth. Object-store checkpoints are durability; there is no local durable working-state database or spill backend. [Configuration](../crates/laminar-db/src/config.rs), [aggregate accounting](../crates/laminar-db/src/aggregate_state/accounting.rs), graph lines 1834–1860. |
| A5 — persistence boundary | P1 / IMPLEMENTED | Immutable state frames, participant manifests, byte-range/digest validation, pipeline identity, source positions/frontiers, assignment, and predecessor bind one committed cut. External sink continuation follows the durable commit decision and is recoverably reconciled. [Checkpoint attempt](../crates/laminar-db/src/checkpoint_coordinator/attempt.rs), [sink protocol](../crates/laminar-db/src/checkpoint_coordinator/sink_protocol.rs), [recovery](../crates/laminar-db/src/recovery_manager/mod.rs). |
| A6 — cluster authority | P1 / IMPLEMENTED | Chitchat or static discovery discovers nodes; renewable leader/process leases and CAS-published object-store assignment authorize them. Gossip alone does not authorize ownership. gRPC/Arrow Flight carries assignment-fenced shuffle. Live reassignment stages committed donor vnode frames under the rotation fence before publishing authority. [Control](../crates/laminar-core/src/cluster/control/), [rebalance](../crates/laminar-db/src/rebalance/), [assignment adoption](../crates/laminar-db/src/db/mod.rs), lines 2814–4222. |

The coordinator graph is shared by pipelines within a DB. More SQL pipelines or local vnodes do
not imply independent compute workers. Cluster distribution gives inter-process parallelism;
separate DB instances can have separate compute threads. Source isolation and graph deferral
provide useful fairness, but do not make expensive user code or a long operator preemptible.

RecordBatch cloning in fan-out generally shares Arrow buffers; it is not evidence of a full data
copy. SQL output construction, grouping keys, MV scalar materialization, and checkpoint encoding
do allocate. Graph execution deliberately holds a rotation read guard across async work to protect
the assignment invariant. MV/subscription publication also takes locks. Whether these costs are
material requires allocation and contention profiles; deleting ownership fences to remove a lock
would be an unsafe optimization. External I/O separation is implemented, not proof that every
operator is allocation-free or nonblocking.

## 2. Verified capabilities and restrictions

| Finding | Classification | Evidence and deployment boundary |
|---|---|---|
| C1 — aggregates | P1 / IMPLEMENTED | Managed keyed state, dirty emission, vnode checkpoint/restore, state accounting, and high-cardinality admission exist. The one-million-group ceiling and byte budget bound admitted growth; a lifetime non-windowed GROUP BY does not become bounded by automatic TTL. Evicting its keys would change SQL results. [Aggregate state](../crates/laminar-db/src/aggregate_state/mod.rs), especially growth validation near line 1716. |
| C2 — event time and windows | P1 / IMPLEMENTED | Late rows are checked against the prior watermark before the current batch advances it. Managed direct-source TUMBLE/HOP/SESSION windows support final/window-close emission, including cluster mode. Watermarks, idle-input handling, closed-window state cleanup, replay/frontier state, and checkpoint round trips exist. [Cycle](../crates/laminar-db/src/pipeline/streaming_coordinator/cycle.rs), [window state](../crates/laminar-db/src/core_window_state/mod.rs), [cluster checks](../crates/laminar-db/src/ddl/cluster_checks.rs), lines 349–444. |
| C3 — bounded joins | P1 / IMPLEMENTED | One bounded watermarked interval-join stage and managed direct-source temporal ASOF execution are admitted in cluster. Interval joins preflight input/output growth; watermarks free history. Temporal history uses finite retention and preserves needed predecessors; old revival probes and source-order regressions fail explicitly. Named join output can feed a separate keyed aggregate. [Interval join](../crates/laminar-db/src/interval_join/mod.rs), [temporal state](../crates/laminar-db/src/temporal_join_state/mod.rs). |
| C4 — fault/recovery invariants | P1 / IMPLEMENTED | Selected-cut corruption is fatal; recovery does not choose an older convenient checkpoint. Prepared sink artifacts are settled, manifests and frame digests validated, then exact state restored. Generation-bound Prepare/Start/Release rounds keep intake fenced; durable terminal faults survive process/leader changes. [Recovery authority](../crates/laminar-db/src/checkpoint_coordinator/recovery.rs), [coordinated recovery](../crates/laminar-db/src/coordinated_recovery/mod.rs), [lifecycle recovery tests](../crates/laminar-db/src/pipeline_lifecycle/). |
| C5 — local tables/MVs | P1 / PARTIAL | Reference-table snapshots and aggregate/append/upsert/multiset MVs exist and recover locally. Upsert and multiset cycles stage validation before mutation; multiplicity underflow/overflow is rejected. Append MVs evict older batches (default 1,000 batches/256 MiB), but retain at least one batch even if it is oversized. Live keyed table/MV growth is not comprehensively quota-controlled. [Table store](../crates/laminar-db/src/table_store/mod.rs), [MV store](../crates/laminar-db/src/mv_store/mod.rs), G3. |
| C6 — local subscriptions | P1 / IMPLEMENTED | Shared byte-bounded logs, a process budget, reader cap (64 per object), monotonically checked sequence numbers, epoch markers, retained replay, explicit lag/pruning errors, and generation invalidation exist. A slow reader can be closed; it is not an unbounded per-client queue. [Registry](../crates/laminar-db/src/subscription/registry/), [portal](../crates/laminar-db/src/subscription/portal/mod.rs). |
| C7 — durable cluster subscriptions | P1 / IMPLEMENTED | Checkpointed output segments, per-partition sequence continuity, distribution certificates, retention pins, digest verification, and AS-OF replay exist. Output is exposed from committed cuts. The runtime only admits certified non-windowed managed keyed-aggregate streams; other SQL support does not imply subscription support. [Admission](../crates/laminar-db/src/db/cluster_subscription.rs), [reader authority](../crates/laminar-db/src/subscription/cluster/reader/authority.rs), [output state](../crates/laminar-db/src/subscription/cluster/output_state.rs). |
| C8 — operations | P1 / IMPLEMENTED | Prometheus cycle/operator/checkpoint/recovery/state/subscription metrics, tracing, health/readiness, configuration validation, secret redaction, bounded connector shutdown, HTTP/WS, and pgwire exist. Readiness checks serving authority and running state, not business freshness. Pgwire rejects remote trust and supports MD5/TLS/mTLS. [Metrics](../crates/laminar-db/src/engine_metrics.rs), [HTTP ops](../crates/laminar-server/src/http/ops.rs), [pgwire](../crates/laminar-server/src/pgwire/mod.rs). G1/G4/G7 qualify operational readiness. |
| R1 — cluster SQL/state scope | P2 / INTENTIONALLY-UNSUPPORTED | Fused join-and-aggregate, windowed joins, inline arbitrary multiway joins, cluster MVs, and whole-node reference state without a distributed lifecycle remain rejected. `n_way_join_differential.rs` tests rejection of unsupported inline shapes; its filename is not evidence of arbitrary multiway execution. Preserve these admission boundaries. [Cluster checks](../crates/laminar-db/src/ddl/cluster_checks.rs), [source contracts](../crates/laminar-db/src/pipeline_lifecycle/source_contracts.rs). |
| R2 — unsupported CDC route | P1 / PARTIAL | PostgreSQL and MongoDB have substantial connector/driver code, but both production source `contract()` implementations reject raw envelopes until canonical primary-key row/delete semantics exist. They are not admitted end-to-end CDC sources. [Postgres contract](../crates/laminar-connectors/src/postgres/cdc/source/lifecycle.rs), [MongoDB contract](../crates/laminar-connectors/src/mongodb/source/lifecycle.rs), [admission tests](../crates/laminar-connectors/tests/cdc_admission.rs). G6; implementation extension is deferred below. |
| R3 — consumption contract | P2 / INTENTIONALLY-UNSUPPORTED | Tail attaches at the current sequence/committed frontier; it is not an atomic initial SELECT snapshot plus live feed. Local AS-OF needs retained in-process history and is not crash-durable replay. Cluster order is per partition, not a total global row order. Reconnect can replay effects since the client's last durable progress boundary. [Local attach](../crates/laminar-db/src/subscription/registry/mod.rs), lines 392 onward; [cluster attach](../crates/laminar-db/src/subscription/cluster/reader/authority.rs). |
| R4 — experimental lakehouse surfaces | P2 / EXPERIMENTAL | Feature names do not certify every cloud/catalog path. `iceberg-azure` is experimental; Delta's Unity feature uses an upstream experimental catalog. Iceberg MOR/COW, changelog reads, non-main writes, and automatic schema evolution are explicitly rejected by capability validation. [Features](../crates/laminar-connectors/Cargo.toml), [Iceberg capabilities](../crates/laminar-connectors/src/lakehouse/iceberg/capabilities.rs). |
| R5 — Delta source integration | P1 / PARTIAL | The Delta reader exists, but its `Ephemeral/FullChangelog` contract leaves row positions unavailable. The ordinary route rejects mutations, temporal-right rejects full changelogs, and bounded interval routing requires ordered deterministic positions. It therefore does not establish an admitted streaming source-to-sink composition. [Delta source](../crates/laminar-connectors/src/lakehouse/delta_source/mod.rs), line 181; [route contracts](../crates/laminar-db/src/pipeline_lifecycle/source_contracts.rs), lines 295–325 and 375–389; [ordinary route](../crates/laminar-db/src/pipeline/streaming_coordinator/mod.rs), line 145. |

Existing tests cover join NULL/composite-key/outer/semi semantics, atomic out-of-order rejection,
hot-key preflight, history cleanup, checkpoint capture immutability, corrupt cardinality, temporal
tombstones/predecessors, pipeline identity mismatches, stale assignment, fencing, and cancellation.
See the private suites beside `interval_join`, `temporal_join_state`, `core_window_state`,
`checkpoint_coordinator`, `coordinated_recovery`, and `subscription/cluster`. These are meaningful
correctness checks, not substitutes for a full workload oracle under network/process faults.

For result consumers, checkpoint progress is the safe recovery boundary, not a WS delivery
sequence alone. The client must durably couple its effects and remembered progress to avoid its
own duplicates. Incremental MVs deliver consolidated plain snapshots, while some stream/sink
paths carry weighted changes. Pgwire continuous SUBSCRIBE requires the supported portal/cursor
flow; a simple unbounded query is rejected. WS has frame/control-size limits, connection caps,
write deadlines, and heartbeat handling. None of these constitutes an end-to-end transactional
contract with an arbitrary client application.

## 3. Delivery semantics: source through external sink

Except for the **P0 / PARTIAL** shedding override below, entries are **P1** findings. Admitted
contracts are IMPLEMENTED; excluded combinations
are INTENTIONALLY-UNSUPPORTED unless marked PARTIAL. Production-load verification is separately
IMPLEMENTED-BUT-UNVERIFIED (G5). Here **exactly-once certified** means the repository's private
certification/admission flag plus its coordinated protocol, not a new certification granted by
this review. **Exactly-once candidate** means plausible reuse of checkpoint machinery but admission
still rejects it. Candidate is never a deployable delivery setting.

### Source classes

| Source configuration | Local embedded/single-node contract | Cluster contract |
|---|---|---|
| Kafka, ordinary append records | Replayable; exact-source certified. ALO or exact with suitable sinks/checkpoints. | Splittable and admitted; the currently supported replayable cluster source. |
| Kafka Debezium/keyed mutations | Same replay certification; requires declared primary key and an admitted ordered mutation-aware operator path. Ordinary append routing is not generally valid. | Same additional plan restrictions; certification does not turn arbitrary CDC SQL into a supported query. |
| Deterministic generator | Replayable, exact-source certified; useful deterministic test source. | Unsupported singleton placement. |
| Files, local-only | Replayable inventory/partial-file cursor; ALO. EO candidate, rejected for lack of source certification. Recovery also needs the referenced files to remain available and valid. | Unsupported singleton placement. |
| Iceberg append read | Replayable snapshot/file progress; ALO. EO candidate, rejected for lack of source certification. | Unsupported singleton placement. |
| Iceberg snapshot read | Ephemeral; best effort only. Changelog read is rejected. | Unsupported. |
| Delta source | Unsupported streaming composition (PARTIAL): reader is ephemeral/full-changelog, has no recovery resume, and lacks the ordered positions needed to pass an admitted mutation route. | Unsupported. |
| NATS Core or JetStream source | Ephemeral, including JetStream: acknowledgement is not a rewindable LaminarDB checkpoint cursor. Best effort only. | Unsupported singleton placement. |
| WebSocket / OTEL source | Ephemeral; best effort only. OTEL node-local ingress is not a cluster rebalance contract. | Unsupported. |
| PostgreSQL CDC / MongoDB CDC | Unsupported production source admission (PARTIAL implementation), even though lower-level driver tests exist. | Unsupported. |
| In-process pushed batches | Useful embedded ingress; no independent external durable replay guarantee is certified by a successful push. | Does not acquire cluster-source certification. |

Evidence: connector `contract()` methods in `kafka/source/lifecycle.rs`, `generator/mod.rs`,
`files/source.rs`, `lakehouse/{iceberg_source,delta_source}/mod.rs`, `nats/source/mod.rs`,
`websocket/source/mod.rs`, `otel/source/mod.rs`, and the CDC lifecycle files above, all under
`crates/laminar-connectors/src/`. The controlling engine checks are
[source admission](../crates/laminar-db/src/pipeline_lifecycle/source_admission.rs), lines 11–94,
and [source plan contracts](../crates/laminar-db/src/pipeline_lifecycle/source_contracts.rs).

### Sink classes

| Sink configuration | External durability contract | Cluster placement / exact restriction |
|---|---|---|
| Kafka append | Durable ALO. Idempotent producer and `acks=all` do not atomically couple engine checkpoint positions with output. | Multiwriter ALO; EO unsupported. Kafka upsert/changelog variants are singleton. |
| PostgreSQL append / MongoDB insert | Durable ALO; replay can repeat external effects. | Multiwriter. Upsert/changelog/CdcReplay variants are singleton and require matching input mutation semantics. |
| NATS JetStream / NATS Core | JetStream durable ALO; Core ephemeral best effort. | Multiwriter topology alone cannot make an ephemeral sink valid for cluster ALO. |
| Local Files | Durable ALO on supported Windows/Unix file publication path, no coordinated exact cursor. | Singleton; unsupported cluster sink. |
| WebSocket client/server | Ephemeral best effort; client singleton, server node-local egress. | Unsupported for admitted cluster delivery. |
| Delta ordinary append | Durable ALO; upsert/overwrite have further input/placement restrictions. | Shared append can be multiwriter; local/upsert/overwrite are singleton. |
| Delta coordinated append | Checkpoint-committable exact path when storage/config validation succeeds. | Exact certified only for direct S3/S3A append. Custom endpoints, other clouds, Unity, and non-append modes do not inherit cluster certification. |
| Iceberg ordinary append | Durable ALO after REST/catalog/storage capability validation. | Shared supported storage can be multiwriter; local storage is singleton. |
| Iceberg coordinated append | Checkpoint-committable exact path when capability and coordinated-publication checks succeed. | Exact certified only REST catalog, direct S3/S3A, supported main/strict append, and certified no-auth/typed-bearer catalog configuration. OAuth, vended credentials, and remote signing do not inherit certification. |

Evidence: `kafka/sink/mod.rs:483`, `postgres/sink/mod.rs:657`, `mongodb/sink/lifecycle.rs:15`,
`nats/sink/mod.rs:420`, `files/sink.rs:504`, `websocket/{sink_client,sink}/mod.rs`,
`lakehouse/delta/lifecycle.rs:21`, `lakehouse/iceberg/mod.rs:364` and `iceberg/capabilities.rs`,
under `crates/laminar-connectors/src/`. All combinations also pass engine
[sink admission](../crates/laminar-db/src/pipeline_lifecycle/sink_admission.rs) and the sink checks
in `source_admission.rs`; no guarantee is inferred from a connector's marketing name.

### Composition matrix

This cross-product covers the source/sink classes above without pretending every SQL mutation
shape, credential mode, or table layout is valid. If source, sink, SQL, topology, or storage
admission rejects any part, the combination is **unsupported**. Mixed sinks must all satisfy the
selected guarantee. Replay guarantees require valid persistent checkpoint configuration.

**P0 / PARTIAL exception (G9):** the following ALO/EO classifications require a lossless overload
policy (`Backpressure`, or `Fail` with recovery). The builder currently accepts `ShedOldest` with
ALO; public `open_with_config` bypasses the builder's EO shedding rejection as well. Those
configurations can lose records while reporting successful processing; classify their actual
end-to-end behavior as **best effort**, regardless of the requested durable label. A drop counter
does not restore lost records.

| Source class | Ephemeral sink | Durable ALO sink | Coordinated exact sink |
|---|---|---|---|
| Ephemeral admitted source | Best effort | Best effort | Unsupported with exact sink contract; use the sink's ordinary mode for best effort. |
| Replayable, not exact-certified (Files/Iceberg append) | Best effort | At-least-once with durable checkpoints; best effort if explicitly chosen without that guarantee. | Exactly-once candidate; unsupported in production admission. |
| Exact-certified Kafka/generator | Best effort locally | At-least-once with durable checkpoints | Exactly-once certified within admitted local configuration; cluster only Kafka → certified Delta or Iceberg above. |
| Rejected source or plan | Unsupported | Unsupported | Unsupported |

There is no separately certified **at-most-once** end-to-end mode. Ephemeral paths can lose data,
and reconnect/retry can also duplicate data; labeling them at-most-once would promise too much.
Cluster best effort and singleton/node-local placements are rejected before connector I/O.

The exact boundary is: capture source decisions and managed state; seal/fence sink epochs; write
prepared participant/artifact descriptors; validate and durably publish the committed index and
decision; finish external commits idempotently; reconcile ambiguous completion during recovery.
A crash after external publication must be resolved using that decision and artifact identity,
not by assuming another append is harmless. ALO sinks can publish before the checkpoint commits
and publish again after replay. Multiple exact sinks share a recoverable decision, but do not
provide simultaneous atomic visibility to unrelated external readers.

## 4. Documentation disagreements

| Finding | Classification | Claim versus implementation |
|---|---|---|
| D1 | P1 / STALE-DOCUMENTATION | [Root README](../README.md), lines 390–391, and [connector README](../crates/laminar-connectors/README.md), lines 12–13, advertise PostgreSQL/MongoDB source modes that current `contract()` rejects. Driver capability is not engine admission. |
| D2 | P2 / STALE-DOCUMENTATION | Root README near line 172 says cluster windowed aggregation is rejected; managed direct-source final TUMBLE/HOP/SESSION admission exists in `ddl/cluster_checks.rs`. Broadening that statement to arbitrary windows would also be wrong. |
| D3 | P2 / STALE-DOCUMENTATION | [Server README](../crates/laminar-server/README.md), near line 310, describes thread-per-core/reactor allocation; runtime launch creates one current-thread compute runtime. |
| D4 | P0 / PARTIAL | Root README cluster example near line 187 uses `0.0.0.0:8080` without a console token. The comment that a missing token is for loopback/dev is not enforced. G1. |
| D5 | P2 / DOCUMENTATION-ONLY | Cloud-support documentation records local Azure certification and native-job availability, but the exact-HEAD native evidence bundle was not established here. Keep provider support, local historical results, CI status, and exact-delivery admission distinct. G5. |

The NATS payments example correctly declares best effort despite a durable lakehouse sink.
The Binance cluster TOMLs explicitly identify themselves as archived, rejected contract fixtures;
they are not evidence of supported cluster WebSocket ingress. `compatibility.json` is generated by
the release workflow and describes release/SDK metadata; its older version alone does not prove a
checkpoint ABI bug. No recommendation to overwrite it from workspace version is made.

## 5. Production gap register and implementation plan

### G1 — refuse accidental unauthenticated remote HTTP

**P0 / PARTIAL. Modes:** single-node and cluster servers exposed to an untrusted network.

- **Problem/evidence:** `validate_config` accepts a remote HTTP bind without `console_token`;
  middleware then calls the protected route unconditionally. SQL, reload, start/stop and checkpoint
  actions share that boundary. The test `remote_cluster_plaintext_is_accepted` demonstrates the
  configuration shape. Default loopback binding is safer, but the README example defeats it.
  [Validation](../crates/laminar-server/src/config/validation.rs), lines 29–43;
  [authentication](../crates/laminar-server/src/http/auth.rs), lines 158–187;
  [router](../crates/laminar-server/src/http/router.rs), lines 43 onward.
- **Reuse:** existing bind parsing, constant-time token validation, diagnostics-token rules,
  config errors, route tests, and explicit pgwire remote-access policy.
- **Smallest solution:** reject a non-loopback HTTP bind unless a console token is configured;
  fix runnable examples; document TLS termination at the trusted proxy and network isolation for
  public health/metrics. Do not silently treat cluster mTLS as HTTP protection or add an identity
  provider framework to solve a missing startup guard.
- **Tests:** IPv4/IPv6 wildcard/remote rejection, token-enabled acceptance, loopback development,
  missing/wrong/duplicate token denial for every administrative route and WS upgrade, diagnostic
  token scope, and runnable example validation.
- **Acceptance:** no token-free non-loopback admin listener starts; unauthorized mutations return
  401 when serving is open, preserving earlier 503 responses while startup/recovery is fenced.
  HTTP and cluster/pgwire TLS boundaries are explicit in examples. Helm quickstarts must supply
  credentials through the existing secret wiring rather than an insecure default token.

### G2 — bound ingress and DataFusion working memory

**P1 / PARTIAL. Modes:** all.

- **Problem/evidence:** 64 queued batches is not a byte bound. Optional per-port byte limits default
  to `None`; the managed-state budget does not cover arbitrary Arrow batches, DataFusion temporary
  memory, output, and all checkpoint copies. Session creation does not install a bounded
  DataFusion memory pool. A few wide batches or an expansive query can exhaust process memory
  while managed state remains within 256 MiB. [Pipeline config](../crates/laminar-db/src/pipeline/config.rs),
  [runtime defaults](../crates/laminar-db/src/pipeline_lifecycle/runtime_launch.rs), lines 641–669;
  [DataFusion setup](../crates/laminar-sql/src/datafusion/mod.rs), lines 100–169.
- **Reuse:** existing byte-cap checks, typed terminal faults, source backpressure, connector writer
  limits, checkpoint staging budgets, and DataFusion 53.1.0 `RuntimeEnvBuilder::with_memory_limit`
  / `GreedyMemoryPool`. The pinned upstream implementation already provides this API.
- **Smallest solution:** enforce a byte limit before queue ownership transfers, provide documented
  finite defaults for existing buffer controls, and install a bounded pool for DataFusion consumers
  that participate in fallible reservation accounting. Share the intended pool with the separately
  created connector-graph context as well as the main DB context; disable compute-path disk spilling
  explicitly. Cover the separate embedded `Source::push_arrow` ring as well as the shared
  connector-to-coordinator channel. Document a process memory envelope including non-pool Arrow
  buffers and measured overhead; a pool alone is not an RSS cap. Use explicit overload errors or
  backpressure, not silent dropping or an unrequested spill subsystem.
- **Tests:** oversized variable-width batch, slow sink, fan-out, several active pipelines, large
  DataFusion intermediate, cancellation during overload, and recovery after rejection. Verify
  refused input is not acknowledged as processed.
- **Acceptance:** each ingress queue has a configured byte maximum; a batch above the maximum is
  rejected before enqueue; fallible DataFusion reservations reject growth at their configured
  limit. Unconditional pool growth and non-pool allocations are not claimed to be hard-capped.
  A one-hour stress run stays inside a declared RSS envelope and produces no silent loss.
  Relevant hot-path benchmarks before/after must meet the repository's 5% regression rule,
  with allocation/IPC profiles recorded.

### G3 — quota live reference tables and keyed MVs

**P1 / PARTIAL. Modes:** embedded and single-node; keep cluster rejection.

- **Problem/evidence:** `TableStore::upsert` inserts each new key and a one-row `batch.slice` without
  a live row/byte quota; a surviving slice can retain a large backing array. MV upsert/multiset maps
  also grow without a live quota. Multiset's count-times-32 estimate is not variable-width key
  accounting. Snapshot materialization and checkpoint-capture caps happen too late to bound the
  retained maps. Append mode's minimum-one-batch rule admits one oversized batch.
  [Table insertion](../crates/laminar-db/src/table_store/mod.rs), lines 751–778;
  [table rows](../crates/laminar-db/src/table_rows.rs);
  [MV update](../crates/laminar-db/src/mv_store/mod.rs), lines 161, 282–319, 456–513.
- **Reuse:** atomic staged MV deltas, existing checkpoint retained-buffer accounting, managed-state
  growth-preflight patterns, Arrow buffer sharing, and current row/byte snapshot safeguards.
- **Smallest solution:** enforce per-object retained row/byte quotas before applying a cycle or
  table refresh; account encoded keys and owned scalar data. Reject an oversized append batch
  explicitly. Measure slice amplification and compact retained rows only where that measurement
  justifies the copy. Checkpoint estimates are not a drop-in exact live-memory counter. Preflight
  quota failures across all affected MVs before publishing any of their updates. Do not evict
  live SQL keys as if they were a cache.
- **Tests:** unique-key growth, wide keys, replacements/deletes, an oversized single batch, atomic
  failed refresh, snapshot restore over the limit, and one surviving row from a large old batch.
- **Acceptance:** all four live storage modes remain within declared quotas; rejected updates leave
  the prior result intact; retained bytes return to their expected bound after delete/replace;
  checkpoint and live limits agree; query results match an independent reference. Changes to cycle
  publication also need the hot-path benchmark/profile gate described in G2.

### G4 — expose real Kafka lag and separate health from freshness

**P1 / PARTIAL. Modes:** Kafka deployments in all modes.

- **Problem/evidence:** Kafka metrics include polls, records, bytes, errors, commits, and rebalances,
  but no consumer-lag metric or statistics callback. The dashboard queries
  `laminardb_kafka_source_consumer_lag`, which is not registered by the Kafka metrics implementation.
  Startup watermark fetches are not continuous lag reporting. Readiness can remain green while a
  running pipeline falls behind. [Kafka metrics](../crates/laminar-connectors/src/kafka/metrics.rs),
  [dashboard](../grafana/laminardb.json), near line 222; [readiness](../crates/laminar-server/src/http/ops.rs).
- **Reuse:** existing metrics registry, assigned-partition positions, librdkafka statistics or
  bounded background watermark queries, checkpoint timestamps, and watermark metrics.
- **Smallest solution:** collect processing lag and committed-recovery lag off the compute thread,
  expose last-successful-poll/checkpoint ages, and repair dashboard queries. Keep liveness separate
  from freshness alerts; do not make a busy source trigger endless process restarts.
- **Tests:** paused consumer, broker failure, committed checkpoint lag, rebalance/partition removal,
  bounded label cardinality, and a scraped-metric fixture exercised by dashboard queries.
- **Acceptance:** every dashboard expression references an emitted metric; pausing consumption
  while broker writes continue shows increasing lag within two configured collection intervals;
  removed assignments disappear;
  checkpoint staleness is alertable without marking a fenced process ready.

### G5 — qualify a workload, not just a benchmark name

**P1 / IMPLEMENTED-BUT-UNVERIFIED. Modes:** qualify embedded, single-node and cluster separately.

- **Problem/evidence:** correctness soaks and latency histograms exist, but they do not establish a
  universal production SLO. Hosted hot-path checks run in `observe` mode; Criterion alerts are
  nonblocking and use a 150% threshold; the native cloud workflow is manual at HEAD. The local
  Azure report is bounded historical evidence. See the performance section below.
- **Reuse:** `checkpoint-fault-soak.yml`, native object-store contracts, server process-kill harnesses,
  independent external-sink readers, temporal/window/interval join oracles, Criterion benches, and
  the existing evidence artifact schema. Keep scaffold-only evidence ineligible.
- **Smallest solution:** select one realistic workload per intended launch composition and run a
  repeatable release-build qualification on named hardware. Capture arrival-to-external-visibility
  latency and queue/RSS growth alongside compute time. Add p99.9 measurement using an existing
  histogram implementation. Keep hosted correctness smoke tests; certify latency only on controlled
  hardware. Archive exact SHA, features, lockfile, configuration, seed, hardware, load and raw results.
- **Tests:** at least one-hour steady runs, one/four concurrent pipelines, uniform/Zipf/hot-key data,
  slow/failed sink, source pause, checkpoint under load, process/leader kill, rejoin, scale-out/in,
  and replay pruning. Run broker/lakehouse/cloud tests against their real boundaries, not only mocks.
- **Acceptance:** declared p50/p95/p99/p99.9 and recovery/checkpoint ceilings pass every certified
  scenario; queues stop growing below the advertised capacity; RSS plateaus; independent oracles
  show no missing ALO records and zero duplicate committed effects for exact compositions. Repeat
  three times. Hot-path changes exceeding 5% regression need explanation/removal before landing.

### G6 — publish the actual supported surface

**P1 / STALE-DOCUMENTATION. Modes:** all.

- **Problem/evidence:** D1–D3 make both false-positive and false-negative support claims. Users can
  choose a CDC design that fails before I/O or reject a window design that is already implemented.
- **Reuse:** typed source/sink contracts, pre-I/O admission tests, existing examples, and cluster SQL
  checks. No second capability registry is needed.
- **Smallest solution:** correct the three README statements and maintain a small mode/SQL/delivery
  table with runnable examples and links to the relevant contract tests. Describe local versus
  durable cluster subscription cursors, the absence of atomic snapshot-plus-tail attachment, and
  Delta reader capability versus streaming route admission.
- **Tests:** parse/validate runnable configs; assert advertised supported combinations pass admission
  and explicitly unsupported combinations reject before opening connectors. Keep archived negative
  fixtures clearly marked.
- **Acceptance:** no published claim contradicts the reviewed contract; every positive example has
  a named validation test; no CDC or exact-delivery claim is inferred from a Cargo feature alone.

### G7 — qualify upgrades and publish the stop/restart boundary

**P1 / IMPLEMENTED-BUT-UNVERIFIED. Modes:** persistent deployments; cluster is most sensitive.

- **Problem/evidence:** state/partition ABI hashes and compatibility rejection are implemented.
  The inspected recovery tests predominantly restore using the same executable; no cross-release
  upgrade qualification was found in the inspected workflows. Legacy barrier acknowledgements are
  deliberately not capture acknowledgements without mixed-version negotiation. Rolling two
  arbitrary versions is therefore not established as safe. [Pipeline identity](../crates/laminar-db/src/pipeline_identity/mod.rs),
  [barrier protocol](../crates/laminar-core/src/cluster/control/barrier/protocol.rs), near line 430.
- **Reuse:** exact checkpoint identity validation, immutable cuts, serving fences, graceful shutdown,
  process-soak binaries, and corruption/incompatibility tests.
- **Smallest solution:** document supported stop/checkpoint/upgrade/restart and rollback procedures;
  add a previous-supported-release checkpoint fixture test. Declare unsupported mixed-version
  rolling operation explicitly until a tested compatibility pair exists. Do not add a generic state
  migration framework before an actual incompatible transition requires one.
- **Tests:** old binary writes a committed cut, new binary restores and continues; incompatible cut
  fails before intake; rollback cannot reuse mutated authority; lost leadership during transition
  remains fenced. Test mixed versions only for pairs the release intends to support.
- **Acceptance:** each release lists tested predecessor versions and matching ABIs; compatible pairs
  preserve source/output continuity; incompatible pairs produce an actionable error with no sink
  writes; rollback/runbook is rehearsed with retained immutable checkpoint data.

### G8 — make dependency security checks enforceable

**P1 / PARTIAL. Modes:** all shipped artifacts.

- **Problem/evidence:** `.github/workflows/ci.yml:415–435` runs `cargo audit` and `cargo deny` with
  `continue-on-error: true`; the `ci-success` dependency list omits both. Those jobs therefore do
  not prevent the workflow's aggregate success when their checks fail. This establishes a gate
  weakness, not a claim that this lockfile contains a particular vulnerability. External branch
  protection was not inspected.
- **Reuse:** the existing audit/deny jobs, `deny.toml`, lockfile and dependency-update workflow.
- **Smallest solution:** triage present findings, document narrowly scoped accepted exceptions, then
  require the audit/deny jobs for the release gate. Use the existing maintained tools; do not write
  another dependency scanner.
- **Tests:** run both tools against the release lockfile and current advisory database; exercise a
  failing audit/deny result in a nonpublishing workflow validation and verify aggregate failure.
- **Acceptance:** no unreviewed security advisory passes the release gate, exceptions have owners
  and review dates, and an audit/deny failure cannot report aggregate CI success.

### G9 — reject lossy overload policy at every durable construction boundary

**P0 / PARTIAL. Modes:** library-configured embedded, single-node or cluster execution when
`ShedOldest` and a durable guarantee are combined. Builder ALO is affected; direct local
`open_with_config` also bypasses the builder's EO guard. Default server configuration uses
lossless Backpressure.

- **Problem/evidence:** `LaminarDbBuilder::validate_backpressure` rejects shedding only for EO.
  It is called only by `build`; public `LaminarDB::open_with_config` reaches the shared constructor
  without that check, and runtime launch copies its policy unchanged.
  `OperatorGraph::shed_to_cap` discards old batches; `record_shed` only increments a counter and
  returns success. Normal output publication then settles source cursors; shedding is neither a
  failed-source nor a deferred-source outcome. A later checkpoint can therefore preserve progress
  past rows that never reached the sink. The passing unit test
  `test_shed_oldest_policy_drops_rows_and_increments_counter` verifies successful execution with
  dropped rows; builder tests cover EO rejection but not ALO rejection. This review establishes
  the admission hole and dropping behavior from code/tests, not a fresh broker-loss experiment.
  [Builder](../crates/laminar-db/src/builder/mod.rs), lines 738–767;
  [public/shared constructors](../crates/laminar-db/src/db/mod.rs), lines 1685–1748;
  [runtime policy transfer](../crates/laminar-db/src/pipeline_lifecycle/runtime_launch.rs), line 667;
  [shedding](../crates/laminar-db/src/operator_graph/mod.rs), lines 1660–1694 and 1736–1745;
  [test](../crates/laminar-db/src/operator_graph/tests.rs), line 6480;
  [cursor settlement](../crates/laminar-db/src/pipeline/streaming_coordinator/cycle.rs), lines 357–378.
- **Reuse:** the existing builder validation, `DeliveryGuarantee` enum, lossless Backpressure/Fail
  policies, shedding metric, and source-cursor recovery tests.
- **Smallest solution:** admit `ShedOldest` only with BestEffort, move validation to the shared
  construction boundary used by both public entry points, and correct the error message. Preserve
  the best-effort policy; do not add a second replay protocol to make intentional dropping look durable.
- **Tests:** table-driven BestEffort/ALO/EO admission through builder/configured startup; existing
  best-effort shedding test; durable Backpressure/Fail saturation followed by checkpoint/restart
  with an independent source/output ledger.
- **Acceptance:** every ALO/EO plus shedding configuration fails before connector I/O; best-effort
  shedding still reports drops; lossless durable overload/recovery tests show no missing records.

### Priority order

1. **G1:** close remote HTTP exposure and fix that deployment example.
2. **G9:** reject lossy shedding for ALO/EO through both public construction paths.
3. **G6:** correct capability claims immediately; this is small and prevents incorrect deployments.
4. **G2:** bound ingress/DataFusion working memory using existing controls and upstream APIs.
5. **G3:** add atomic live table/MV quotas; retain their current SQL semantics.
6. **G5:** certify the intended workload and exact delivery composition with archived evidence.
7. **G4:** complete Kafka freshness telemetry; land before operational acceptance of G5.
8. **G8:** make dependency security checks enforceable before release qualification.
9. **G7:** qualify the first supported upgrade pair before promising unattended upgrades.

G2 and G3 are separate changes because queue ownership and stored SQL results have different
failure/rollback rules. G4 is a prerequisite for interpreting the Kafka qualification, and G8 must
precede release acceptance, even though G5 determines the workload and required measurements.
This priority list describes importance; the [execution plan](production-hardening-plan.md)
provides dependency order, including early workload baselines and security/telemetry prerequisites.
No broad implementation was started here.

## 6. Performance evidence and risks

| Finding | Classification | What evidence proves / does not prove |
|---|---|---|
| P-A — microbenchmarks | P1 / IMPLEMENTED-BUT-UNVERIFIED | `core/benches/latency_bench.rs` measures a window assigner, not source-to-durable-sink latency. DB stream-executor, recovery and operator benches measure useful components. `tests/perf_handle.rs` prints a small push measurement. None establishes a one-hour production workload's p99.9, RSS, or queue stability. |
| P-B — fault soaks | P1 / IMPLEMENTED | Server `cluster_soak.rs` and `iceberg_cluster_soak.rs`, connector lakehouse tests, and checkpoint-fault workflows contain real-process recovery and independent output assertions. They cover skew, retained-state floors, joins, windows, subscriptions and failures. Their existence is substantial evidence of engineering, but successful execution of a different SHA/configuration cannot certify this deployment. |
| P-C — latency gate strength | P1 / PARTIAL | `.github/workflows/bench.yml` uses short Criterion samples, nonblocking alerts and 150% threshold; feature-gated cluster benches are not automatically exercised by no-default-feature commands. Checkpoint fault profiles set `LAMINAR_SOAK_HOT_SLO_MODE=observe`; latency summaries include p50/p95/p99 bucket bounds, not a demonstrated p99.9 contract. G5. |
| P-D — correlated stalls | P1 / PARTIAL | One compute runtime, between-operator time budgets, shared-source routing and awaited sink enqueue allow a costly operator or backpressure to affect siblings. `shared_source_isolation.rs` tests sibling survival with isolation and starvation without it. Preserve this working mechanism and quantify limits before changing scheduling. G2/G5. |
| P-E — allocation/retention | P1 / PARTIAL | Arrow fan-out is mostly shallow; row-key conversion, owned scalar MVs, checkpoint capture, and output materialization are real costs. Live/prepared/retired state metrics are not RSS, and shared sliced arrays complicate accounting. No profile in this review proves which allocation is the dominant cost. G2/G3/G5. |
| P-F — recovery liveness | P1 / IMPLEMENTED-BUT-UNVERIFIED | Timeouts and bounded recovery rounds exist; cloud latency, source replay and external sink reconciliation determine real recovery time. Hosted native-soak failures and a local 90-second-ceiling report do not justify the old sub-10-second microbenchmark target for every deployment. G5. |

Required qualification dimensions are sustained arrival/output throughput; p50/p95/p99/p99.9;
source and sink queue slopes; RSS plus retained/prepared/retired bytes; independent correctness;
checkpoint capture/stall/durable/external-publication times; recovery backlog catch-up; and
multi-pipeline interference. Measure both processing visibility and committed visibility: final
windows inherently wait for watermarks, and exact sink/subscription visibility depends on the
checkpoint/publication cadence. A sub-microsecond operator result cannot remove those waits.

## 7. Correctness and operational risks

| Finding | Classification | Deployment consequence and existing protection |
|---|---|---|
| O1 — guarantee overreach | P1 / PARTIAL | Documentation can encourage unsupported CDC/exact compositions (G6). Actual typed admission is fail-closed; keep it. Sink idempotence or durable operator state alone is insufficient. |
| O2 — state-limit failure | P1 / PARTIAL | Managed operators reject growth; other live state can exhaust memory (G2/G3). Rejecting a valid workload preserves correctness but is still an availability failure needing an explicit capacity envelope. Do not solve it with silent row dropping or TTL on lifetime aggregates. |
| O3 — replay and retention | P1 / IMPLEMENTED | Source retention, checkpoint artifacts, and subscription replay history must outlive the intended recovery interval. Pruned replay, corrupt frames, incompatible identity and sequence gaps produce errors. Consumers need an explicit recovery policy; no fallback-to-older-cut promise should be added. |
| O4 — control security | P0 / PARTIAL | Optional HTTP auth is the demonstrated remote exposure (G1). HTTP TLS termination is external; pgwire and cluster TLS are independent. A token protects one administrative trust domain, not tenant/object-level authorization. |
| O5 — freshness blindness | P1 / PARTIAL | Existing readiness is authority/lifecycle readiness. It does not establish source progress, recent checkpoints, or external-sink freshness; dashboard Kafka lag is incomplete (G4). |
| O6 — upgrade safety | P1 / IMPLEMENTED-BUT-UNVERIFIED | ABI rejection is valuable protection; transparent mixed-version rolling changes are not certified by same-version restart tests (G7). |
| O7 — secret and shutdown handling | P1 / IMPLEMENTED | Typed config validation/redaction, bounded connector close, tracked child generations, supervised checkpoint tails, and runtime drain ownership exist. The initial test-resource failures require the recorded CI settings; they do not justify weakening cancellation or terminal-proof assertions. |
| O8 — admitted lossy durable delivery | P0 / PARTIAL | Opt-in `ShedOldest` can discard buffered rows under an ALO label; direct configuration also bypasses the builder's EO rejection (G9). Source replay alone cannot fix a cut that has already advanced past discarded work. Default lossless backpressure remains implemented. |

G9 establishes a delivery-policy admission hole and successful row-dropping behavior from code and
existing tests. No fresh broker-loss experiment or additional checkpoint-corruption failure was
demonstrated. This is not a security audit pass, a native-provider pass, or proof of every fault ordering.

## 8. Current upstream approaches and dependency compatibility

Primary sources consulted on 2026-09-19. Comparisons below identify useful constraints, not missing
features merely because a peer has them.

| Platform | Relevant production approach | LaminarDB decision |
|---|---|---|
| Flink 2.2 | Bounded network buffers and buffer debloating reduce barrier delay. Unaligned checkpoints capture in-flight data and trade additional persistence work for reduced backpressure sensitivity; they still cannot interrupt a long record operation. [Upstream checkpoint guidance](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/ops/state/checkpointing_under_backpressure/). | **P2 / INTENTIONALLY-UNSUPPORTED:** do not introduce unaligned checkpoint machinery now. First bound bytes and measure LaminarDB's existing capture/tail protocol under load (G2/G5). |
| Kafka Streams 4.1 | Exactly-once v2 couples input offsets, state changes and Kafka output transactions; it is an end-to-end Kafka contract. [Core concepts](https://kafka.apache.org/41/streams/core-concepts/). | **P1 / IMPLEMENTED:** keep LaminarDB Kafka output labeled ALO. Producer idempotence is not that transactional coupling. Kafka-output EO is a separate, deferred product requirement. |
| Feldera | Fault tolerance checkpoints pipeline state/progress; its documented exactly-once mode adds journaling and output duplicate suppression and still depends on connector support. [Fault-tolerance documentation](https://docs.feldera.com/pipelines/fault-tolerance/). | **P1 / IMPLEMENTED:** retain connector-specific source/state/sink admission and independent sink oracles. Do not derive a universal guarantee from an incremental SQL engine. |
| RisingWave | Barrier/epoch recovery and object-store-backed state support distributed state management at a different storage/operational cost. [Fault tolerance](https://docs.risingwave.com/reference/fault-tolerance), [2026 architecture](https://risingwave.com/blog/architecture-distributed-streaming-database/). | **P3 / INTENTIONALLY-UNSUPPORTED:** a general remote LSM/spill state engine is not a prerequisite for bounded in-memory low-latency workloads. Establish actual state-size demand first. |
| Materialize | Durable consumers record completed progress frontiers and resume within retained history; compaction limits how far back they can resume. [Durable subscriptions](https://materialize.com/docs/transform-data/patterns/durable-subscriptions/). | **P1 / IMPLEMENTED:** LaminarDB already has progress/retention machinery. Explain its local/cluster distinctions and consumer-side effects (G6); do not promise unbounded replay or global ordering. |
| DataFusion / Arrow | Vectorized execution and bounded memory-pool APIs are reusable building blocks, not source/sink transaction or cluster-recovery guarantees. [DataFusion](https://docs.rs/datafusion/latest/datafusion/), [memory pools](https://docs.rs/datafusion-execution/latest/datafusion_execution/memory_pool/index.html). The pinned 53.1.0 source also contains `with_memory_limit`. | **P1 / PARTIAL:** wire the existing pool API where appropriate (G2), and retain LaminarDB's streaming state/frontier protocol. A blanket engine rewrite or partition-count increase is not supported by evidence. |

### Dependencies and features

**P2 / IMPLEMENTED** — the pinned analytical family is deliberate. Root `Cargo.toml` and
`tools/check_analytical_dependencies.py` enforce a coherent Arrow/DataFusion/Delta generation and
prevent accidental duplicate incompatible families. Partition hashing and Arrow row encoding are
part of the state ABI; `xxhash-rust` is pinned as well. Broad default connector/cloud features pull
native build dependencies, so a minimal-feature build and a production distribution are different
artifacts. Core/SQL defaults are small; server defaults include cluster/cloud support. Server's DB
dependency also enables `api`, `remote`, and `local` independently of the server default-feature flag.

| Family | HEAD / lockfile | Upstream observed as of review | Compatibility conclusion |
|---|---|---|---|
| Arrow / Parquet | `=58.4.0` | Arrow `60.0.0`; latest observed 58.x is `58.4.0`. [Versions](https://docs.rs/crate/arrow/latest). | Do not update Arrow alone. Newer Arrow APIs do not make the current generation unsupported. |
| DataFusion / sqlparser | `=53.1.0` / `=0.61.0` | DataFusion `55.1.0` declares Arrow `^59.2.0`, sqlparser `^0.62` and object_store `^0.13.2`. [Manifest](https://docs.rs/crate/datafusion/latest). | Not compatible with the pinned Delta generation as a one-line update; latest Arrow 60 is not even this DF generation. |
| delta-rs | `=0.32.4` | Latest observed `0.32.4`; core still declares Arrow `^58`, DF `^53.1.0`, sqlparser `^0.61.0`. [Core manifest](https://docs.rs/crate/deltalake-core/latest). | Current LaminarDB analytical pins follow upstream constraints; no evidence-based upgrade PR is proposed now. |
| Iceberg | `=0.10.1` family | Latest observed `0.10.1`. [Crate](https://docs.rs/crate/iceberg/latest). | Reuse public append APIs. Local capability checks correctly reject unsupported row mutation actions. |
| object_store | `=0.13.2` | Latest observed `0.14.2`; latest observed 0.13.x is `0.13.2`. [Versions](https://docs.rs/crate/object_store/latest). | Stay on the common DF/Delta compatible family until consumers move together; existing GCS credential adaptation has a concrete compatibility reason. |
| Tokio / rdkafka / Axum | lock `1.53.1` / `0.39.0` / `0.8.9` | Same latest versions observed: [Tokio](https://docs.rs/crate/tokio/latest), [rdkafka](https://docs.rs/crate/rdkafka/latest), [Axum](https://docs.rs/crate/axum/latest). | No modernization gap established. Keep native dependencies and shutdown behavior in validation. |
| Chitchat | lock `0.10.1`, manifest `0.10` | Latest observed `0.13.0`; latest observed 0.10.x is `0.10.1`. [Crate](https://docs.rs/crate/chitchat/latest). | Semver-incompatible minor family; being behind alone is not a production defect or a reason to change durable authority. |
| pgwire / replication | Local `laminardb-*` vendor packages | Local fork behavior is authoritative for this review. | Preserve auth/replication tests; do not replace forks merely because upstream exists. |

**P2 / IMPLEMENTED-BUT-UNVERIFIED** — these are upstream manifest/version observations, not a
claim that an unbuilt newer combination passes LaminarDB tests. No dependency upgrade is required
by a verified finding. If a security or correctness fix necessitates one, use the existing dependency
guard, exact feature CI, source/sink integration tests, state-ABI fixtures and before/after benchmarks
as its acceptance gates. The advisory dependency/audit workflows are not evidence that a specific
release has passed a fresh vulnerability audit in this session.

## 9. Explicitly not worth building yet

All entries are deferred decisions, not omissions to silently fill while implementing G1–G9.

| Finding | Classification | Reason to defer / trigger for reconsideration |
|---|---|---|
| N1 — new generic state backend, RocksDB, remote LSM, automatic spill | P3 / INTENTIONALLY-UNSUPPORTED | Concrete vnode state and object-store cuts already work. First establish a workload that cannot meet the bounded RAM envelope; storage would change latency, checkpoint and reassignment contracts. |
| N2 — embedded Raft or thread-per-core rewrite | P3 / INTENTIONALLY-UNSUPPORTED | Leased/CAS authority and a dedicated compute runtime are deliberate. No demonstrated correctness defect requires replacing either. Profile contention/parallelism before proposing a new scheduler. |
| N3 — arbitrary distributed joins, cluster MVs/reference tables | P2 / INTENTIONALLY-UNSUPPORTED | Whole-node state lacks the admitted distributed lifecycle. Keep rejection until a concrete required SQL shape has a bounded state, restore and ownership design. |
| N4 — promise all connectors exactly-once | P1 / INTENTIONALLY-UNSUPPORTED | Files/Iceberg sources are candidates, NATS/Delta are currently ephemeral, Kafka sinks lack checkpoint-coupled transactions, and CDC admission is disabled. Each requires its own external failure-boundary proof. |
| N5 — finish both CDC systems at once | P2 / PARTIAL | First correct the claims. If demanded, scope one PostgreSQL canonical row/delete adapter and transaction-cut contract using existing replication code; do not enable raw envelopes or advertise snapshot/EO support as a shortcut. That work needs a separate plan and real database crash/transaction tests. |
| N6 — custom Iceberg delete/manifest protocol | P3 / INTENTIONALLY-UNSUPPORTED | Current upstream public actions are insufficient for the rejected mutation modes. Preserve capability gates; reconsider when upstream supports the required atomic actions and a workload needs them. |
| N7 — global subscription total order, infinite replay, transactional arbitrary clients | P3 / INTENTIONALLY-UNSUPPORTED | Existing per-partition/progress/retention contracts meet a narrower useful need. These additions impose coordination/storage costs without evidence of a current requirement. |
| N8 — tenant RBAC, plugin backends, broad compatibility framework | P3 / INTENTIONALLY-UNSUPPORTED | G1 needs a startup authentication guard; G7 needs a tested release transition. Neither establishes demand for a new platform abstraction. |

The credible launch unit is a named, bounded workload on a stated deployment mode and certified
connector composition, with archived correctness and operational evidence. The implementation
already supplies much of that foundation; the backlog closes concrete boundaries around it.
