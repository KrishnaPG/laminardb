# Production hardening execution plan

**Status:** S1–S3, S4a–S4e and S5–S7 verified locally; S4 dependency findings still block CI/release; S6a has an explained sort-latency cost; S6b shared reservation limits and S7 source queue bounds pass the local gates; S8–S13 remain unimplemented.
**Date:** 2026-09-20. **Base:** `b429d0dfd02a435219f1b5977a442da9972e0c3d` (`0.30.0`).
**Evidence:** [production-readiness review](production-readiness.md). Its G1–G9 identifiers are used below.

Implement the smallest correctness fixes first. Establish performance evidence before changing
memory ownership or publication, then qualify the resulting build against a declared workload.
Treat each session below as one bounded change, normally one PR. This is an execution plan, not
a claim that every fix, performance target, or release qualification has been achieved.

The initial planning assumption is **single-node qualification first, then cluster**; embedded
correctness remains in scope throughout. Production throughput, latency, memory and recovery
targets remain to be selected. Their absence does not block the two P0 fixes.

## Order and dependencies

1. Run **S1 and S2 independently** to close the two configuration-dependent P0s. Start B0's
   workload definition alongside them; neither cold-path fix needs a performance project first.
2. Correct public support claims in **S3**. Security enforcement **S4** and telemetry **S5** can
   proceed in separate ownership lanes. Both must finish before production release qualification.
3. Complete B0's measurements, then implement **S6 → S7 → S8 → S9 → S10 → S11** in order. These
   sessions share configuration, lifecycle and memory ownership, so concurrent implementations
   would increase integration risk. Refresh the baseline immediately before each hot-path change.
4. Qualify the integrated build in **S12**. Prepare upgrade fixtures early, but complete **S13**
   against the final candidate before advertising a supported upgrade path.

S10/S11 cover local tables and MVs. A first qualification may exclude those features explicitly;
that qualifies the narrower workload and leaves G3 open. Cluster admission must stay fail-closed.

## Bounded implementation sessions

### B0 — define the workload and capture performance baselines (G5)

**Output:** a recorded workload/configuration, exact build identity, commands, raw Criterion
results and allocation/CPU profiles. No engine redesign or speculative optimization.

For memory work, record each domain's owner, admission/release points, transient allowance and
overflow behavior: source queues, parked/staged cycles, graph ports, DataFusion consumers, live
tables/MVs and checkpoint scratch. This is a short ownership contract, not a new accounting framework.

Choose the source/sink composition, SQL, row widths, batch sizes, key distribution, pipeline
count, event rate, checkpoint interval, retention, hardware and object-store location. Record
numerical latency/recovery ceilings and the RSS envelope before a certifying run. Diagnostic
runs with unset targets remain observational. Use the benchmark map below; add a focused
measurement only where existing benches do not exercise the changed path.

### S1 — require authentication for remote HTTP (G1)

**Owns:** server auth configuration/tests, directly affected examples and Helm documentation.
**Modes:** single-node and cluster. **Risk:** low runtime risk; intentional startup compatibility change.

- Add the rule in `collect_http_auth_errors` in [validation.rs](../crates/laminar-server/src/config/validation.rs).
  `load_config`/`validate_config` and `run_server` already share this guard. Reject missing console
  credentials on non-loopback binds before bootstrap, lease acquisition or listener creation.
- Preserve token rules, diagnostic-only scope and startup/recovery fencing. Cover wildcard,
  routable, loopback and IPv4-mapped IPv6 addresses explicitly; fail closed for ambiguous cases.
- Update the root/server READMEs, `examples/laminardb-cluster.toml`, and Helm quickstarts together.
  Reuse `consoleToken.existingSecret` and environment substitution. Provide no static default token.
  A chart using a remote bind must explain the missing credential before deployment or startup;
  custom configuration remains subject to server validation.

**Exit:** TOML and programmatic starts reject remote anonymous configuration before other work;
valid credentials and loopback development still work. Protected HTTP/WS routes reject invalid,
missing and duplicate credentials when serving is open; preserve earlier 503 responses while
fenced. Validate Helm rendering with/without a secret and with configuration overrides. Existing
cluster plaintext/mTLS tests retain their transport assertions with HTTP credentials supplied.

### S2 — enforce delivery-policy admission in every constructor (G9)

**Owns:** DB configuration, builder/shared-constructor validation and regression tests.
**Modes:** all DB construction paths. **Risk:** low; cold-path only. **Precedes:** S6–S11.

- Permit `ShedOldest` only with BestEffort. Keep lossless Backpressure and Fail behavior.
- Put the rule on [LaminarConfig](../crates/laminar-db/src/config.rs), reused by builder validation
  and `open_with_config_and_vars_and_rules`. Preserve early builder rejection before cluster lease
  binding. Two calls to the same validator are preferable to two implementations of the rule.
- Extract the existing cohesive validation/default-normalization phase from the shared constructor
  when wiring this in: `db/mod.rs`, this constructor and builder `build` have frozen readability
  baselines. Preserve source-idle, future-skew, state and checkpoint-limit validation semantics.
  Error messages and rustdoc must say “BestEffort only.”

**Exit:** test all three guarantees × all three policies through builder and direct configuration,
effective-cap edge cases, and inferred cluster ALO. Preserve cluster BestEffort rejection. Prove
invalid policy invokes no registered connector callback. Existing shedding, lossless deferral,
Fail-at-cap and cursor/recovery tests pass. Keep external overload/restart ledger tests in S12;
this fix does not need a new replay protocol or changes to the record path.

### S3 — correct supported-surface claims (G6)

**Owns:** root/crate READMEs and the smallest supporting example/admission tests.
**Modes:** all. **Risk:** low. **Depends on:** S1 for shared README/example edits.

Describe rejected PostgreSQL/MongoDB CDC admission, Delta reader versus admitted streaming
routes, supported managed cluster windows, the single compute runtime, and local/cluster
subscription boundaries. Link existing typed-contract tests instead of building another registry.

**Exit:** each positive example has a named parse/admission check; unsupported compositions still
fail before connector I/O. No feature flag or checkpoint alone implies exactly-once delivery.

### S4 — enforce existing security checks (G8)

**Owns:** existing audit/deny jobs, `deny.toml`, narrowly necessary dependency fixes.
**Modes:** shipped artifacts. **Risk:** medium dependency/build risk; no broad upgrades.

Run current audit/deny tools against the locked graph and current advisory data; triage findings
before changing gate behavior. Any accepted exception needs a specific reason, owner and review
date. Require both jobs in `ci-success` and remove advisory-only failure handling. The release
workflow already calls this CI workflow: preserve and verify that dependency rather than adding
a second scanner or release pipeline.

**Exit:** clean or explicitly accepted findings, plus a nonpublishing failure exercise proving
audit/deny failures prevent CI/release success. External branch-protection settings are a separate
verification; do not infer their state from workflow YAML.

### S5 — expose Kafka progress and freshness (G4)

**Owns:** Kafka metrics/background collection, required engine progress export and dashboard.
**Modes:** Kafka in all modes. **Risk:** medium semantic/observability risk.

Reuse `LaminarConsumerContext`, assigned partitions, the metrics registry and librdkafka statistics
or bounded metadata tasks. Define the offset convention before implementation: fetched/consumed
position, settled processing position and committed checkpoint recovery position are different.
Broker advisory commits cannot substitute for Laminar's committed recovery frontier. Coordinate
any DB callback changes with the single DB integration owner.

**Exit:** pause consumption while continuing broker writes: lag rises within two collection
intervals. Test source freshness age separately when production stops. Stale/unavailable measurements
are distinguishable from zero; revoked assignments disappear; label count is bounded. Dashboard
expressions match emitted metrics. Checkpoint/source freshness is alertable while readiness keeps
its authority/lifecycle meaning. No network polling or per-row metric allocation on compute.

### S6 — bound participating DataFusion working memory (G2)

**Progress (2026-09-20):** implemented and verified locally. See S6a below for the cached-plan
repair and measured sort cost, and S6b for the shared reservation pool, no-spill configuration,
correctness gates and matched performance evidence. Wider memory bounds remain S7–S11 work.

**Owns:** cached physical-plan reuse, configuration and DataFusion runtime/context creation.
**Modes:** all DB modes. **Risk:** medium; execution-state reuse, query failures and spilling behavior
change. **Depends on:** S2 and B0.

Reuse DataFusion 53.1's bounded pool and runtime APIs. Establish the budget scope explicitly and
share the intended per-DB pool with both the main context and the separately built connector
operator-graph context in [operator_graph.rs](../crates/laminar-db/src/pipeline_lifecycle/operator_graph.rs).
Inspect other public context constructors so the documented scope has no unbounded bypass.
The cluster-only `collect_local_table` diagnostic also constructs a fresh context; include it
in the per-DB scope. Standalone `laminar-sql` context factories and the expression-only lambda
context need an explicit scope decision rather than an implicit whole-process guarantee.

Disable disk spilling for execution on the compute runtime using the existing disk-manager API;
the upstream default can use OS temporary files. Do not introduce a new spill subsystem. The
pool governs participating fallible reservations, not every Arrow allocation or whole-process RSS.

**Exit:** expensive real plans hit a typed allocation/query error at the intended reservation
boundary, release reservations on error/cancel, create no compute-path spill files, and preserve
source progress/recovery. Test concurrent contexts against the shared budget and cached-plan
reuse after failure. Baseline and rerun representative DataFusion/coordinator workloads.

### S7 — bound connector-to-coordinator queued bytes (G2)

**Progress (2026-09-20):** implemented and verified locally. See the dated S7 record below for
the ownership contract, correctness gates, corrected burst fixtures and matched performance evidence.

**Owns:** `SourceMsg` admission and source-task handoff, including shutdown-tail sends.
**Modes:** all connector pipelines. **Risk:** high correctness/performance. **Depends on:** S6 + fresh B0.

Trace reservation ownership through normal, pending-cursor and direct shutdown `try_send` paths,
drain, parked messages, recovery and cancellation. The count-bounded source channel is shared
across sources. A queue permit released at dequeue does not bound graph retention; document the
handoff into separately bounded owners and complete prospective graph enforcement in S9.

Reuse existing caps/backpressure with finite documented defaults chosen from B0. Reserve before
queue admission; reject a single oversized batch instead of waiting forever. Control/barrier
traffic must retain FIFO ordering and a path to make progress when data admission is saturated.
Use batch-level accounting; no new per-row allocations, locks or hashing without benchmark evidence.

**Exit:** wide batches, multiple sources, slow sinks, parked input and cancellation respect the
queue budget; accepted work is not silently dropped and refused input is not settled. All charges
release exactly once. Saturated queues permit shutdown, checkpoint and recovery progress or their
existing bounded errors. This session alone does not close the whole G2 memory bound.

### S8 — close the embedded push-queue bypass (G2)

**Owns:** public core streaming source admission and its DB configuration plumbing.
**Modes:** embedded/in-process sources. **Risk:** high API/performance. **Depends on:** S7 + fresh B0.

`Source::push_arrow` first puts an arbitrary batch in a separate count-bounded `SourceMessage`
ring. S7 alone cannot bound it. Inspect DB typed-to-Arrow conversion, `SourceEntry::push_and_buffer`,
snapshot retention, broadcast transfer and cloned producers too. Reuse the existing source owner
and nonblocking API; share the budget across clones. Document rejection and ownership semantics
without requiring caller-created reservation wrappers or an async push API. Do not expand this
into generic heap-size accounting for every possible user-defined `Record<T>`.

**Exit:** the documented Arrow/DB-handle paths respect their stated limits, including variable-width
records. Tests cover concurrent producers, full/closed queues, failed pushes, flush and cancellation
without leaked charges; unsuccessful admission leaves sequence/snapshot state unchanged. Explicitly
state the standalone generic-record API's accounting scope. Push/streaming benchmarks pass the gate.

### S9 — enforce prospective graph-buffer limits (G2)

**Owns:** source priming, graph-port admission, output fan-out and deferred input ownership.
**Modes:** all. **Risk:** high correctness/performance. **Depends on:** S7/S8 + fresh B0.

Existing gates inspect current usage before routing; the next output can overshoot a byte cap.
Inspect `is_downstream_at_capacity`, `gate_decision`, `push_to_port`, `route_output` and
`prime_sources` together. Preflight the incoming size with explicit fan-out/shared-buffer charging.
Preserve accepted input across deferral. An operator may already have mutated state before its
output size is known: never blindly rerun it on admission failure. Use the existing fault/recovery
boundary where deferral cannot safely preserve the executed result.

**Exit:** empty/nearly-full ports cannot overshoot the declared retained-byte limit; oversized
operator output has a typed, tested failure path. Fan-out, checkpoint/restart and retry tests find
no lost input or duplicate state transitions. Operator results match an independent reference;
benchmark queueing and output admission as well as the core execution loop.

### S10 — quota live reference-table state (G3)

**Owns:** table storage/refresh/restore and quota configuration.
**Modes:** embedded and single-node; retain cluster rejection.
**Risk:** high accounting/atomicity. **Depends on:** S9 + fresh B0 for any hot-path changes.

Preflight the final state of an upsert/refresh/restore before mutation. Count encoded keys and
retained Arrow storage with explicit treatment of shared buffers and capacity. A row slice can
retain its original large allocation; existing checkpoint capture estimates are not an exact
live counter. Avoid a whole-map clone per update. Measure retention amplification before choosing
compaction; no eviction of live SQL keys or generic state-backend layer.

**Exit:** growth and over-limit restore fail atomically; replacements/deletes release the expected
retention; wide/nested/dictionary/view arrays and one surviving slice of a large batch are covered.
Failed multi-table refresh preserves the previous complete installation and readiness. Lookup,
refresh and checkpoint-under-load measurements remain acceptable.

### S11 — quota all local MV storage modes (G3)

**Owns:** MV storage, publication preflight and restore.
**Modes:** embedded and single-node; retain cluster rejection.
**Risk:** high publication/performance. **Depends on:** S10 + fresh B0.

Cover Aggregate, Append, Upsert and Multiset modes, including one oversized append batch, owned
scalar/key bytes and multiplicity errors. Preserve intentional append retention semantics.
Reuse staged deltas, but validate all affected MV quota updates before applying/publishing any
of them. `update_mv_stores` currently updates and sends each MV sequentially; adding a later
quota error without this preparation could expose a partial cycle. This is a change-design
hazard, not evidence that local subscriptions offer transactional delivery today.

**Exit:** a failing second MV leaves both stores and quota-dependent publications unchanged;
successful cycles, replacements, deletes, restore and snapshot materialization match independent
expected results. State and cursor fault handling stay consistent. Reuse staged deltas; any new
state clone or lock needs bounded transient memory and benchmark evidence. MV update/materialization
and coordinator benchmarks/profiles pass.

### S12 — qualify the integrated workload (G5)

**Owns:** extensions to existing soak/oracle tooling and immutable evidence bundles.
**Depends on:** S1–S9, and S10/S11 when tables/MVs are advertised.

Use existing process-kill/checkpoint/rejoin harnesses and external sink readers. Measure arrival
through external visibility separately from compute-cycle and checkpoint-stall time. Record
p50/p95/p99/p99.9 with sufficient sample counts and tail resolution, queue slope, RSS and recovery.
Use a consistent observer clock or explicitly bound inter-host clock error; record offered load
and backpressure so a stalled producer cannot make latency look artificially healthy.

Run at least one-hour steady workloads, three repetitions, one/four pipelines and declared
uniform/skewed/hot-key distributions. Include slow/failed sinks, paused sources, checkpoint under
load, process/leader loss, rejoin/scale changes where supported, corrupt cuts and expired replay.
Add the G9 durable Backpressure/Fail saturation → checkpoint → restart external-ledger cases.

**Exit:** declared latency/recovery ceilings pass; queues stabilize below advertised capacity; RSS
plateaus inside the declared envelope. An independent oracle finds no missing ALO records and
no duplicate committed effects for exact compositions. Qualify each mode/composition separately:
Kafka output remains ALO; cluster exact candidates retain the existing Kafka → direct-S3 Delta
or supported REST-Iceberg admission. Native provider ALO evidence and ineligible standalone
contract scaffolds cannot certify another composition. Publish limitations with the evidence.

### S13 — qualify one supported upgrade pair (G7)

**Owns:** cross-version process/fixture tests and upgrade/rollback instructions.
**Modes:** persistent deployments. **Risk:** high authority/replay risk.

Select an actual predecessor release and inspect its state/partition/pipeline ABIs. An old binary
writes the committed cut; the candidate restores and continues against an external oracle.
Rehearse stop/checkpoint/upgrade/restart, incompatible-cut rejection before intake, and leadership
loss during transition. A rollback must respect authority and external effects after new commits;
copying an old checkpoint over current authority is not a safe generic rollback.

**Exit:** list the tested pair, supported directions and required retained artifacts. Either the
pair preserves continuity or it fails closed with an actionable unsupported-transition result.
No blanket rolling-upgrade, state-migration or arbitrary previous-version compatibility promise.

## Common verification and performance gates

Each implementation starts with a failing regression or a measured reproducer for its stated
problem, then uses existing coverage before adding tests. Do not add tests that mirror private
implementation. A refactor must preserve current error ordering, cleanup and admission boundaries.

For each completed code change run the relevant targeted tests, then the repository gates:

```powershell
$env:RUST_MIN_STACK = '4194304'
cargo test --workspace --lib --locked -j1 -- --test-threads=2
cargo clippy --workspace --all-features --all-targets --locked -j1 -- -D warnings
cargo clippy --workspace --no-default-features --locked -j1 -- -D warnings
cargo +nightly fmt --all -- --check
cargo run --quiet --manifest-path tools/readability-check/Cargo.toml -- .
python tools/check_analytical_dependencies.py
```

S1 also requires the server binary suite, which `--lib` does not run. The server's default
features include cluster support. Run the suites together with the same stack setting:

```powershell
cargo test --workspace --lib --bin laminardb --locked -j1 -- --test-threads=2
```

S2 must similarly include the feature-gated DB cluster-admission regressions; a passing local-only
suite cannot cover inferred cluster ALO. Run integration targets named by the affected session.

Use `--offline` only when dependencies are already cached; S4 advisory data must be current.
The previous review passed these gates with documented Windows resource settings. That evidence
belongs to the base SHA and does not substitute for tests after implementation. Run heavy builds
serially on this machine; prior parallel linking exhausted available paging resources.

| Changed path | Required focused performance evidence |
|---|---|
| Constructor/auth/config only (S1/S2) | No hot-path benchmark required while edits remain cold |
| DataFusion/coordinator/queued bytes (S6/S7/S9) | `latency_bench`, `stream_executor_bench`, relevant `hot_path_micro` cases, plus changed admission/cancel/fan-out measurements |
| Core push/ring admission (S8) | `latency_bench`, `streaming_bench`, focused typed/Arrow push cases |
| Table/MV lookup and publication (S10/S11) | `latency_bench`, `lookup_join_bench`, relevant graph/hot-path cases; add meaningful update/refresh/MV cases if absent |
| Checkpoint/recovery affected by any change | `recovery_bench` plus existing real-process recovery tests; a microbenchmark is not a cloud recovery SLO |

Run `cargo bench --bench latency_bench` and the applicable benches before and after each hot-path
change on the same quiet hardware, release profile, feature set and workload. Confirm requested
feature-gated cases actually ran. Save raw distributions, throughput, allocation/retention and CPU
profiles (including IPC where the target hardware exposes it). Explain counter limitations.
The repository's IPC > 2 guideline is diagnostic, not a substitute for latency measurements.

**A regression over 5% blocks landing until removed or explained with evidence.** Do not hide it
by widening thresholds, dropping difficult inputs or benchmarking during other builds/soaks.
Statistical uncertainty requires another controlled measurement. Numeric defaults must fit the
measured envelope; limits on queues, DataFusion reservations and state are not additive proofs
of total RSS because buffers may be shared and some allocations remain outside those domains.

## Agent/session coordination

Use separate worktrees for simultaneous implementation sessions and one integration owner.
Adjacent small sessions may share a task while retaining separate reviewable changes; the list
does not require thirteen simultaneous agents or thirteen new tasks.
Do not commit unrelated `.zcode/` work. Suggested concurrency is S1 + S2, then S4 + S5 alongside
the serial DB lane where file ownership permits. Run one benchmark/soak at a time per host.
Independent review can run while another owner prepares an unrelated change; review does not
replace the owner running tests. Shared files are assigned before editing, not resolved by racing
agents. Never increase frozen readability baselines to accommodate growth.

Use this brief for each fresh session:

> Implement only session **S#** from `docs/production-hardening-plan.md`, using the current source
> and `docs/production-readiness.md` as evidence. Record the starting SHA and completed dependencies;
> recheck relevant assumptions if HEAD moved. Preserve the stated mode restrictions. Reproduce the
> problem, make the smallest change, run the session exit tests and common gates, and inspect the
> final diff for duplication and unrelated edits. For hot-path work, capture before/after benchmarks
> and profiles before claiming completion. Report changed APIs/config defaults, tests, benchmark
> deltas, residual risks and evidence paths. Split newly discovered problems into bounded follow-up
> work; do not silently widen this session or weaken an admission/test to make it pass.

At handoff record: base/result SHA or uncommitted diff, affected modes, reproduction, test results,
performance evidence, any compatibility change, outstanding blockers, and the next dependency.
A passing PR closes its scoped gap only; production readiness requires the corresponding S12/S13
evidence. S1–S3, S4a–S4e and S5 are **implemented and verified locally**. S4 is **partially
implemented**; the enforced scans still fail on unresolved dependency findings. B0 now has local
diagnostic timings and CPU/allocation profiles. S6a repairs cached-plan retention; S6b reservation
limits and S7 source queue byte bounds are **implemented and verified locally**. S8–S13 remain
**not implemented**. Production
qualification is open.

## Implementation progress

S1/S2 changes were made against the base SHA above. Before production edits, five new delivery-policy
tests and five new HTTP tests failed for the expected admission/parser problems. Shared policy
validation now rejects durable shedding, and the existing server guard requires a console token
for non-loopback HTTP. Duplicate WS query tokens, including bare duplicate keys, fail authentication.
Docker, Helm and remote examples use environment/Secret references; no default credential was added.

The independent code review's bare-query-key finding was corrected and rechecked; no further
actionable findings remained. Verification on Windows passed:

- Workspace libraries and server binary: **6,003 passed, zero failed, one ignored**. This includes
  all five new delivery-policy regressions and existing graph/recovery coverage. The ignored test
  downloads an ONNX model and requires an external runtime; it is unrelated to these changes.
- Clippy with all features/all targets and with no default features, both with `-D warnings`.
- Nightly formatting, readability, analytical-dependency generation and diff whitespace checks.
- Strict Helm lint for defaults and all three CI values files; six render cases covered defaults,
  a token Secret, standalone/cluster/full values and a custom configuration/Secret key. Docker's
  bundled TOML parsed with the required token environment reference. No image build or Kubernetes
  deployment was run.

The full test command was `cargo test --workspace --lib --bin laminardb --locked --offline -j1 --
--test-threads=2`, with `RUST_MIN_STACK=4194304`. The default server features include cluster, and the
cluster delivery-admission test ran. These changes stay in configuration/startup and HTTP
authentication, outside the streaming record path; no hot-path benchmark claim is made.

Compatibility: non-loopback HTTP now requires `server.console_token`; durable delivery now rejects
`ShedOldest` through builder and direct configuration. Use Backpressure or Fail for durable delivery.
Regression logs and gate output are under `target/p0-hardening/`. S12/S13 release and upgrade
qualification remain outstanding.

### S3 — supported-surface documentation

Completed against `a16c5a6d` on `codex/p0-production-hardening`. The root and crate READMEs now
describe rejected PostgreSQL/MongoDB CDC admission, Delta reader versus streaming-route
capability, managed direct-source cluster windows, the single compute runtime, and local versus
cluster subscription replay. The root README links named admission examples and rejection tests.
The SQL README no longer presents PostgreSQL CDC as a positive connector example. `SECURITY.md`
now reflects the existing latest-minor support policy for 0.30 and the S1 HTTP authentication rule.

This is documentation only, affecting guidance for all modes. It changes no API, admission rule,
dependency or execution path. Existing contract coverage was reused; no duplicate capability
registry or tests of document wording were added. An independent source/test review found no
actionable inaccuracies, and local Markdown links resolve.

Validation: `cargo test --workspace --lib --bin laminardb --test cdc_admission --locked --offline
-j1 -- --test-threads=2` with `RUST_MIN_STACK=4194304` passed **6,005 tests, zero failed, one ignored**.
All 15 admission/replay/config checks linked from the corrected sections ran and passed. Both
Clippy gates, nightly formatting, readability, analytical-dependency generation and whitespace
checks passed. Test linking reported cached OpenSSL debug-symbol warnings; the builds and tests
succeeded. The ignored ONNX model test and proc-macro future-compatibility notice are unchanged.
Logs are under `target/s3-hardening/`. No performance or external-system qualification is claimed.

### S4 — initial dependency triage before enforcement

Read-only triage used cargo-audit **0.22.2**, cargo-deny **0.20.2**, and freshly fetched RustSec
revision `d5c17953a895cf19e8d3ce66eaa42b6fcfe1fb16` on 2026-09-19 against the unchanged lockfile.
Audit reported **seven vulnerability findings** in the lockfile; this is not a claim that all
seven are reachable in each deployed binary. In particular, the old rkyv entry has no selected
edge in the all-features/all-targets graph inspected during triage.

| Locked dependency | Finding / fixed range | Next bounded work |
|---|---|---|
| crossbeam-epoch 0.9.18 | RUSTSEC-2026-0204; fixed >=0.9.20 | Compatible update and regression checks |
| h2 0.4.14 | RUSTSEC-2026-0258; fixed >=0.4.16 | Compatible update and transport checks |
| quick-xml 0.39.4 | RUSTSEC-2026-0194 and RUSTSEC-2026-0195; fixed >=0.41.0 | object_store 0.13.2 and OpenDAL 0.57.0 constrain XML to 0.39; review a backport or coordinated dependency migration. Cloud response parsing reaches the affected reader. |
| rkyv 0.7.46 | RUSTSEC-2026-0235; fixed >=0.8.17 | Dormant optional rust_decimal dependency; review parent resolution/feature removal without changing the active 0.8 state ABI |
| rsa 0.9.10 | RUSTSEC-2023-0071; no patched release listed | Finish transitive signing/decryption reachability review before considering any exception |
| rustls 0.23.40 | RUSTSEC-2026-0285; fixed >=0.23.45 | Exact-version dry run succeeds but also upgrades aws-lc native crypto and webpki; validate all TLS feature sets |

Additional findings: unsound `event-listener` 5.4.1 (fixed >=5.4.2) and `lru` 0.16.3
(fixed >=0.18.2, constrained by chitchat), yanked `chacha20` 0.10.0, and unmaintained transitive
dependencies. Compatible-update dry runs succeeded for crossbeam-epoch, event-listener, h2 and
chacha20. A generic rustls update selected a still-affected version, so it is insufficient.

The checked-in `deny.toml` fails the current tool's schema before scanning. A temporary modernized
configuration also exposed missing license allow-list entries for Unicode-3.0, bzip2-1.0.6,
CDLA-Permissive-2.0 and BSL-1.0. License review, schema migration, finding remediation and the
nonpublishing failure exercise remain part of S4. The release workflow already depends on reusable
CI; audit/deny remain advisory-only and absent from `ci-success` until this work is completed.

No dependency versions, scanner policy, exceptions or workflows changed during this triage.
Full scan output, parent graphs and dry runs are under `target/s4-hardening/`. Follow up with
compatible dependency repairs first, then the constrained XML/LRU and RSA work, before enabling
the required audit/deny gates. Preserve the analytical-generation invariant and do not hide
unresolved findings behind blanket ignores. S5 can proceed independently; B0 remains required
before the serial S6–S11 memory changes.

### S4a — compatible dependency repairs and PEM migration

Implemented against `aa68f16e` on `codex/p0-production-hardening`. This bounded part of S4 repairs
dependencies compatible with the current analytical generation and removes the unmaintained
server PEM wrapper. It does not complete S4 or accept any advisory/license exceptions.

| Dependency | Previous | Updated |
|---|---|---|
| crossbeam-epoch | 0.9.18 | 0.9.21 |
| event-listener | 5.4.1 | 5.4.2 |
| h2 | 0.4.14 | 0.4.19 |
| chacha20 | 0.10.0 | 0.10.2 |
| rustls | 0.23.40 | 0.23.45 |
| rustls-webpki | 0.103.13 | 0.103.15 |
| aws-lc-rs | 1.17.0 | 1.18.1 |
| aws-lc-sys | 0.41.0 | 0.45.0 |

`concurrent-queue` leaves the graph with the upstream event-listener update. `rustls-pemfile` is
removed from the server and lockfile; pgwire now calls the existing rustls-pki-types `PemObject`
API through tokio-rustls. The former wrapper already used that parser. File-open ordering,
empty-file diagnostics, first supported private key selection (PKCS#1/PKCS#8/SEC1), certificate
and CA bundles, key permission warnings, expiry checks and failed-reload retention are preserved.
Underlying malformed-PEM detail strings now use the maintained parser's formatting; the server's
contextual error labels are unchanged.

The dependency updates affect builds using this workspace lockfile in all modes; the PEM change
affects the single-node and cluster pgwire listeners. No coordinator/operator code or state ABI
changes. Updated native crypto and transport behavior still require shipped-platform CI and
workload qualification; no hot-path benchmark or production latency claim is made here.

Cargo initially reselected unrelated parking_lot, socket2 and Windows dependency edges. The
committed selections for unchanged packages were preserved and locked metadata revalidated.
Independent review confirmed exactly eight version changes and two removals, with no unrelated
edge changes. The only changed edge on an unchanged package is the removed server PEM dependency.
Analytical dependency generations remain unchanged.

Against the same current RustSec revision recorded above, the scoped audit comparison verifies
five resolved advisory IDs: RUSTSEC-2026-0204, RUSTSEC-2026-0221, RUSTSEC-2026-0258,
RUSTSEC-2026-0285 and RUSTSEC-2025-0134. Vulnerability findings fall from seven to four. The two
quick-xml advisories, RSA and dormant rkyv remain; unsound LRU and three unmaintained dependencies
remain warnings in cargo-audit. The strict temporary cargo-deny configuration still fails on
active unresolved advisories and the existing license allow-list gaps. Scanner policies and CI
workflows are unchanged. Raw before/after reports and dependency evidence are in
`target/s4-compatible/`.

Validation: the focused TLS run passed **44 tests**. `cargo test --workspace --lib --bin laminardb
--test cluster_tls_integration --locked --offline -j1 -- --test-threads=2` with
`RUST_MIN_STACK=4194304` passed **6,007 tests, zero failed, one ignored**. This includes the three
new PEM regressions, both roots in the expanded CA-bundle test and the real cluster mTLS exchange.
Both Clippy gates, nightly formatting, readability, analytical-dependency generation, locked
metadata and whitespace checks passed. Independent source and dependency review found no
actionable issues. Cached OpenSSL debug-symbol warnings, the ignored ONNX model
test and the proc-macro future-compatibility notice are unchanged. Logs are under
`target/s4-compatible/`.

Next S4 work is the constrained XML/LRU dependency remediation, dormant-rkyv scope and RSA review,
then license/schema policy and required CI gate enforcement. No clean audit, release qualification
or branch-protection result is claimed.

### S4b — remove dormant legacy serialization dependency

The targeted Cargo update from rust_decimal 1.41.0 to 1.43.0 removes its optional rkyv 0.7
support and ten unused packages from the lockfile. Laminar's active rkyv 0.8.18 codec and the
analytical dependency generations are unchanged. The upstream release also includes decimal
arithmetic and formatting changes, so this is validated as a dependency update, not merely
manual lockfile cleanup. [Upstream release](https://github.com/paupino/rust-decimal/releases/tag/1.43.0).

Audit against the current RustSec snapshot now reports three vulnerability findings: the two
quick-xml advisories and RSA. The removed rkyv finding described an inactive optional dependency;
this update does not claim to repair an active Laminar checkpoint vulnerability. No advisory
exception or scanner policy change was made. Logs are under `target/s4-remaining/`.

Focused pgwire text/binary decimal checks with postgres-types 0.2.14 pass six cases on both
rust_decimal versions, covering signs, zero, extrema, scale 28, trailing zeros, known wire bytes,
invalid text and truncated/special binary values. A seventh check fails on both versions:
upstream `Decimal::from_sql` panics for an out-of-range binary NUMERIC value (`10^32`). This
pre-existing helper defect is retained as a reproducible finding, not suppressed. Current Laminar
handlers do not call that decoder; review it before adding typed NUMERIC parameter decoding or
qualifying downstream uses of the maintained pgwire helper. Workspace validation passed with
the S5 changes as recorded below; the exploratory decoder failure remains open.

The remaining XML fixes require an analytical-generation migration or maintained upstream
backport. LRU first resolves through Chitchat 0.13, which also changes the transport API, gossip
envelopes and dead-node retention; that requires a separate cluster upgrade and mixed-version
tests. Current Chitchat keys have no panicking destructor, but no advisory exception is accepted.
RSA still has no patched compatible parent; active reqsign uses randomized signing, not decryption,
which narrows the observed surface without proving absence of timing leakage. Preserve these
findings while proceeding with the independent S5 work; avoid local forks or compatibility wrappers.

### S4c — require dependency checks in CI and release

Implemented from `a8aedeee`. This changes repository/release policy for all shipped modes;
runtime code, dependency versions and hot-path behavior are unchanged. Enforcement now proceeds
before the constrained dependency migrations: the existing findings deliberately block CI and
release instead of being hidden by successful aggregate results. S4 remains incomplete.

The existing audit/deny jobs no longer tolerate failure, and both are required by `ci-success`.
Audit treats warnings as errors; deny checks the locked, all-features graph across the existing
six target triples and rejects vulnerability, unsoundness, unmaintained and yanked findings.
The old `instant` advisory ignore was removed; no advisory exceptions were added. The existing
release dependency chain already requires reusable CI before artifact builds, release creation,
crate publication and manifest updates, so it needs no additional scanner or workflow.

The deny policy uses the [current configuration schema](https://embarkstudios.github.io/cargo-deny/checks/advisories/cfg.html).
Unlicensed and unlisted licenses remain errors under the tool's default-deny license policy.
Four permissive license identifiers used by the locked dependencies are now explicitly allowed,
after checking package license files against their SPDX texts:

| License | Existing dependency examples | Conditions relevant to redistribution |
|---|---|---|
| [Unicode-3.0](https://spdx.org/licenses/Unicode-3.0.html) | ICU4X, unicode-ident | Retain copyright/permission notices in copies or documentation; no unauthorized name promotion. |
| [bzip2-1.0.6](https://spdx.org/licenses/bzip2-1.0.6.html) | libbz2-rs-sys | Retain source notices, identify altered sources and respect origin/endorsement restrictions. |
| [CDLA-Permissive-2.0](https://spdx.org/licenses/CDLA-Permissive-2.0.html) | webpki-roots, webpki-root-certs | Include the agreement with shared data. |
| [BSL-1.0](https://spdx.org/licenses/BSL-1.0.html) | xxhash-rust | Retain notices and license text, subject to the license's object-code exception. |

This allow-list decision does not verify notice packaging in every release artifact. Package
license paths/hashes and scan output are retained under `target/s4-gates/`.

Fresh scans with cargo-audit 0.22.2 and cargo-deny 0.20.2 used RustSec revision
`d5c17953a895cf19e8d3ce66eaa42b6fcfe1fb16`. Both exit **1** for substantive advisory findings.
Deny's license, source and ban checks have no errors; duplicate-version and one unused-license
warning remain. Its seven advisory errors are the two quick-xml vulnerabilities, RSA, unsound
LRU, and unmaintained number_prefix, paste and proc-macro-error2. Audit additionally reports
unmaintained `instant` in its broader lockfile scope. These are remediation/review work, not a
clean security scan or newly accepted exceptions.

The nonpublishing failure exercise executes the actual aggregate Bash step extracted from CI:
**65 cases passed**, including every required job failing, cancelling, skipping or missing its
result, all-success, and all audit/deny status combinations. The release dependency chain was
checked locally; actionlint 1.7.12 accepts both workflows. Three isolated license probes confirm
MIT is accepted while an unlisted GPL license and an unlicensed package are rejected. No hosted
CI/release run or external branch-protection setting was changed or verified. The exercise is
local evidence, not a publishing rehearsal.

Workspace library tests passed **5,672 tests, zero failed, one ignored**, using the locked,
offline graph, `RUST_MIN_STACK=4194304`, one build job and two test threads. Both Clippy gates,
nightly formatting, readability and whitespace checks passed. The ignored ONNX test and cached
OpenSSL debug-symbol warning remain unchanged. Validation logs are under `target/s4-gates/`.

Next S4 work remains constrained dependency remediation or explicit, owned, time-bounded
advisory review; B0 still blocks the serial memory changes. These passing regression and policy
checks do not override the failing security scans.

### S4d — remove unmaintained download-progress dependency

Implemented from `3d8b502d`. The targeted `hf-hub` update from
0.4.3 to 0.5.0 replaces indicatif 0.17.11 with 0.18.6 and console 0.15.11 with 0.16.6,
removing number_prefix 0.4.0 in favor of unit-prefix 0.5.2. The selected Tokio/Rustls client
features remain the same, as recorded in the [published 0.5.0 manifest](https://docs.rs/crate/hf-hub/0.5.0/source/Cargo.toml).
The upgrade retains Laminar's existing async-client and cache APIs.

This applies to builds enabling local AI, including the server. Cluster SQL admission is
unchanged. Production loader/inference code and the analytical dependency generations are
unchanged. Two focused regressions cover the existing Hugging Face snapshot layout and a local
HTTP download with progress reporting, label-cache publication and a second read without HTTP.
They need neither an external model download nor an ONNX Runtime installation.

Fresh audit/deny scans against RustSec revision `d5c17953a895cf19e8d3ce66eaa42b6fcfe1fb16`
remove only `RUSTSEC-2025-0119`, with no new findings. Deny retains six advisory errors;
audit also reports unmaintained `instant` in its broader lockfile scope. Both scans still
exit 1. No scanner policy or advisory exception changed. Evidence is under `target/s4-hub/`.

Validation passed: **six focused local-backend tests** and **5,674 workspace library tests**,
zero failures, plus both Clippy gates, nightly formatting, readability, locked metadata,
analytical-dependency generation and whitespace checks. The existing model-download/ONNX test
remains ignored; this is cache/client compatibility evidence, not a new inference qualification.
The Windows feature change required a wider rebuild; existing OpenSSL debug-symbol warnings
are unchanged. No coordinator/core-operator code changed, and no hot-path performance claim is made.

### S4e — remove the unsound gossip cache dependency

Implemented from `0c78edc0`. Chitchat 0.10.1 → 0.13.0 permits the fixed LRU 0.18.4,
removing `RUSTSEC-2026-0253`. The only added package is itertools 0.15.0. LRU now enables
default allocator/hasher features on the existing hashbrown 0.17.1; no other existing package's
selected features change. The analytical dependency generations and Tokio version are unchanged.

This affects cluster gossip discovery. Embedded, single-node and static discovery behavior are
unchanged. Chitchat's new [protocol selector](https://docs.rs/chitchat/0.13.0/chitchat/struct.ChitchatConfig.html)
is explicitly V0, preserving the existing uncompressed wire format. The existing partition-test
transport forwards the new envelope/outcome types and socket address, and reports zero bytes
when it drops a simulated packet. KV lookup compares the new shared node-ID type through borrowed
strings. No new wrapper, configuration knob or compatibility layer is added.
Callers supplying their own Chitchat transports must adopt the updated upstream socket API.
Laminar's software-version, discovery-protocol and process-generation admission checks remain intact.

A regression checks actual outbound V0 bytes, replies to legacy SYN vectors and rejection of
a foreign cluster. The vectors were checked against the published 0.10.1 UDP encoder in an
isolated probe. A second probe runs published 0.10.1 and 0.13.0 peers together over loopback UDP:
both directions pass live membership, initial values, updates, tombstones, dead-peer collection
and higher-generation rejoin. Its separate old dependency graph is test evidence under
`target/s4-gossip/mixed/`, not part of the workspace or shipped lockfile. This is protocol evidence,
not an S13 cross-release upgrade or rollback qualification.

Upstream also increases the bounded garbage-collected-node history from 500 to 5,000 entries and
uses shared node-ID strings. Laminar's explicit failure-detector and tombstone grace periods are
preserved. The larger history can reserve and retain more control-plane memory; production
RSS qualification remains open. No coordinator/core-operator code changed and no hot-path
performance claim is made.

Fresh scans at RustSec revision `d5c17953a895cf19e8d3ce66eaa42b6fcfe1fb16` remove only the
LRU advisory, with no new findings. Both scans still exit 1: deny retains five advisory errors
(two quick-xml findings, RSA, paste and proc-macro-error2), and audit additionally reports instant.
No advisory exception or scanner policy changed. Raw dependency, scan and probe evidence is
under `target/s4-gossip/`.

The locked/offline workspace library and cluster integration run passed **5,685 tests, zero
failed, two ignored**, using `RUST_MIN_STACK=4194304`, one build job and two test threads. This
includes **5,675 library tests** with the new V0 regression and ten core/DB cluster integration cases.
The normally ignored same-ID rejoin case also passed when explicitly selected; its existing
test ignore remains because one successful run does not establish repeatability. The ONNX
model test remains ignored. Both Clippy gates, nightly formatting, readability, locked metadata,
analytical-dependency generations and whitespace checks passed. Readability exceptions did not grow.

The test command was `cargo test --workspace --lib --test cluster_integration --locked --offline
-j1 -- --test-threads=2`; the rejoin check used the same targets with the
`killed_node_can_rejoin -- --ignored --exact --test-threads=1` filter. The two isolated compatibility
probe tests also passed. These local Windows results do not replace shipped-platform CI or S12/S13.

Windows linking initially exhausted disk space. Removing older generated incremental caches
freed approximately 110 GiB, preserving benchmark baselines, compiled outputs and evidence;
the same validation then passed. Existing OpenSSL debug-symbol warnings remain unchanged.

A 2026-09-20 registry/manifest follow-up found no additional narrow repair: object_store 0.13.2
and OpenDAL 0.57.0 remain the latest releases in their permitted series and still require XML
0.39. Delta Lake 0.32.4 still requires validator 0.19. Published reqsign-core 3.3.1,
reqsign-google 3.1.1 and reqsign-azure-storage 3.2.1 still depend on RSA 0.9; their archives were
checked against the registry checksums. No dependency or advisory policy changed. This was a
manifest review, not a fresh security scan; evidence is under `target/s4-next/`.

### S5 — Kafka reader progress and delivery freshness

Implemented from `598a656c` alongside S4b. This applies to Kafka sources in embedded, single-node
and cluster modes when a metrics registry and canonical source name are supplied.

Implementation reuses the existing canonical source name, source task tracker, bounded Kafka
metadata lookup and Prometheus registry. A source-owned sampler runs every ten seconds,
independently of the reader queue and connector polling. Samples use fresh broker high watermarks
and the next offset handed to the Kafka reader, not broker advisory commits. No new coordinator
or core-operator work, connector trait hook, dependency, or per-record allocation is introduced.

New source-labelled metrics expose reader offset distance, sample availability and timestamp,
and the timestamp of the last nonempty successful connector poll. Unknown positions, failed
queries and assignment changes cannot manufacture zero lag. Partition labels follow the current
assignment; source registration has one cleanup owner so retired workers cannot overwrite a
replacement's metrics. Sampling uses the existing shutdown budget and retains cancelled native
work through the existing blocking-task owner.

The overview dashboard distinguishes unavailable/stale samples from zero lag and shows source
delivery age separately. Checkpoint-stall guidance uses the existing successful-completion counter
for explicitly selected periodically checkpointed pipelines, preserving readiness semantics.
Reader lag may include offset gaps and uncommitted transactions; these signals do not measure
settled SQL progress, committed recovery offset lag or external sink visibility.

Eight focused regressions passed, including real librdkafka MockCluster writes while the reader
is paused, each assignment-generation fence, revocation, failed lookup, bounded close, startup
wiring and source replacement. The initial focused run caught a missing canonical source name
in the test fixture; the corrected fixture uses the existing validated startup contract.

Validation: `cargo test --workspace --lib --bin laminardb --test cluster_tls_integration --locked
--offline -j1 -- --test-threads=2` with `RUST_MIN_STACK=4194304` passed **6,015 tests, zero failed,
one ignored**. Both Clippy gates, nightly formatting, readability, locked metadata, analytical
dependency generations and whitespace checks passed. Final Clippy cleanup only renamed a test
binding. Dashboard JSON, all 60 dashboard/documentation PromQL expressions and local links were
validated. Independent source/dependency review found no blocking issue. Logs are under
`target/s4-remaining/`; the query validator is under `target/s5-query-validation/`.

No coordinator or core-operator code changed, so no hot-path performance claim is made. MockCluster
is local protocol evidence, not production broker qualification; the metadata-query overhead at
production partition counts remains S12 work. The existing ignored ONNX model test, cached OpenSSL
debug-symbol warnings and proc-macro future-compatibility notice are unchanged. Next is B0 before S6.

### B0 — Windows timing baseline (2026-09-19)

Started from `42657355`. Reuse `latency_bench` and the existing `stream_executor_bench` cases
`plain_select`, `agg_group_by`, `sort_limit` and `query_chain`. The SQL diagnostic uses one local,
ephemeral DB, 1,024-row batches with four numeric columns and one short string column, four uniformly
distributed region keys, and the in-process source/subscription path. Input is closed-loop:
push a batch, then wait for output. This does not measure an independently offered event rate,
external sink visibility, durable recovery or Kafka overhead. Checkpointing is disabled and no
external object store is involved. Production latency/recovery ceilings and an RSS envelope remain unset.

Host inventory records an AMD Ryzen 9 7900X (12 cores / 24 logical processors), approximately
31 GiB usable RAM, Windows 11 Pro build 26200 and Rust 1.98.0. Use the same optimized bench profile
with `CARGO_PROFILE_BENCH_DEBUG=1` and `CARGO_PROFILE_BENCH_STRIP=none` for before/after runs, so
profiles retain symbols. Builds and benchmark runs use `RUST_MIN_STACK=4194304`.
Raw build/host evidence is under `target/b0/`.

The original SQL smoke run failed source-schema admission: the fixture registered a two-column mock
connector for a five-column source, then pushed batches through the separate embedded input path.
The four selected cases now use the existing in-process source directly, create Tokio's timeout
inside its runtime and reuse one warmed pipeline. Setup and graceful shutdown are outside timing;
each measured iteration includes the shallow input clone, push, scheduling and first output decoding.
This avoids concurrent fixtures and timed teardown from the previous batched harness. The separate
high-cardinality cases are unchanged and excluded from this baseline. All four corrected optimized
smoke cases passed. All-features/all-targets Clippy, nightly formatting and readability passed;
the earlier workspace and no-default-features gates cover the unchanged production code.
No engine execution code or dependency changed.

Uninstrumented Criterion runs completed against source commit
`54b551b14f7bb8b36b0ab7504e8a7c0f48a66c70`, using the matching working-tree binaries built before
that commit. `target/b0/baseline-identity.json` records executable paths, SHA-256 hashes and run time.
Raw samples and estimates are saved under `target/criterion/**/s6-before/`; the host remains on its
existing Balanced power scheme. Each case collected 100 samples after a three-second warmup, with
a five-second measurement target automatically extended for the slower SQL cases.

| Diagnostic | Criterion mean | 95% confidence interval |
|---|---:|---:|
| Tumbling-window assignment | 1.414 ns | 1.409–1.421 ns |
| Projection/filter, 1,024 input rows | 0.921 ms | 0.795–1.051 ms |
| Four-group aggregate, 1,024 input rows | 1.476 ms | 1.265–1.702 ms |
| Sort/top ten, 1,024 input rows | 1.161 ms | 0.989–1.336 ms |
| Three-query chain, 1,024 input rows | 1.434 ms | 1.285–1.584 ms |

These are mean estimates from `estimates.json`, not Criterion's displayed regression slopes or
event-latency percentiles. SQL intervals are too wide to resolve a 5% regression confidently;
repeat matched before/after measurements on a stable target host before accepting hot-path changes.
The window-assignment microbenchmark does not measure full event processing.

The reproducible commands, with the two executable paths from `target/b0/executables.json`, are:

```text
cargo bench -p laminar-core -p laminar-db --no-default-features --bench latency_bench --bench stream_executor_bench --no-run --message-format=json --locked --offline -j1
<latency_bench.exe> --bench --noplot --save-baseline s6-before
<stream_executor_bench.exe> --bench "^(plain_select|agg_group_by|sort_limit|query_chain)/" --noplot --save-baseline s6-before
```

CPU/IPC and allocation profiles were not captured in this Windows run. WPR rejected the named
CPU/PMC capture with `0xc5585011` (could not enable the profiling policy), and image-specific heap tracing with
`0x80070005` (access denied). The execution token lacks the profiling privilege. Final checks show
the task's WPR instance idle and heap tracing disabled; no profile workload ran. Capture logs and
the validated PMC profile were recorded under `target/b0/`. This timing-only run did not close B0.
The Linux follow-up below supplies diagnostic profiles. The previous ignored Windows evidence
directories were no longer present at that follow-up; the numbers above remain historical results.
No production workload targets or memory defaults have been inferred from them.

The memory ownership contract for the later serial work is deliberately limited to existing owners:

| Domain | Admission, ownership and release | Current bound / transient gap |
|---|---|---|
| Connector queue | Source actor transfers a batch through `SourceMsg`; dequeue transfers ownership to the coordinator, not necessarily to free memory. | Count-bounded plus a shared 64 MiB Arrow-byte budget, retained through parking and released at staging/discard. Each source may additionally hold one validated waiting batch up to the same limit; connector decode scratch is outside this budget. |
| Embedded input | `SourceEntry::push_and_buffer` admits into the core source channel and retains snapshot/broadcast references until their owners release them. | Count limits do not cover arbitrary Arrow width or all retained snapshots. S8 owns this separate path. |
| Parked/staged cycles | The coordinator retains parked messages and cycle buffers until execution, retry, recovery or cleanup resolves them. Cursor settlement follows successful publication. | These retained references outlive dequeue; a released queue permit cannot serve as their memory budget. |
| Graph ports | `OperatorGraph` admits and retains input/output batches, then releases port ownership when consumed or cleared. | Existing Backpressure/Fail/BestEffort shedding applies; pre-route current-usage checks can overshoot on the next batch. S9 owns prospective admission and fan-out treatment. |
| DataFusion reservations | The per-DB runtime pool owns participating reservations until the consumer releases/drops them; main, graph and auxiliary contexts share it. | S6b adds a 256 MiB configurable fallible-reservation limit and disables DB-context spilling. Direct Arrow/expression allocations remain outside reservations. |
| Tables/MVs | Stores retain live keys/rows through upsert, refresh, publication and restore; replacement/delete or configured append retention releases them. | No general live-byte quota; shared Arrow slices can retain large allocations. S10/S11 own preflight and failure atomicity. |
| Checkpoint scratch | Capture retains immutable frames while background serialization/persistence overlaps live state, then releases them on completion/cleanup. | Existing checkpoint data limits do not establish a whole-process or transient scratch-memory allowance. |

No values in this ownership inventory are an additive RSS guarantee. Establish measured headroom
before selecting memory defaults; keep the S6 pool scoped to participating reservations.

### B0 — Linux timings, profiles and retained-plan finding (2026-09-20)

Measured production/benchmark source at `9bb1e996f9ff2b7287d2decc80d55d3b482bda4b`, with only
documentation edits during the run. The existing Ubuntu 24.04.4 WSL2 environment supplies
user-process profiling without changing kernel profiling permissions. It exposes 24 logical CPUs
on the same Ryzen 9 7900X, approximately 15.2 GiB RAM and 4 GiB swap. Windows reported Balanced
power at run start. Rust 1.95.0 built the locked graph with one build job, opt-level 3, thin LTO,
one codegen unit, debug level 1, no debug stripping and `RUST_MIN_STACK=4194304`. This is a benchmark build
at the declared MSRV, not an all-feature MSRV qualification.

Reused the workload described above. All five optimized smoke cases passed. Each timing case
collected 100 samples after a three-second warmup; SQL measurement targets were twenty seconds,
and the assignment target was five seconds. Timings ran before profiling, with no concurrent build.
The table reports Criterion **means and 95% confidence intervals**, not regression slopes or
per-event percentiles. Different OS/toolchain and source identities prevent comparison with the
Windows numbers as a regression/improvement claim.

| Diagnostic | Mean | 95% confidence interval | User-process IPC | Profile peak heap, MB |
|---|---:|---:|---:|---:|
| Tumbling-window assignment | 5.057 ns | 5.030–5.082 ns | — | — |
| Projection/filter, 1,024 rows | 136.15 µs | 134.53–137.82 µs | 0.453 | 1.47 |
| Four-group aggregate, 1,024 rows | 144.17 µs | 142.66–145.67 µs | 1.134 | 1.49 |
| Sort/top ten, 1,024 rows | 150.69 µs | 148.40–153.24 µs | 0.390 | 16.74 |
| Three-query chain, 1,024 rows | 161.12 µs | 158.79–163.89 µs | 0.646 | 40.49 |

Perf 6.8.12 recorded grouped user cycles/instructions for requested fifteen-second SQL profiles;
both counters report 100% running time. These aggregate process counters include runtime/harness
work under virtualization. They are below the IPC > 2 heuristic and do not establish an optimized
compute kernel. Kernel scheduling counters were excluded; their reported zeros are not evidence
of no context switches. Separate CPU stack captures use 16 KiB DWARF stacks at 199 Hz. The first
chain capture lost samples and is superseded by a 99 Hz repeat. All four accepted captures report
zero lost samples and contain resolved application stacks. Perf writes on the Windows mount
failed; captures succeeded on Linux's native filesystem and were copied into the workspace.

[Heaptrack](https://github.com/KDE/heaptrack) 1.5.0 profiled the same SQL cases separately with a
requested five-second duration. The table's rounded decimal MB values measure intercepted heap
allocations across fixture setup, warmup, execution and teardown using the system allocator.
They are not DataFusion reservation totals or production RSS limits. The uninstrumented SQL timing
process peaked at **165,056 KiB RSS**; this short diagnostic does not establish a steady plateau.

**New G2 evidence:** a fifteen-second chain allocation profile peaked at **143.27 MB**, compared
with 40.49 MB in the five-second profile. Its exported live-heap timeline rises through roughly
40, 61, 82, 105 and 127 MB during the long execution, then falls to about 134 KB on teardown.
Retained stacks point to DataFusion metric labels/values and their registry under cached projection
and sort execution. Source inspection matches the observation: `execute_cached_plan` repeatedly
collects the same plan; `ProjectionExec::execute` registers new metrics, and
`ExecutionPlanMetricsSet::register` appends to its shared set. This is retained execution state
until the plan is released, outside participating memory reservations. S6 therefore starts with
a focused reuse repair before adding the pool. No engine fix or safe memory default is claimed here.

Raw samples, profiles, reports, exact commands, hashes and the retention timeline are under
`target/b0-wsl/`; `summary.json` identifies the accepted and superseded CPU reports. The native
build/profiles remain in `/home/sujit/.cache/laminardb-b0-9bb1e996` inside Ubuntu. Executable hashes:

- `stream_executor_bench-a84282b6a7c6d167`: `7b478bdb422666695b3d79aff3c122b3aa8cac2de114921332652dbb17a8fd41`.
- `latency_bench-c15ce6117c8edbe1`: `41cf9196f2b4a8f65dd4ee6e693c4f53c2859b059bdea0e4cadf1eb7f66d0b9a`.

Reproduction uses the prior build command with `cargo +1.95.0`, the documented profile variables
and native `CARGO_TARGET_DIR`. Run the two binaries with `--bench --noplot --save-baseline
s6-before-wsl`; add the existing four-case SQL filter and `--measurement-time 20` for SQL.
Profiles use that filter narrowed to one case with `--profile-time 15` (perf) or `5` (heaptrack).
Refresh the matched Linux baseline immediately before S6; retain the >5% regression gate and
repeat uncertain measurements. Production workload targets, external visibility, recovery,
other feature combinations and shipped-platform qualification remain S12/S13 work.

All 32 recorded benchmark/capture/export commands exited successfully; trace quality review rejected
the lossy first chain capture despite its zero exit status. Formatting, readability, analytical
dependency, local-link and whitespace checks passed. This follow-up changes documentation only;
prior workspace test/Clippy
results are historical, and no new full-workspace regression run is claimed.

### S6a — release cached execution state between batches (2026-09-20)

Implemented for all DB modes. Cached plans remain unexecuted templates; each collection uses
DataFusion's [`reset_plan_states`](https://github.com/apache/datafusion/blob/53.1.0/datafusion/physical-plan/src/execution_plan.rs)
to give metrics and join build state one execution lifetime, including errors and cancellation.
Live source slots stay shared. All SQL, aggregate/window pre-projection and post-projection cache
paths use the same execution helper. No dependency, public setting or cache framework was added.

Preparation disables dynamic-filter pushdown only in its planning-state copy and rejects recursive
plans after view expansion. Direct file scans are also rejected because DataFusion 53.1's
[`DataSourceExec`](https://github.com/apache/datafusion/blob/53.1.0/datafusion/datasource/src/source.rs)
returns the same leaf on reset, retaining file-source metrics. Connector I/O supplies live Arrow
batches to streaming plans; ordinary one-shot DataFusion queries keep their existing behavior.

**Correctness:** nine new regressions cover changing ascending/descending Top-K inputs, live join
inputs, running aggregates, window closure, compiled fallback after an error, repeated query errors,
cancellation and subsequent execution, and recursive/file-plan rejection without changing ad-hoc
queries. The workspace library suite with cluster features passed **5,684 tests, 0 failures,
1 ignored** (the existing external ONNX-model test). Both required Clippy configurations, nightly
formatting, readability (19 module / 195 function exceptions), analytical dependency and whitespace
checks passed. The diff review found no new unused code, unnecessary abstraction or unrelated cleanup.

**Performance:** refreshed the existing Linux baseline before editing and preserved both binaries.
Same WSL host, Rust 1.95.0, bench profile, five workloads and 100-sample protocol as B0; builds were
finished before measurements. Initial Criterion means:

| Workload | Before | After | Change |
|---|---:|---:|---:|
| Window assignment | 4.872 ns | 5.107 ns | +4.83% |
| Plain select | 131.49 µs | 137.69 µs | +4.72% |
| Four-group aggregate | 142.97 µs | 150.00 µs | +4.91% |
| Sort / top 10 | 143.37 µs | 175.83 µs | +22.64% |
| Three-query chain | 157.17 µs | 163.24 µs | +3.87% |

The sort result exceeds the 5% gate and is retained as an **explained correctness cost**, not a
sub-5% performance claim. Two matched repeats, with reversed run order, measured sort changes of
**+4.23%** (167.59 → 174.69 µs) and **+15.29%** (153.48 → 176.95 µs). The unchanged select control
varied −4.07% / +4.92%; even the identical core binary varied +4.83% in the initial comparison.
CPU profiles attribute 28.81% of post-fix samples to Top-K heap maintenance. An independent probe
confirmed that the old cached plan returned `[1000, 900]` for one batch, then incorrectly returned
no rows for `[100, 90, 80]`; resetting returned `[100, 90]`. The old retained cutoff skipped valid
sorting work. Keeping that shortcut would preserve incorrect results. This does not establish a
production latency budget; target-workload qualification remains S12.

Heaptrack peaks changed from **16.74 MB → 1.66 MB** for sort and **40.49 MB → 1.73 MB** for the
five-second chain profile. The longer chain profile changed from **143.27 MB → 1.73 MB**; sampled
live heap stayed near 1.49–1.71 MB after warmup and returned to 134 KB at teardown. Participating
reservation limits and whole-process memory bounds are still separate work. Both 99 Hz CPU captures
had zero lost samples; process IPC ranged 0.47–1.33 on WSL, below the 2.0 kernel guideline.

Evidence is in `target/s6-cache/`: commands, test/Clippy logs, Criterion samples and confidence
intervals, CPU/heap traces, the isolated Top-K probe, source hashes and `summary.json`. The final
SQL benchmark SHA-256 is `65a2b170a048949b7553d20715f9cfbf625292adf0d35fe09acda6a56291fd19`;
the core binary is unchanged from B0. Starting HEAD was `23666ebf`. Native binaries and profiles
remain under `/home/sujit/.cache/laminardb-b0-9bb1e996/s6-cache` for the next bounded change.

### S6b — share bounded DataFusion reservations (2026-09-20)

**Status:** implemented and verified locally. Starting HEAD:
`07d0e6b45b001732274ab81a9d6387d7bc88b7b5`; result is an uncommitted diff.

All DB modes now create one DataFusion 53.1 `GreedyMemoryPool` per `LaminarDB`, shared through
the runtime used by main queries, connector operator graphs, sink-filter contexts and the
cluster local-table diagnostic. New graph generations retain the same pool. Separate DBs have
separate budgets. Catalogs remain separate where they were previously separate.

`LaminarConfig::datafusion_memory_limit_bytes`, the matching builder method, and
`[server].datafusion_memory_limit_bytes` select the finite limit; the default is **256 MiB**.
Zero is rejected, including direct server/cluster startup before discovery or lease acquisition.
Changing the server setting requires restart. DB-owned contexts use `DiskManagerMode::Disabled`,
including ad-hoc queries. Participating allocation failures retain DataFusion's resource error
or the existing `DbError::QueryPipeline`; translation identifies resource exhaustion as query
execution failure (`LDB-9001`) rather than an internal bug.

The budget covers fallible DataFusion reservations, not every Arrow allocation or process RSS.
Queues, managed state, tables/MVs and checkpoint scratch remain separately owned. Connector-owned
I/O contexts, standalone `laminar-sql` factories and its thread-local lambda context retain their
existing defaults and are explicitly outside the per-DB scope. The 256 MiB policy does not establish
a production memory envelope or close G2. S7–S11 and S12 workload sizing remain separate work.

Before implementation, regressions demonstrated that a default DB admitted a reservation larger
than the proposed cap and enabled temporary spill files. Fourteen new tests cover default/explicit
limits, independent DBs and concurrent contexts, real sort/aggregate/join exhaustion, cached-plan
retry and cancellation with live reservations, the cluster diagnostic, connector-graph failure
source reporting and reconstruction, configuration/startup validation, and error translation.
All **6,040 workspace library/server tests passed, zero failed, one ignored** (the existing
external ONNX-model test). Both Clippy configurations with `-D warnings`, nightly formatting,
readability, analytical-dependency and whitespace checks passed. Tests used one Cargo build job,
two test threads and `RUST_MIN_STACK=8388608`; existing cached OpenSSL debug-symbol warnings did
not prevent linking. No production cursor/recovery behavior or cluster SQL admission was changed.

**Performance:** same WSL host, Rust 1.95.0, release settings, five workloads and 100-sample
protocol as S6a, with a refreshed baseline before implementation. SQL measurement targets were
20 seconds, with three-second warmups; builds and profiling did not overlap measurements.
The table reports Criterion means; raw estimates include their 95% confidence intervals.

| Workload | Before | After | Change |
|---|---:|---:|---:|
| Window assignment | 5.160 ns | 5.107 ns | −1.02% |
| Plain select, 1,024 rows | 137.42 µs | 139.06 µs | +1.19% |
| Four-group aggregate, 1,024 rows | 149.71 µs | 151.96 µs | +1.51% |
| Sort / top 10, 1,024 rows | 179.24 µs | 178.00 µs | −0.69% |
| Three-query chain, 1,024 rows | 164.28 µs | 169.45 µs | +3.15% |

The chain's initial approximate 95% change interval overlapped the 5% gate, so it and the select
control were repeated with reversed run order and 30-second measurement targets. The chain
measured **170.05 → 168.02 µs (−1.19%)**, with an approximate change interval of **−2.86% to +0.48%**;
select measured **141.82 → 143.18 µs (+0.96%)**. No measured mean exceeded the 5% gate. This is local
diagnostic evidence, not a production latency or RSS qualification. The unchanged `hot_path_micro`
kernels passed optimized smoke checks; they do not construct a DataFusion runtime and do not
measure this pool change. No record-path kernel, queue admission or fan-out implementation changed.

Separate Heaptrack profiles measured peak heap **1.66 → 1.63 MB** for sort and **1.73 → 1.69 MB**
for the 15-second chain; both returned to the same roughly 134 KB process teardown remainder as
their before runs. These figures include fixture/runtime allocations and are not pool accounting.
Four 99 Hz CPU captures had zero lost samples. Grouped user counters ran 100% of the requested
time: sort IPC **1.286 → 1.284**, chain **0.657 → 0.678**. These virtualized process counters include
runtime/harness work and remain below the IPC > 2 kernel guideline; no kernel optimization or
target-hardware qualification is claimed.

`target/s6-memory/` contains commands, source/binary hashes, regression and gate logs, Criterion
samples/intervals, CPU/heap traces and `summary.json`. Native artifacts remain under
`/home/sujit/.cache/laminardb-b0-9bb1e996/s6-memory`. The before binaries were preserved from S6a and
verified by hash. The after SQL benchmark SHA-256 is
`a6cb9365e40cc25eb4dafaebf6e546bd8b165c34ae19653b10cd1a1d4a4f4ed3`; the core binary is unchanged.
The source/diff review found no new dependencies, per-row bookkeeping, unused abstractions or
unrelated edits. The next serial session is S7; release qualification and remaining S4 findings
remain open.

### S7 — bound connector-to-coordinator queued bytes (2026-09-20)

**Status:** implemented and verified locally; correctness gates pass and the final matched timing
means have no regression above 5%.
Starting HEAD: `014b997f07ffcef443f95da978e4a2a07fdebe3d`.
**Modes:** all connector pipelines in embedded, single-node and cluster deployments. Cluster
SQL/delivery admission, checkpoint formats and committed cursor semantics remain unchanged.

`LaminarConfig::source_queue_max_bytes`, its builder method, `PipelineConfig::source_queue_max_bytes`
and `[server].source_queue_max_bytes` configure a **64 MiB** default shared by all source senders
in one coordinator generation. The existing 64-message default also remains in force. Zero and
values above the platform semaphore/u32 range (`MAX_SOURCE_QUEUE_BYTES`) fail before connector
startup; both server entry points validate before discovery/leases. Server changes require restart.

The source channel owns one Tokio byte semaphore. An owned permit travels with each queued
message through dequeue and intake parking until staging or discard. Closing the receiver wakes
byte waiters even when a parked message still holds capacity. Normal sends, pending-cursor sends
and both shutdown-tail `try_send` paths share admission. A batch larger than the entire budget
fails promptly, before a pending cursor can retain it. Refused input never advances the
coordinator's recovery cursor. Cancelled acquisition, failed count admission and dropped messages
return their charges. Barriers remain in the mixed FIFO and bypass only the data-byte semaphore;
per-source ordering and existing bounded checkpoint/shutdown failure paths are preserved.

Accounting uses Arrow-reported retained array storage plus fixed batch/column charges. Slices,
views and nested arrays retain their backing storage in this accounting; aliases are charged
independently, without a per-row allocator or deduplication map. Each source can additionally
hold one validated batch while waiting for capacity or a cursor, up to the configured limit.
Connector decode scratch and schema/cursor metadata are outside this queue charge. Staging
transfers Arrow ownership into the cycle/graph; it does not prove that storage was freed.
Embedded push rings, staged/graph buffers, replay, tables/MVs, sink buffers and checkpoint scratch
remain separate owners. S8/S9 and the remaining memory/qualification sessions are still required.

**Correctness:** the pre-change regression admitted a batch exceeding 64 MiB. Eighteen new tests
cover configuration/startup in each mode, oversized data and deferred cursor capture, parked input
through actual coordinator staging, multiple producers with a slow consumer, FIFO barriers while
bytes are saturated, partial reservation cancellation, lease loss, shutdown-tail admission,
receiver closure and backing storage retained by wide slices/views. Targeted validation passed
**404 tests**. All **6,058 workspace library/server tests passed, zero failed, one existing ignored**
ONNX-model test. Both Clippy configurations with `-D warnings`, nightly formatting, readability,
analytical-dependency and whitespace checks passed. Builds used one Cargo job, two test threads
and `RUST_MIN_STACK=8388608`; cached OpenSSL debug-symbol warnings did not prevent linking.

**Performance:** standard core/SQL baselines were refreshed before implementation. Final SQL
comparisons use the same B0 WSL/Linux host, Rust 1.95.0, no default features, optimized builds
with debug symbols, 100 Criterion samples, a 3-second warmup and a 30-second measurement target.
Candidate runs precede baseline runs; builds, timings and profiles run serially. The unchanged
core benchmark binary measured 5.368 → 5.176 ns in the initial matched check; optimized core and
hot-path kernel smoke checks passed.

Three new public-API burst cases exercise narrow rows, 4 KiB strings and four sources. Each
sends 64 batches of 256 rows per source and waits for every row at the subscription. Setup and
shutdown are untimed; input clones share Arrow backing storage. A nullable-schema mismatch was
corrected in both fixtures. The wide fixture also explicitly retains 128 MiB of output in both
versions: its original 16 MiB live-log default could evict unread output from a 64 MiB burst,
under tracing and during a longer timing run. Those incomplete runs are retained as invalid
evidence. Both rebuilt versions use the identical corrected fixture, all 64 batches and the
same source-admission configuration. The fixture's output retention is separate from S7's budget.

| Criterion mean | Before | After | Change |
|---|---:|---:|---:|
| Plain SELECT, 1,024 rows | 142.39 µs | 140.88 µs | −1.06% |
| GROUP BY, 1,024 rows / 4 groups | 154.41 µs | 151.67 µs | −1.77% |
| Sort / top 10, 1,024 rows | 182.92 µs | 181.03 µs | −1.03% |
| Three-query chain | 168.75 µs | 168.23 µs | −0.31% |
| Narrow burst | 174.33 µs | 177.00 µs | +1.53% |
| Wide burst with 128 MiB output history | 10.310 ms | 6.723 ms | −34.79% |
| Four-source burst | 437.32 µs | 431.71 µs | −1.28% |

The initial four-source (+33.4%) and aggregate (+15.5%) slowdowns did not recur in this longer
matched comparison. Raw samples, mean confidence intervals and approximate change intervals
are retained. The wide case has substantial variance; its observed improvement is diagnostic,
not a promised speedup. No production code was changed to make a timing pass.

CPU and heap captures passed for four-source bursts, wide bursts and GROUP BY in both versions.
CPU sampling used 49 Hz DWARF stacks; final reports disable the failing inline-symbol lookup.
Some frames remain unresolved, so attribution is qualitative. Arrow concatenation dominates
the burst profiles (about 56% and 91–93% inclusive CPU respectively). Source publication is
about 1.5% in the candidate four-source profile. IPC remains below the >2 heuristic in both
versions: 0.41 → 0.55 for four sources, 0.17 → 0.17 for wide bursts and 1.16 → 1.16 for GROUP BY.

Heaptrack peak allocated memory was 19.79 → 20.34 MB for four sources, 149.23 → 191.22 MB for
wide bursts and 1.84 → 1.84 MB for GROUP BY. These fixed-time profiles execute different amounts
of work and include connector concatenation, output history and runtime allocations. The wide
heap increase is retained in the evidence; a source-queue limit does not constrain those other
owners. Instrumented RSS also includes profiler overhead. These local diagnostics do not qualify
a production RSS or external-visibility envelope. S8/S9 and workload qualification remain necessary.

Commands, gate/regression logs, source/binary identities, raw Criterion samples and profiles are
under `target/s7-queue/`, including `final-summary.json`, `final-perf-commands.json` and final
source/binary hashes. The pre-change source snapshot and corrected fixture are preserved there;
native binaries remain under `/home/sujit/.cache/laminardb-b0-9bb1e996/s7-queue`. Final candidate
SQL benchmark SHA-256: `a256a6042c7db3fe6f1503c262838b76e4d79b975c3865fb9b0b551c96ab836b`.
All 36 final build/smoke/timing/profile commands passed. Runtime source hashes are unchanged
since the correctness gates; formatting and all-target Clippy passed again after the fixture fix.
The final diff review found no dependencies, readability exceptions, per-row bookkeeping or
unrelated changes. The next serial session is S8. S4 dependency findings and production
release/upgrade qualification remain open.
