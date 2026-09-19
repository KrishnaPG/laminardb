# Production hardening execution plan

**Status:** S1–S3, S4a/S4b and S5 verified locally; remaining S4 security work is open; B0 preparation is underway; S6–S13 not started.
**Date:** 2026-09-19. **Base:** `b429d0dfd02a435219f1b5977a442da9972e0c3d` (`0.30.0`).
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

**Owns:** configuration and DataFusion runtime/context creation. **Modes:** all DB modes.
**Risk:** medium; query failures and spilling behavior change. **Depends on:** S2 and B0.

Reuse DataFusion 53.1's bounded pool and runtime APIs. Establish the budget scope explicitly and
share the intended per-DB pool with both the main context and the separately built connector
operator-graph context in [operator_graph.rs](../crates/laminar-db/src/pipeline_lifecycle/operator_graph.rs).
Inspect other public context constructors so the documented scope has no unbounded bypass.

Disable disk spilling for execution on the compute runtime using the existing disk-manager API;
the upstream default can use OS temporary files. Do not introduce a new spill subsystem. The
pool governs participating fallible reservations, not every Arrow allocation or whole-process RSS.

**Exit:** expensive real plans hit a typed allocation/query error at the intended reservation
boundary, release reservations on error/cancel, create no compute-path spill files, and preserve
source progress/recovery. Test concurrent contexts against the shared budget and cached-plan
reuse after failure. Baseline and rerun representative DataFusion/coordinator workloads.

### S7 — bound connector-to-coordinator queued bytes (G2)

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
evidence. S1–S3, S4a/S4b and S5 are **implemented and verified locally**. S4 is **partially
implemented**; remaining dependency findings and gate enforcement are unresolved. B0 workload
and tooling inspection is underway. S6–S13 remain **not started**.

## Implementation progress — 2026-09-19

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

### S4 — current dependency triage, enforcement still pending

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

### B0 — local baseline preparation

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
profiles retain symbols. Raw build/host evidence is under `target/b0/`. Timing and allocation/CPU
profiles have not yet been captured.

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

The memory ownership contract for the later serial work is deliberately limited to existing owners:

| Domain | Admission, ownership and release | Current bound / transient gap |
|---|---|---|
| Connector queue | Source actor transfers a batch through `SourceMsg`; dequeue transfers ownership to the coordinator, not necessarily to free memory. | Count-bounded channel; waiting producers retain their batch. S7 adds the byte boundary. |
| Embedded input | `SourceEntry::push_and_buffer` admits into the core source channel and retains snapshot/broadcast references until their owners release them. | Count limits do not cover arbitrary Arrow width or all retained snapshots. S8 owns this separate path. |
| Parked/staged cycles | The coordinator retains parked messages and cycle buffers until execution, retry, recovery or cleanup resolves them. Cursor settlement follows successful publication. | These retained references outlive dequeue; a released queue permit cannot serve as their memory budget. |
| Graph ports | `OperatorGraph` admits and retains input/output batches, then releases port ownership when consumed or cleared. | Existing Backpressure/Fail/BestEffort shedding applies; pre-route current-usage checks can overshoot on the next batch. S9 owns prospective admission and fan-out treatment. |
| DataFusion reservations | A context's runtime pool owns participating reservations until the consumer releases/drops them. Main DB and graph contexts are constructed separately. | Currently unbounded; direct Arrow/expression allocations are outside reservations. S6 shares a bounded pool and disables compute spilling. |
| Tables/MVs | Stores retain live keys/rows through upsert, refresh, publication and restore; replacement/delete or configured append retention releases them. | No general live-byte quota; shared Arrow slices can retain large allocations. S10/S11 own preflight and failure atomicity. |
| Checkpoint scratch | Capture retains immutable frames while background serialization/persistence overlaps live state, then releases them on completion/cleanup. | Existing checkpoint data limits do not establish a whole-process or transient scratch-memory allowance. |

No values in this ownership inventory are an additive RSS guarantee. Establish measured headroom
before selecting memory defaults; keep the S6 pool scoped to participating reservations.
