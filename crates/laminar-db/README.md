# laminar-db

Unified database facade for LaminarDB. The main entry point that wires the SQL parser, query planner, DataFusion context, streaming infrastructure, and connector registry.

## Key Types

- **`LaminarDB`** -- Main database handle. Manages sources, streams, sinks, and the streaming pipeline lifecycle.
- **`LaminarDbBuilder`** -- Fluent builder for constructing `LaminarDB` with custom configuration, connectors, UDFs, and deployment profiles.
- **`ExecuteResult`** -- Result of executing a SQL statement (DDL, query, rows affected, metadata).
- **`QueryHandle`** -- Handle to a running streaming query with schema and subscription access.
- **`SourceHandle<T>`** / **`UntypedSourceHandle`** -- Typed and untyped handles for pushing data into sources.
- **`TypedSubscription<T>`** -- Subscription to a named stream with automatic RecordBatch-to-struct conversion.
- **`SubscriptionRegistry`** / **`SubscriptionPortal`** -- Broadcast fan-out and per-consumer pump.
- **`CheckpointCoordinator`** -- Seals source/operator state, records the exact durable decision, and hands coordinated external publication to the designated committer.
- **`RecoveryManager`** -- Restores operator state, connector offsets, and watermarks from the latest checkpoint.
- **`Profile`** -- Deployment profile (`BareMetal`, `Embedded`, `Durable`, `Cluster`).
- **`PipelineMetrics`** / **`PipelineCounters`** -- Real-time pipeline observability.
- **`DbError`** -- Structured error type with stable `LDB-NNNN` codes.

## Architecture

This crate sits at the top of the dependency graph, integrating other LaminarDB crates:

```
laminar-db
  |-- laminar-core        (operators, streaming channels, checkpoint barriers, storage)
  |-- laminar-sql         (SQL parsing + DataFusion)
  |-- laminar-connectors  (external connectors)
```

One `StreamingCoordinator` task executes on the dedicated single-threaded `laminar-compute`
runtime. Connector I/O, checkpoint persistence and sink publication run on the main runtime.
This model applies to embedded, single-node and cluster execution.

See the [SQL and delivery boundaries](../../README.md#supported-sql-and-delivery-boundaries) for
mode-specific admission and executable contract examples. Cluster plans deliberately reject local
materialized views and reference-table enrichment; managed direct-source final windows and
certified interval/temporal joins have their own admitted paths. Feature flags and checkpoints
alone do not imply exactly-once delivery.

Local subscriptions use in-memory replay history; cluster subscriptions expose committed,
partition-ordered output only for certified non-windowed keyed aggregates. Neither a separate
snapshot query followed by a subscription nor a client cursor establishes an atomic
snapshot-plus-tail or transactional external-consumer guarantee. See the
[subscription boundaries](../../README.md#ddl) and linked replay tests before designing consumers.

## DataFusion memory limit

Every `LaminarDB` has a shared 256 MiB limit for participating fallible DataFusion reservations.
Set `LaminarConfig::datafusion_memory_limit_bytes` or
`LaminarDB::builder().datafusion_memory_limit_bytes(bytes)` to change it; zero is rejected.
The limit applies in embedded, single-node and cluster modes, per DB instance (per node in
a cluster). Main queries, connector operator graphs, sink-filter contexts and local-table
diagnostics share the budget, including concurrent queries and restarted graph generations.
DB-owned contexts disable disk spilling. Exhaustion returns an allocation/query error;
streaming delivery and recovery use their existing failure handling.

This is a reservation limit, not a process RSS cap. Direct Arrow/expression allocations,
managed operator state, queues, tables/MVs, checkpoint scratch and connector-owned I/O
contexts have separate ownership. Standalone `laminar-sql` factories and its thread-local
lambda evaluation context retain upstream defaults and do not join a DB's pool. The default
is an execution policy, not a qualified production memory envelope; size it for the workload
and leave headroom for allocations outside DataFusion reservations.

## Feature Flags

| Flag | Purpose |
|------|---------|
| `api` | FFI-friendly API module with `Connection`, `Writer`, `QueryStream` |
| `ffi` | C FFI layer with `extern "C"` functions and Arrow C Data Interface (implies `api`) |
| `kafka` | Kafka source/sink connector |
| `postgres-cdc` | PostgreSQL CDC implementation (source admission rejected); also builds the supported `postgres` lookup connector |
| `postgres-sink` | PostgreSQL sink |
| `mongodb-cdc` | MongoDB sink/lookup and CDC implementation (CDC source admission rejected) |
| `delta-lake` | Delta Lake sink and source |
| `delta-lake-s3` / `delta-lake-azure` / `delta-lake-gcs` | Cloud storage backends for Delta Lake |
| `delta-lake-unity` / `delta-lake-glue` | Databricks Unity / AWS Glue catalogs for Delta Lake |
| `delta-lake-all` | All Delta Lake storage backends and catalogs |
| `iceberg` | Apache Iceberg source and sink |
| `websocket` | WebSocket source and sink connectors |
| `files` | File source (AutoLoader) and sink (rolling files) |
| `parquet-lookup` | Parquet schema and codec helpers; no standalone connector |
| `otel` | OpenTelemetry OTLP/gRPC source |
| `cluster` | Distributed mode with gRPC control plane, vnode state, and gossip/static discovery; ALO plus capability-gated EO. |
| `aws` / `gcs` / `azure` | Object-store checkpoint backends (forwards to laminar-core) |

## Related Crates

- [`laminar-core`](../laminar-core) -- Operators, streaming channels, window assigners, checkpoint barriers, storage
- [`laminar-sql`](../laminar-sql) -- SQL parser and DataFusion integration
- [`laminar-connectors`](../laminar-connectors) -- External system connectors
- [`laminar-derive`](../laminar-derive) -- Derive macros for typed data handling

