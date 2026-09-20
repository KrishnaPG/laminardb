//! Configuration for `LaminarDB`.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::HashMap;
use std::path::PathBuf;

use laminar_connectors::connector::DeliveryGuarantee;
use laminar_core::streaming::{BackpressureStrategy, StreamCheckpointConfig};

use crate::error::DbError;

/// Default pipeline-wide lower-bound charge allowed for managed operator working state.
///
/// This execution budget is independent of checkpoint storage.
pub const DEFAULT_MAX_MANAGED_STATE_BYTES: usize = 256 * 1024 * 1024;

/// Default per-DB limit for participating `DataFusion` reservations (256 MiB).
/// This does not limit direct Arrow allocations or process RSS.
pub const DEFAULT_DATAFUSION_MEMORY_LIMIT_BYTES: usize = 256 * 1024 * 1024;

pub(crate) fn event_time_max_future_skew_ms(
    skew: std::time::Duration,
) -> Result<i64, &'static str> {
    let skew_ms = i64::try_from(skew.as_millis())
        .map_err(|_| "event_time_max_future_skew exceeds the supported millisecond range")?;
    if !skew.is_zero() && skew_ms == 0 {
        return Err("event_time_max_future_skew must be zero or at least 1ms");
    }
    Ok(skew_ms)
}

pub(crate) fn source_idle_timeout_ms(
    timeout: Option<std::time::Duration>,
) -> Result<Option<u64>, &'static str> {
    let Some(timeout) = timeout else {
        return Ok(None);
    };
    let timeout_ms = u64::try_from(timeout.as_millis())
        .map_err(|_| "source_idle_timeout exceeds the supported millisecond range")?;
    if timeout_ms == 0 {
        return Err("source_idle_timeout must be at least 1ms");
    }
    Ok(Some(timeout_ms))
}

pub(crate) fn temporal_join_idle_history_retention_ms(
    retention: Option<std::time::Duration>,
) -> Result<i64, &'static str> {
    let retention = retention
        .ok_or("temporal_join_idle_history_retention must be configured for temporal joins")?;
    let retention_ms = i64::try_from(retention.as_millis()).map_err(|_| {
        "temporal_join_idle_history_retention exceeds the supported millisecond range"
    })?;
    if retention_ms == 0 {
        return Err("temporal_join_idle_history_retention must be at least 1ms");
    }
    Ok(retention_ms)
}

/// What to do when an operator's input buffer exceeds its cap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BackpressurePolicy {
    /// Defer the producer; sources block on `send`. No data loss.
    #[default]
    Backpressure,
    /// Drop oldest batches; counted in `shed_records_total`.
    /// Available only with [`DeliveryGuarantee::BestEffort`].
    ShedOldest,
    /// Error out the cycle.
    Fail,
}

/// String wrapper whose `Debug` redacts the value, for credentials in [`LaminarConfig`].
#[derive(Clone)]
pub struct SecretString(String);

impl SecretString {
    /// Wrap a secret value.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Borrow the underlying secret. Call only at the point of use.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for SecretString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("\"[REDACTED]\"")
    }
}

/// Auto-restart policy for the fault supervisor (see `LaminarDB::enable_supervision`).
#[derive(Debug, Clone)]
pub struct RestartPolicy {
    /// Max restarts within `window` before the pipeline is left hard-faulted.
    pub max_restarts: usize,
    /// Sliding window over which `max_restarts` is counted.
    pub window: std::time::Duration,
    /// Backoff before the first restart in a window.
    pub initial_backoff: std::time::Duration,
    /// Cap on the exponential backoff.
    pub max_backoff: std::time::Duration,
}

impl Default for RestartPolicy {
    fn default() -> Self {
        Self {
            max_restarts: 5,
            window: std::time::Duration::from_secs(60),
            initial_backoff: std::time::Duration::from_millis(500),
            max_backoff: std::time::Duration::from_secs(30),
        }
    }
}

/// Configuration for a `LaminarDB` instance.
#[derive(Debug, Clone)]
pub struct LaminarConfig {
    /// Shared limit for participating `DataFusion` reservations across this DB's contexts.
    /// Must be greater than zero; defaults to [`DEFAULT_DATAFUSION_MEMORY_LIMIT_BYTES`].
    /// DB-owned contexts cannot spill to disk. Direct Arrow allocations, managed state,
    /// queues, connector-owned contexts and process RSS are outside this budget.
    pub datafusion_memory_limit_bytes: usize,
    /// Streaming channel buffer size.
    pub default_buffer_size: usize,
    /// Backpressure strategy.
    pub default_backpressure: BackpressureStrategy,
    /// Checkpoint directory. `None` = in-memory only.
    pub storage_dir: Option<PathBuf>,
    /// Checkpoint config. `None` = disabled.
    pub checkpoint: Option<StreamCheckpointConfig>,
    /// Emit dirty-only changelogs for keyed non-windowed aggregate materialized views instead of
    /// re-materializing every group each cycle. This is query execution policy, not checkpointing.
    pub incremental_emit: bool,
    /// Cloud checkpoint URL, e.g. `s3://bucket/prefix`.
    pub object_store_url: Option<String>,
    /// Credential/config overrides for the object store.
    pub object_store_options: HashMap<String, String>,
    /// Bearer token presented when forwarding requests to the cluster leader's
    /// HTTP API (set when the server gates `/api/v1` with `console_token`).
    pub http_auth_token: Option<SecretString>,
    /// Delivery guarantee.
    pub delivery_guarantee: DeliveryGuarantee,
    /// Source-to-coordinator channel capacity. `None` = 64.
    pub pipeline_channel_capacity: Option<usize>,
    /// Micro-batch coalescing window. `None` = 5ms connectors / 0 embedded.
    pub pipeline_batch_window: Option<std::time::Duration>,
    /// Drain budget per cycle (ns). `None` = 1ms.
    pub pipeline_drain_budget_ns: Option<u64>,
    /// Per-query budget (ns). `None` = 8ms.
    pub pipeline_query_budget_ns: Option<u64>,
    /// Per-port operator input-buffer cap (batches). `None` = 256.
    pub pipeline_max_input_buf_batches: Option<usize>,
    /// Per-port operator input-buffer cap (bytes). `None` = disabled.
    pub pipeline_max_input_buf_bytes: Option<usize>,
    /// Pipeline-wide managed working-state budget in charged bytes. `None` resolves to
    /// [`DEFAULT_MAX_MANAGED_STATE_BYTES`] when the database is constructed.
    pub pipeline_max_managed_state_bytes: Option<usize>,
    /// Retention contract for right-side history while a temporal join input is idle.
    /// Required only when the pipeline contains a temporal join.
    pub temporal_join_idle_history_retention: Option<std::time::Duration>,
    /// Mark inactive watermarked sources and input channels idle after this duration.
    /// `None` disables automatic idle detection.
    pub source_idle_timeout: Option<std::time::Duration>,
    /// Event timestamps farther ahead of wall clock do not advance source watermarks.
    /// Zero disables the guard.
    pub event_time_max_future_skew: std::time::Duration,
    /// Backpressure policy. [`BackpressurePolicy::ShedOldest`] is `BestEffort` only.
    pub pipeline_backpressure_policy: BackpressurePolicy,
    /// Auto-restart policy applied when supervision is enabled.
    pub restart_policy: RestartPolicy,
    /// Isolate queries that share a source into independent failure domains.
    /// Default off; when off, shared-source queries fault and recover together.
    pub shared_source_isolation: bool,
}

impl LaminarConfig {
    pub(crate) fn validate_and_normalize(&mut self) -> Result<(), DbError> {
        if self.datafusion_memory_limit_bytes == 0 {
            return Err(DbError::Config(
                "datafusion_memory_limit_bytes must be greater than zero".into(),
            ));
        }
        self.source_idle_timeout = source_idle_timeout_ms(self.source_idle_timeout)
            .map_err(|error| DbError::Config(error.to_string()))?
            .map(std::time::Duration::from_millis);
        let future_skew_ms = event_time_max_future_skew_ms(self.event_time_max_future_skew)
            .map_err(|error| DbError::Config(error.to_string()))?;
        self.event_time_max_future_skew =
            std::time::Duration::from_millis(future_skew_ms.unsigned_abs());
        let max_managed_state_bytes = self
            .pipeline_max_managed_state_bytes
            .unwrap_or(DEFAULT_MAX_MANAGED_STATE_BYTES);
        if max_managed_state_bytes == 0 {
            return Err(DbError::Config(
                "pipeline_max_managed_state_bytes must be greater than zero".into(),
            ));
        }
        self.pipeline_max_managed_state_bytes = Some(max_managed_state_bytes);

        if let Some(checkpoint) = self.checkpoint.as_mut() {
            let max_node_data_bytes = checkpoint.max_node_data_bytes.unwrap_or(
                laminar_core::checkpoint::checkpoint_store::DEFAULT_MAX_CHECKPOINT_NODE_DATA_BYTES,
            );
            laminar_core::checkpoint::checkpoint_store::validate_max_checkpoint_node_data_bytes(
                max_node_data_bytes,
            )
            .map_err(|error| DbError::Config(format!("checkpoint.max_node_data_bytes: {error}")))?;
            checkpoint.max_node_data_bytes = Some(max_node_data_bytes);
        }

        self.validate_backpressure_policy()
    }

    pub(crate) fn validate_backpressure_policy(&self) -> Result<(), DbError> {
        let policy = self.pipeline_backpressure_policy;
        if policy == BackpressurePolicy::Backpressure {
            return Ok(());
        }

        let has_count_cap = self.pipeline_max_input_buf_batches.is_none_or(|c| c > 0);
        let has_byte_cap = self.pipeline_max_input_buf_bytes.is_some_and(|b| b > 0);
        if !has_count_cap && !has_byte_cap {
            return Err(DbError::Config(format!(
                "backpressure_policy={policy:?} requires at least one of \
                 pipeline_max_input_buf_batches (>0) or pipeline_max_input_buf_bytes"
            )));
        }

        if policy == BackpressurePolicy::ShedOldest
            && self.delivery_guarantee != DeliveryGuarantee::BestEffort
        {
            return Err(DbError::Config(
                "ShedOldest drops data and supports BestEffort only; at-least-once and \
                 exactly-once delivery require Backpressure or Fail."
                    .into(),
            ));
        }
        Ok(())
    }
}

impl Default for LaminarConfig {
    fn default() -> Self {
        Self {
            datafusion_memory_limit_bytes: DEFAULT_DATAFUSION_MEMORY_LIMIT_BYTES,
            default_buffer_size: 65536,
            default_backpressure: BackpressureStrategy::Block,
            storage_dir: None,
            checkpoint: None,
            incremental_emit: true,
            object_store_url: None,
            object_store_options: HashMap::new(),
            http_auth_token: None,
            delivery_guarantee: DeliveryGuarantee::default(),
            pipeline_channel_capacity: None,
            pipeline_batch_window: None,
            pipeline_drain_budget_ns: None,
            pipeline_query_budget_ns: None,
            pipeline_max_input_buf_batches: None,
            pipeline_max_input_buf_bytes: None,
            pipeline_max_managed_state_bytes: None,
            temporal_join_idle_history_retention: None,
            source_idle_timeout: None,
            event_time_max_future_skew: std::time::Duration::from_millis(
                laminar_core::time::DEFAULT_MAX_FUTURE_SKEW_MS.unsigned_abs(),
            ),
            pipeline_backpressure_policy: BackpressurePolicy::default(),
            restart_policy: RestartPolicy::default(),
            shared_source_isolation: false,
        }
    }
}
