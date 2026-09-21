use std::fs::File;
use std::io::{BufWriter, Write as _};
use std::path::Path;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::{ensure, Context as _, Result};
use serde::Serialize;

use super::super::{prometheus_histogram_latency, Node};
use super::load::Counts;
use super::spec::Spec;

#[derive(Serialize)]
pub(super) struct Sample {
    pub seconds: f64,
    pub generation: u64,
    pub offered: u64,
    pub enqueued: u64,
    pub acknowledged: u64,
    pub observed: u64,
    pub backlog: u64,
    pub rss_bytes: Option<f64>,
    pub cycle_p50_p95_p99_ms: Option<[f64; 3]>,
    pub checkpoint_p99_ms: Option<f64>,
    pub checkpoint_stall_p99_ms: Option<f64>,
}

pub(super) struct Evidence {
    raw: BufWriter<File>,
    pub samples: Vec<Sample>,
}

#[derive(Serialize)]
pub(super) struct Summary {
    pub samples: usize,
    pub peak_rss_bytes: Option<f64>,
    pub rss_growth_bytes_per_second: Option<f64>,
    pub backlog_growth_rows_per_second: Option<f64>,
    pub max_backlog_rows: u64,
    pub checkpoint_p99_ms: Option<f64>,
}

impl Evidence {
    pub fn new(directory: &Path) -> Result<Self> {
        Ok(Self {
            raw: BufWriter::new(File::create(directory.join("metrics.jsonl"))?),
            samples: Vec::new(),
        })
    }

    pub fn event(&mut self, elapsed: Duration, event: serde_json::Value) -> Result<()> {
        writeln!(
            self.raw,
            "{}",
            serde_json::json!({
                "seconds": elapsed.as_secs_f64(), "elapsed_ns": elapsed.as_nanos(), "event": event
            })
        )?;
        self.raw.flush()?;
        Ok(())
    }

    pub fn sample(
        &mut self,
        spec: &Spec,
        node: &Node,
        counts: &Counts,
        elapsed: Duration,
    ) -> Result<()> {
        let offered = spec.offered(elapsed);
        let observed = counts.observed.load(Ordering::Acquire);
        let enqueued = counts.enqueued.load(Ordering::Acquire);
        let acknowledged = counts.acknowledged.load(Ordering::Acquire);
        let metrics = node.http_get("/metrics");
        let histogram = |name: &str| {
            metrics
                .as_ref()
                .and_then(|body| prometheus_histogram_latency(body, name).ok())
        };
        let rss_bytes = metrics.as_ref().and_then(|body| {
            body.lines().find_map(|line| {
                let rest = line.strip_prefix("laminardb_process_resident_memory_bytes")?;
                if !rest.starts_with(['{', ' ']) {
                    return None;
                }
                rest.split_whitespace()
                    .last()?
                    .parse::<f64>()
                    .ok()
                    .filter(|v| v.is_finite() && *v > 0.0)
            })
        });
        let sample = Sample {
            seconds: elapsed.as_secs_f64(),
            generation: node.process_generation,
            offered,
            enqueued,
            acknowledged,
            observed,
            backlog: offered.saturating_sub(observed),
            rss_bytes,
            cycle_p50_p95_p99_ms: histogram("laminardb_cycle_duration_seconds_bucket").map(|h| {
                [
                    h.p50_upper_seconds * 1_000.0,
                    h.p95_upper_seconds * 1_000.0,
                    h.p99_upper_seconds * 1_000.0,
                ]
            }),
            checkpoint_p99_ms: histogram("laminardb_checkpoint_duration_seconds_bucket")
                .filter(|h| spec.limits.is_none() || h.observations >= 100)
                .map(|h| h.p99_upper_seconds * 1_000.0),
            checkpoint_stall_p99_ms: histogram(
                "laminardb_checkpoint_pipeline_stall_duration_seconds_bucket",
            )
            .map(|h| h.p99_upper_seconds * 1_000.0),
        };
        writeln!(
            self.raw,
            "{}",
            serde_json::json!({"sample":sample,"prometheus":metrics})
        )?;
        self.raw.flush()?;
        self.samples.push(sample);
        Ok(())
    }

    pub fn summary(&self, spec: &Spec) -> Summary {
        // Exclude warmup and final drain. The second half tests sustained growth after settling.
        let steady: Vec<_> = self
            .samples
            .iter()
            .filter(|s| s.seconds >= (spec.seconds / 2) as f64 && s.seconds < spec.seconds as f64)
            .collect();
        let rss_points: Option<Vec<_>> = steady
            .iter()
            .map(|s| Some((s.seconds, s.rss_bytes?)))
            .collect();
        let backlog: Vec<_> = steady
            .iter()
            .map(|s| (s.seconds, s.backlog as f64))
            .collect();
        let peak_rss_bytes = self
            .samples
            .iter()
            .filter_map(|s| s.rss_bytes)
            .reduce(f64::max);
        // Preserve the final cumulative histogram from every process generation; no restart reset can erase a slow generation.
        let mut generations = std::collections::BTreeMap::new();
        for sample in &self.samples {
            generations.insert(sample.generation, sample.checkpoint_p99_ms);
        }
        let checkpoints: Option<Vec<_>> = generations.values().copied().collect();
        Summary {
            samples: self.samples.len(),
            peak_rss_bytes,
            rss_growth_bytes_per_second: rss_points.as_ref().and_then(|p| slope(p)),
            backlog_growth_rows_per_second: slope(&backlog),
            max_backlog_rows: self.samples.iter().map(|s| s.backlog).max().unwrap_or(0),
            checkpoint_p99_ms: checkpoints.and_then(|values| values.into_iter().reduce(f64::max)),
        }
    }
}

pub(super) fn slope(points: &[(f64, f64)]) -> Option<f64> {
    if points.len() < 3 {
        return None;
    }
    let n = points.len() as f64;
    let x = points.iter().map(|p| p.0).sum::<f64>() / n;
    let y = points.iter().map(|p| p.1).sum::<f64>() / n;
    let denominator = points.iter().map(|p| (p.0 - x).powi(2)).sum::<f64>();
    (denominator > 0.0)
        .then(|| points.iter().map(|p| (p.0 - x) * (p.1 - y)).sum::<f64>() / denominator)
}

impl Summary {
    pub fn check(&self, spec: &Spec, recovery_ms: Option<f64>) -> Result<()> {
        let Some(limits) = &spec.limits else {
            return Ok(());
        };
        ensure!(
            self.peak_rss_bytes.context("RSS unavailable")? <= limits.rss_bytes as f64,
            "RSS ceiling exceeded"
        );
        ensure!(
            self.rss_growth_bytes_per_second
                .context("RSS growth unavailable")?
                <= limits.rss_growth_bytes_per_second,
            "RSS did not plateau within the declared growth ceiling"
        );
        ensure!(
            self.backlog_growth_rows_per_second
                .context("backlog growth unavailable")?
                <= limits.backlog_growth_rows_per_second,
            "backlog did not stabilize within the declared growth ceiling"
        );
        ensure!(
            self.checkpoint_p99_ms
                .context("checkpoint latency unavailable")?
                <= limits.checkpoint_p99_ms,
            "checkpoint p99 ceiling exceeded"
        );
        if spec.fault == super::spec::Fault::ProcessKill {
            ensure!(
                recovery_ms.context("no externally observed recovery")? <= limits.recovery_ms,
                "recovery ceiling exceeded"
            );
        }
        Ok(())
    }
}
