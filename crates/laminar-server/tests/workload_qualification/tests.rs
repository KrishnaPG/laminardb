use std::sync::atomic::Ordering;
use std::time::Duration;

use super::evidence::slope;
use super::latency::Latency;
use super::load::{Counts, Inputs};
use super::observer::Ledger;
use super::spec::{Distribution, Fault, Limits, Spec};

fn spec() -> Spec {
    Spec {
        hardware: "unit fixture".into(),
        seconds: 20,
        warmup_seconds: 1,
        drain_seconds: 30,
        pipelines: 1,
        rps_per_pipeline: 100,
        partitions: 2,
        payload_bytes: 16,
        keys: 128,
        seed: 42,
        distribution: Distribution::Uniform,
        checkpoint_ms: 500,
        fault: Fault::None,
        limits: None,
    }
}

fn row(spec: &Spec, id: u64) -> serde_json::Value {
    serde_json::json!({"origin":0,"id":id,"key":Inputs::new(spec).key(id),"value":id * 3 + 7,"payload":"X".repeat(spec.payload_bytes)})
}

#[test]
fn qualification_oracle_rejects_corruption_without_advancing_and_counts_alo_duplicates() {
    let spec = spec();
    let mut ledger = Ledger::new(&spec).unwrap();
    let counts = Counts::default();
    let mut value = row(&spec, 0);
    value["value"] = serde_json::json!(99);
    assert!(ledger
        .observe(0, &value, Duration::from_secs(1), &counts)
        .is_err());
    assert_eq!(ledger.unique_rows, 0);
    let value = row(&spec, 0);
    assert!(ledger
        .observe(0, &value, Duration::from_secs(1), &counts)
        .unwrap());
    assert!(!ledger
        .observe(0, &value, Duration::from_secs(2), &counts)
        .unwrap());
    assert_eq!(counts.observed.load(Ordering::Relaxed), 1);
    assert_eq!(counts.frontiers[0].load(Ordering::Relaxed), 1);
    assert!(ledger
        .observe(0, &row(&spec, 2_000), Duration::from_secs(21), &counts)
        .is_err());
}

#[test]
fn qualification_missing_record_cannot_be_hidden_by_later_rows_or_another_pipeline() {
    let mut spec = spec();
    spec.pipelines = 4;
    let mut ledger = Ledger::new(&spec).unwrap();
    let counts = Counts::default();
    ledger
        .observe(0, &row(&spec, 1), Duration::from_secs(1), &counts)
        .unwrap();
    let mut sibling_row = row(&spec, 0);
    sibling_row["origin"] = serde_json::json!(1);
    ledger
        .observe(1, &sibling_row, Duration::from_secs(1), &counts)
        .unwrap();
    assert_eq!(counts.frontiers[0].load(Ordering::Relaxed), 0);
    ledger
        .observe(0, &row(&spec, 0), Duration::from_secs(1), &counts)
        .unwrap();
    assert_eq!(counts.frontiers[0].load(Ordering::Relaxed), 2);
    assert_eq!(counts.frontiers[2].load(Ordering::Relaxed), 0);
}

#[test]
fn qualification_cross_routed_rows_cannot_satisfy_another_pipelines_frontier() {
    let mut spec = spec();
    spec.pipelines = 4;
    let mut ledger = Ledger::new(&spec).unwrap();
    let counts = Counts::default();
    let value = row(&spec, 0);
    assert!(ledger
        .observe(1, &value, Duration::from_secs(1), &counts)
        .is_err());
    assert_eq!(ledger.unique_rows, 0);
    assert_eq!(counts.observed.load(Ordering::Relaxed), 0);
    assert_eq!(counts.frontiers[1].load(Ordering::Relaxed), 0);
    assert!(ledger
        .observe(0, &value, Duration::from_secs(1), &counts)
        .unwrap());
}

#[test]
fn qualification_tail_histogram_retains_p999_and_out_of_range_observations() {
    let histogram = Latency::new().unwrap();
    for _ in 0..99_899 {
        histogram.observe(0.001);
    }
    for _ in 0..101 {
        histogram.observe(0.1);
    }
    let result = histogram.distribution();
    assert_eq!(result.samples, 100_000);
    assert!(result.quantile_upper_ms[2].unwrap() < 1.02);
    assert!((100.0..=101.01).contains(&result.quantile_upper_ms[3].unwrap()));
    assert!(result.check(&[2.0, 2.0, 2.0, 90.0]).is_err());
    assert!(result.check(&[2.0, 2.0, 2.0, 102.0]).is_ok());
    let overflow = Latency::new().unwrap();
    overflow.observe(1_200.0);
    assert_eq!(overflow.distribution().quantile_upper_ms, [None; 4]);
}

#[test]
fn qualification_schedule_preserves_stalls_and_explicit_source_pause() {
    let mut spec = spec();
    assert_eq!(spec.scheduled(200), Duration::from_secs(2));
    assert_eq!(spec.offered(Duration::from_secs(5)), 501);
    // Producer acknowledgement is deliberately absent from the offered-load calculation.
    assert_eq!(spec.offered(Duration::from_secs(50)), 2_000);
    spec.fault = Fault::SourcePause;
    assert_eq!(spec.scheduled(999), Duration::from_millis(9_990));
    assert_eq!(spec.scheduled(1_000), Duration::from_secs(15));
    assert_eq!(spec.rows_per_pipeline(), 1_500);
    assert_eq!(spec.offered(Duration::from_secs(12)), 1_000);
    assert_eq!(spec.offered(Duration::from_secs(15)), 1_001);
}

#[test]
fn qualification_limits_require_hour_duration_and_sufficient_per_pipeline_tail_samples() {
    let mut spec = spec();
    spec.limits = Some(Limits {
        visibility_ms: [1.0, 2.0, 3.0, 4.0],
        rss_bytes: 1_000,
        rss_growth_bytes_per_second: 0.0,
        backlog_growth_rows_per_second: 0.0,
        checkpoint_p99_ms: 1.0,
        recovery_ms: 1.0,
    });
    assert!(spec.validate().is_err());
    spec.seconds = 3_600;
    spec.warmup_seconds = 60;
    assert!(spec.validate().is_err());
    spec.seconds = 3_660;
    assert!(spec.validate().is_ok());
    spec.fault = Fault::SourcePause;
    assert!(spec.validate().is_err());
    spec.seconds = 3_665;
    assert!(spec.validate().is_ok());
    spec.rps_per_pipeline = 1;
    spec.pipelines = 4;
    assert!(spec.validate().is_err());
    spec.rps_per_pipeline = 100;
    spec.limits.as_mut().unwrap().visibility_ms[3] = f64::INFINITY;
    assert!(spec.validate().is_err());
}

#[test]
fn qualification_slope_distinguishes_a_plateau_from_sustained_growth() {
    assert_eq!(slope(&[(1.0, 10.0), (2.0, 10.0), (3.0, 10.0)]), Some(0.0));
    assert_eq!(slope(&[(1.0, 10.0), (2.0, 12.0), (3.0, 14.0)]), Some(2.0));
    assert_eq!(slope(&[(1.0, 10.0)]), None);
}

#[test]
fn qualification_recovery_target_requires_an_input_scheduled_after_process_death() {
    let mut spec = spec();
    spec.fault = Fault::ProcessKill;
    for pipelines in [1, 4] {
        spec.pipelines = pipelines;
        let sampled_before_hashing = Duration::from_secs(10);
        let confirmed_dead = Duration::from_millis(11_237);
        let stale_target = spec.offered(sampled_before_hashing) / pipelines as u64;
        let recovery_target = spec.offered(confirmed_dead) / pipelines as u64;
        assert!(spec.scheduled(stale_target) < confirmed_dead);
        assert!(spec.scheduled(recovery_target) > confirmed_dead);
    }
}

#[test]
fn qualification_requested_kill_requires_recovery_even_without_limits() {
    let mut spec = spec();
    spec.fault = Fault::ProcessKill;
    let resources = super::evidence::Summary {
        samples: 0,
        peak_rss_bytes: None,
        rss_growth_bytes_per_second: None,
        backlog_growth_rows_per_second: None,
        max_backlog_rows: 0,
        checkpoint_p99_ms: None,
    };
    let output = super::observer::Observation {
        unique_rows: 0,
        duplicates: 0,
        latency: Vec::new(),
        consumed_offsets: Vec::new(),
        frozen_offsets: Vec::new(),
    };
    assert!(super::check_limits(&spec, &resources, &output, None).is_err());
    assert!(super::check_limits(&spec, &resources, &output, Some(1.0)).is_ok());
    spec.fault = Fault::None;
    assert!(super::check_limits(&spec, &resources, &output, None).is_ok());
}
