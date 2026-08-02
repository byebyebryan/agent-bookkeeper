//! Explicit, bounded V1.5 reconciliation plus Hindsight delivery.
//!
//! This is intentionally a one-shot command. Operators invoke it after a raw
//! mirror has changed; it does not install a timer or daemon and it never
//! performs network transport itself.

use std::env;
use std::path::PathBuf;
use std::process;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use agent_bookkeeper::{
    Catalog, CodexRolloutLayout, ControlledDeliveryAttempt, ControlledDeliveryOutcome,
    ControlledRunLimits, DeletionMode, DeliveryRoots, HindsightConsumer, HindsightHttpConfig,
    HindsightHttpRunner, MaterializationCache, MaterializationLimits, ProducerId, ReconcileReport,
    Reconciler, SourceConfig, SourceRoot, StabilityPolicy, SubscriptionConfig, SubscriptionMode,
    catalog_status, run_path_consumer,
};
use uuid::Uuid;

const DEFAULT_CONSUMER_ID: &str = "hindsight-pilot";

fn main() {
    if let Err(error) = run(env::args().skip(1).collect()) {
        eprintln!("agent-bookkeeper-hindsight-controller: {error}");
        process::exit(2);
    }
}

fn run(arguments: Vec<String>) -> Result<(), String> {
    let config = ControllerConfig::parse(arguments)?;
    let mut catalog = Catalog::open(&config.catalog_path).map_err(display_error)?;
    let now_ms = current_time_ms()?;
    let status = catalog_status(&catalog, now_ms).map_err(display_error)?;
    let subscriptions = status
        .subscriptions
        .into_iter()
        .filter(|subscription| subscription.consumer_id == config.consumer_id)
        .collect::<Vec<_>>();
    let subscription_id = match subscriptions.as_slice() {
        [] => {
            catalog
                .create_subscription(
                    SubscriptionConfig::new(&config.consumer_id, 1, true, false)
                        .map_err(display_error)?,
                    SubscriptionMode::ReplayEvents,
                    now_ms,
                )
                .map_err(display_error)?
                .id
        }
        [subscription] => subscription.id,
        _ => {
            return Err(format!(
                "refusing ambiguous consumer_id {:?}: {} subscription epochs exist; select or retire one explicitly",
                config.consumer_id,
                subscriptions.len()
            ));
        }
    };

    let source_config = SourceConfig::new(
        &config.source_id,
        ProducerId::from_uuid(config.producer_id),
        vec![
            SourceRoot::new("active", &config.active_root, None).map_err(display_error)?,
            SourceRoot::new("archived", &config.archived_root, None).map_err(display_error)?,
        ],
        DeletionMode::Disabled,
        StabilityPolicy::new(2).map_err(display_error)?,
    )
    .map_err(display_error)?
    .with_hash_byte_budget_per_scan(config.hash_budget_bytes)
    .with_max_candidate_bytes(config.max_candidate_bytes)
    .map_err(display_error)?;
    let mut reconciler = Reconciler::new(source_config, CodexRolloutLayout);
    let mut reports = Vec::with_capacity(config.reconcile_passes as usize);
    for _ in 0..config.reconcile_passes {
        reports.push(reconciler.scan(&mut catalog).map_err(display_error)?);
    }

    let roots = DeliveryRoots::new(vec![
        ("active".to_owned(), config.active_root.clone()),
        ("archived".to_owned(), config.archived_root.clone()),
    ])
    .map_err(display_error)?;
    let cache = MaterializationCache::new(
        config.cache_root,
        MaterializationLimits::new(config.max_candidate_bytes, 1, config.max_candidate_bytes)
            .map_err(display_error)?,
    )
    .map_err(display_error)?;
    let runner = HindsightHttpRunner::new(
        HindsightHttpConfig::new(
            config.hindsight_base_url,
            Duration::from_secs(config.hindsight_timeout_seconds),
        )
        .map_err(display_error)?,
    );
    let mut consumer =
        HindsightConsumer::new(config.receipt_root, config.hindsight_bank.clone(), runner)
            .map_err(display_error)?;
    let delivery_report = run_path_consumer(
        &mut catalog,
        subscription_id,
        &roots,
        &cache,
        &mut consumer,
        ControlledRunLimits::new(
            config.max_deliveries,
            config.max_payload_bytes,
            config.lease_duration_ms,
        )
        .map_err(display_error)?,
        current_time_ms()?,
    )
    .map_err(display_error)?;
    let final_status = catalog_status(&catalog, current_time_ms()?).map_err(display_error)?;
    let totals = reconcile_totals(&reports);
    println!(
        "{}",
        serde_json::json!({
            "format_version": 1,
            "source_id": config.source_id,
            "consumer_id": config.consumer_id,
            "subscription_id": subscription_id.as_uuid().to_string(),
            "hindsight": {"bank_id": config.hindsight_bank},
            "reconcile_passes": config.reconcile_passes,
            "reconcile": totals,
            "delivery": {
                "attempts": delivery_report.attempts.len(),
                "payload_bytes_admitted": delivery_report.payload_bytes_admitted,
                "outcomes": delivery_report.attempts.iter().map(delivery_attempt_json).collect::<Vec<_>>(),
            },
            "catalog": {
                "latest_event_sequence": final_status.latest_event_sequence,
                "active_records": final_status.active_records,
                "tombstoned_records": final_status.tombstoned_records,
                "revisions": final_status.revisions,
            },
        })
    );
    Ok(())
}

#[derive(Debug)]
struct ControllerConfig {
    catalog_path: PathBuf,
    source_id: String,
    producer_id: Uuid,
    active_root: PathBuf,
    archived_root: PathBuf,
    cache_root: PathBuf,
    receipt_root: PathBuf,
    hindsight_base_url: String,
    hindsight_bank: String,
    hindsight_timeout_seconds: u64,
    hash_budget_bytes: u64,
    max_candidate_bytes: u64,
    max_deliveries: u32,
    max_payload_bytes: u64,
    lease_duration_ms: u64,
    reconcile_passes: u32,
    consumer_id: String,
}

impl ControllerConfig {
    fn parse(arguments: Vec<String>) -> Result<Self, String> {
        let mut values = std::collections::BTreeMap::new();
        let mut index = 0;
        while index < arguments.len() {
            let key = &arguments[index];
            if key == "--help" {
                return Err(usage().to_owned());
            }
            if !key.starts_with("--") {
                return Err(format!(
                    "unexpected positional argument {key:?}\n{}",
                    usage()
                ));
            }
            let value = arguments
                .get(index + 1)
                .ok_or_else(|| format!("missing value for {key}\n{}", usage()))?;
            if values.insert(key.clone(), value.clone()).is_some() {
                return Err(format!("duplicate option {key}"));
            }
            index += 2;
        }
        let config = Self {
            catalog_path: absolute_path(required(&mut values, "--catalog")?, "--catalog")?,
            source_id: required(&mut values, "--source-id")?,
            producer_id: Uuid::parse_str(&required(&mut values, "--producer-id")?)
                .map_err(|error| format!("--producer-id must be a UUID: {error}"))?,
            active_root: absolute_path(required(&mut values, "--active-root")?, "--active-root")?,
            archived_root: absolute_path(
                required(&mut values, "--archived-root")?,
                "--archived-root",
            )?,
            cache_root: absolute_path(required(&mut values, "--cache-root")?, "--cache-root")?,
            receipt_root: absolute_path(
                required(&mut values, "--receipt-root")?,
                "--receipt-root",
            )?,
            hindsight_base_url: required(&mut values, "--hindsight-base-url")?,
            hindsight_bank: required(&mut values, "--hindsight-bank")?,
            hindsight_timeout_seconds: parse_u64(
                required(&mut values, "--hindsight-timeout-seconds")?,
                "--hindsight-timeout-seconds",
            )?,
            hash_budget_bytes: parse_u64(
                required(&mut values, "--hash-budget-bytes")?,
                "--hash-budget-bytes",
            )?,
            max_candidate_bytes: parse_u64(
                required(&mut values, "--max-candidate-bytes")?,
                "--max-candidate-bytes",
            )?,
            max_deliveries: parse_u32(
                required(&mut values, "--max-deliveries")?,
                "--max-deliveries",
            )?,
            max_payload_bytes: parse_u64(
                required(&mut values, "--max-payload-bytes")?,
                "--max-payload-bytes",
            )?,
            lease_duration_ms: parse_u64(
                required(&mut values, "--lease-duration-ms")?,
                "--lease-duration-ms",
            )?,
            reconcile_passes: parse_u32(
                values
                    .remove("--reconcile-passes")
                    .unwrap_or_else(|| "2".to_owned()),
                "--reconcile-passes",
            )?,
            consumer_id: values
                .remove("--consumer-id")
                .unwrap_or_else(|| DEFAULT_CONSUMER_ID.to_owned()),
        };
        if !values.is_empty() {
            return Err(format!(
                "unknown option(s): {}\n{}",
                values.keys().cloned().collect::<Vec<_>>().join(", "),
                usage()
            ));
        }
        if config.source_id.trim() != config.source_id || config.source_id.is_empty() {
            return Err("--source-id must be non-empty and trimmed".to_owned());
        }
        if config.consumer_id.trim() != config.consumer_id || config.consumer_id.is_empty() {
            return Err("--consumer-id must be non-empty and trimmed".to_owned());
        }
        if config.hindsight_timeout_seconds == 0
            || config.max_deliveries == 0
            || config.max_payload_bytes == 0
            || config.lease_duration_ms == 0
            || config.reconcile_passes == 0
        {
            return Err("all controller limits must be greater than zero".to_owned());
        }
        if config.max_payload_bytes < config.max_candidate_bytes {
            return Err("--max-payload-bytes must be at least --max-candidate-bytes".to_owned());
        }
        Ok(config)
    }
}

fn required(
    values: &mut std::collections::BTreeMap<String, String>,
    name: &str,
) -> Result<String, String> {
    values
        .remove(name)
        .ok_or_else(|| format!("missing required {name}\n{}", usage()))
}

fn absolute_path(value: String, name: &str) -> Result<PathBuf, String> {
    let path = PathBuf::from(value);
    if !path.is_absolute() {
        return Err(format!("{name} must be an absolute path"));
    }
    Ok(path)
}

fn parse_u64(value: String, name: &str) -> Result<u64, String> {
    value
        .parse()
        .map_err(|error| format!("{name} must be an unsigned integer: {error}"))
}

fn parse_u32(value: String, name: &str) -> Result<u32, String> {
    value
        .parse()
        .map_err(|error| format!("{name} must be an unsigned integer: {error}"))
}

fn reconcile_totals(reports: &[ReconcileReport]) -> serde_json::Value {
    serde_json::json!({
        "files_enumerated": reports.iter().map(|report| report.files_enumerated).sum::<u64>(),
        "awaiting_stability": reports.iter().map(|report| report.awaiting_stability).sum::<u64>(),
        "already_stable": reports.iter().map(|report| report.already_stable).sum::<u64>(),
        "deferred_by_hash_budget": reports.iter().map(|report| report.deferred_by_hash_budget).sum::<u64>(),
        "deferred_by_size_limit": reports.iter().map(|report| report.deferred_by_size_limit).sum::<u64>(),
        "changed_during_scan": reports.iter().map(|report| report.changed_during_scan).sum::<u64>(),
        "bytes_hashed": reports.iter().map(|report| report.bytes_hashed).sum::<u64>(),
        "events": reports.iter().map(|report| report.events.len()).sum::<usize>(),
    })
}

fn delivery_attempt_json(attempt: &ControlledDeliveryAttempt) -> serde_json::Value {
    let outcome = match &attempt.outcome {
        ControlledDeliveryOutcome::Settled(outcome) => {
            serde_json::json!({"state": "settled", "outcome": format!("{outcome:?}")})
        }
        ControlledDeliveryOutcome::Retried { reason } => {
            serde_json::json!({"state": "retried", "reason": reason})
        }
        ControlledDeliveryOutcome::Blocked { reason } => {
            serde_json::json!({"state": "blocked", "reason": reason})
        }
        ControlledDeliveryOutcome::DeferredByByteBudget => {
            serde_json::json!({"state": "deferred_by_byte_budget"})
        }
    };
    serde_json::json!({
        "event_id": attempt.event_id.as_uuid().to_string(),
        "record_id": attempt.record_id.as_uuid().to_string(),
        "record_version": attempt.record_version,
        "outcome": outcome,
    })
}

fn current_time_ms() -> Result<i64, String> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("system clock precedes Unix epoch: {error}"))?;
    i64::try_from(duration.as_millis())
        .map_err(|_| "system time exceeds i64 milliseconds".to_owned())
}

fn display_error(error: impl std::fmt::Display) -> String {
    error.to_string()
}

fn usage() -> &'static str {
    "usage: agent-bookkeeper-hindsight-controller \\\n+  --catalog ABSOLUTE_PATH --source-id ID --producer-id UUID \\\n+  --active-root ABSOLUTE_PATH --archived-root ABSOLUTE_PATH \\\n+  --cache-root ABSOLUTE_PATH --receipt-root ABSOLUTE_PATH \\\n+  --hindsight-base-url URL --hindsight-bank ID --hindsight-timeout-seconds N \\\n+  --hash-budget-bytes N --max-candidate-bytes N --max-deliveries N \\\n+  --max-payload-bytes N --lease-duration-ms N [--reconcile-passes N] [--consumer-id ID]"
}
