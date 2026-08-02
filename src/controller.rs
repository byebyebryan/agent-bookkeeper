//! Bounded, synchronous controller helpers for V1.5 consumer proofs.
//!
//! Discovery only appends delivery rows. A caller explicitly starts a
//! controlled run, which admits a small amount of work from one subscription
//! and gives path-only adapters a verified, lease-scoped materialization.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::catalog::Catalog;
use crate::delivery::{DeliveryError, DeliveryLease, SubscriptionId};
use crate::domain::{DeliveryOutcome, EventId, RecordId};
use crate::payload::{CurrentExternalRevision, MaterializationCache, PayloadError};

/// Configured roots used only to resolve a delivery's normalized logical
/// location. They must be Bookkeeper-readable mirrors, never adapter-owned
/// paths.
#[derive(Clone, Debug, Default)]
pub struct DeliveryRoots {
    roots: BTreeMap<String, PathBuf>,
}

impl DeliveryRoots {
    pub fn new(
        entries: impl IntoIterator<Item = (String, PathBuf)>,
    ) -> Result<Self, ControllerError> {
        let mut roots = Self::default();
        for (role, path) in entries {
            roots.insert(role, path)?;
        }
        Ok(roots)
    }

    pub fn insert(
        &mut self,
        root_role: impl Into<String>,
        path: impl Into<PathBuf>,
    ) -> Result<(), ControllerError> {
        let root_role = root_role.into();
        if root_role.is_empty() || root_role.trim() != root_role || root_role.contains('\0') {
            return Err(ControllerError::InvalidConfiguration(
                "delivery root role must be non-empty, trimmed, and free of NUL bytes".to_owned(),
            ));
        }
        let path = path.into();
        if !path.is_absolute() {
            return Err(ControllerError::InvalidConfiguration(
                "delivery roots must be absolute paths".to_owned(),
            ));
        }
        if self.roots.insert(root_role.clone(), path).is_some() {
            return Err(ControllerError::InvalidConfiguration(format!(
                "duplicate delivery root role {root_role:?}"
            )));
        }
        Ok(())
    }

    pub fn get(&self, root_role: &str) -> Option<&Path> {
        self.roots.get(root_role).map(PathBuf::as_path)
    }
}

/// Per-invocation bounds. These are intentionally independent of a
/// subscription's durable lease limit: a large backlog must not turn one
/// explicit controller invocation into an uncontrolled indexing burst.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ControlledRunLimits {
    max_deliveries: u32,
    max_payload_bytes: u64,
    lease_duration_ms: u64,
}

impl ControlledRunLimits {
    pub fn new(
        max_deliveries: u32,
        max_payload_bytes: u64,
        lease_duration_ms: u64,
    ) -> Result<Self, ControllerError> {
        if max_deliveries == 0 || lease_duration_ms == 0 {
            return Err(ControllerError::InvalidConfiguration(
                "controlled runs require at least one delivery and a positive lease duration"
                    .to_owned(),
            ));
        }
        Ok(Self {
            max_deliveries,
            max_payload_bytes,
            lease_duration_ms,
        })
    }
}

/// A path-only external adapter. `payload` is `None` for metadata-only events
/// such as a tombstone. A successful result means the adapter has durably
/// recorded the event's identity and revision provenance.
pub trait PathConsumer {
    fn apply(
        &mut self,
        delivery: &DeliveryLease,
        payload: Option<&Path>,
    ) -> Result<DeliveryOutcome, String>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ControlledDeliveryOutcome {
    Settled(DeliveryOutcome),
    Retried { reason: String },
    DeferredByByteBudget,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlledDeliveryAttempt {
    pub event_id: EventId,
    pub record_id: RecordId,
    pub record_version: u64,
    pub outcome: ControlledDeliveryOutcome,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ControlledRunReport {
    pub attempts: Vec<ControlledDeliveryAttempt>,
    pub payload_bytes_admitted: u64,
}

/// Runs a bounded amount of one subscription's work. Adapter failures and
/// unavailable mutable bytes are returned to `queued` and recorded in the
/// report; the caller can start a later controlled run after the source or
/// adapter recovers. The function deliberately stops after one retry so it
/// cannot spin on an immediately redeliverable failure.
pub fn run_path_consumer<C: PathConsumer>(
    catalog: &mut Catalog,
    subscription_id: SubscriptionId,
    roots: &DeliveryRoots,
    cache: &MaterializationCache,
    consumer: &mut C,
    limits: ControlledRunLimits,
    now_ms: i64,
) -> Result<ControlledRunReport, ControllerError> {
    let mut report = ControlledRunReport::default();
    for _ in 0..limits.max_deliveries {
        let Some(delivery) =
            catalog.lease_next(subscription_id, now_ms, limits.lease_duration_ms)?
        else {
            break;
        };
        let payload_bytes = delivery
            .revision
            .map(|revision| revision.byte_length())
            .unwrap_or(0);
        let next_payload_bytes = report
            .payload_bytes_admitted
            .checked_add(payload_bytes)
            .ok_or(ControllerError::CounterOverflow)?;
        if next_payload_bytes > limits.max_payload_bytes {
            catalog.retry_delivery(&delivery, now_ms)?;
            report.attempts.push(attempt(
                &delivery,
                ControlledDeliveryOutcome::DeferredByByteBudget,
            ));
            break;
        }

        let outcome = if let Some(revision) = delivery.revision {
            let Some(location) = delivery.location.as_ref() else {
                retry_with_configuration_error(
                    catalog,
                    &delivery,
                    now_ms,
                    "byte-bearing delivery has no logical location",
                )?;
                return Err(ControllerError::MissingDeliveryLocation);
            };
            let Some(root) = roots.get(location.root_role()) else {
                retry_with_configuration_error(
                    catalog,
                    &delivery,
                    now_ms,
                    "delivery root role is not configured",
                )?;
                return Err(ControllerError::UnknownRootRole {
                    root_role: location.root_role().to_owned(),
                });
            };
            let external =
                CurrentExternalRevision::new(root, location.source_relative_path(), revision)?;
            let materialized = match cache.materialize(&external) {
                Ok(lease) => lease,
                Err(error) => {
                    catalog.retry_delivery(&delivery, now_ms)?;
                    report.attempts.push(attempt(
                        &delivery,
                        ControlledDeliveryOutcome::Retried {
                            reason: format!("verified materialization failed: {error}"),
                        },
                    ));
                    break;
                }
            };
            let outcome = consumer.apply(&delivery, Some(materialized.path()));
            materialized.release()?;
            match outcome {
                Ok(outcome) => outcome,
                Err(reason) => {
                    catalog.retry_delivery(&delivery, now_ms)?;
                    report.attempts.push(attempt(
                        &delivery,
                        ControlledDeliveryOutcome::Retried { reason },
                    ));
                    break;
                }
            }
        } else {
            match consumer.apply(&delivery, None) {
                Ok(outcome) => outcome,
                Err(reason) => {
                    catalog.retry_delivery(&delivery, now_ms)?;
                    report.attempts.push(attempt(
                        &delivery,
                        ControlledDeliveryOutcome::Retried { reason },
                    ));
                    break;
                }
            }
        };

        catalog.settle_delivery(&delivery, outcome, now_ms, None)?;
        report.payload_bytes_admitted = next_payload_bytes;
        report.attempts.push(attempt(
            &delivery,
            ControlledDeliveryOutcome::Settled(outcome),
        ));
    }
    Ok(report)
}

fn retry_with_configuration_error(
    catalog: &mut Catalog,
    delivery: &DeliveryLease,
    now_ms: i64,
    _reason: &str,
) -> Result<(), ControllerError> {
    catalog.retry_delivery(delivery, now_ms)?;
    Ok(())
}

fn attempt(
    delivery: &DeliveryLease,
    outcome: ControlledDeliveryOutcome,
) -> ControlledDeliveryAttempt {
    ControlledDeliveryAttempt {
        event_id: delivery.event_id,
        record_id: delivery.record_id,
        record_version: delivery.record_version,
        outcome,
    }
}

#[derive(Debug, Error)]
pub enum ControllerError {
    #[error(transparent)]
    Delivery(#[from] DeliveryError),
    #[error(transparent)]
    Payload(#[from] PayloadError),
    #[error("invalid controller configuration: {0}")]
    InvalidConfiguration(String),
    #[error("delivery refers to an unconfigured root role {root_role:?}")]
    UnknownRootRole { root_role: String },
    #[error("byte-bearing delivery has no logical location")]
    MissingDeliveryLocation,
    #[error("controlled-run counter overflowed")]
    CounterOverflow,
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::fs;
    use std::path::Path;

    use tempfile::tempdir;

    use super::{
        ControlledDeliveryOutcome, ControlledRunLimits, DeliveryRoots, PathConsumer,
        run_path_consumer,
    };
    use crate::catalog::Catalog;
    use crate::delivery::{DeliveryLease, RetryPolicy, SubscriptionConfig, SubscriptionMode};
    use crate::domain::{
        CanonicalRevision, DeliveryOutcome, LogicalLocation, ProducerId, RecordIdentity,
    };
    use crate::payload::{MaterializationCache, MaterializationLimits};

    struct LostAcknowledgementConsumer {
        applied: HashSet<(uuid::Uuid, uuid::Uuid)>,
        source_path: std::path::PathBuf,
        calls: u32,
    }

    impl PathConsumer for LostAcknowledgementConsumer {
        fn apply(
            &mut self,
            delivery: &DeliveryLease,
            payload: Option<&Path>,
        ) -> Result<DeliveryOutcome, String> {
            self.calls += 1;
            let payload = payload.ok_or_else(|| "fixture expected bytes".to_owned())?;
            if payload == self.source_path {
                return Err("consumer was given the mutable source path".to_owned());
            }
            if fs::read(payload).map_err(|error| error.to_string())? != b"fixture bytes\n" {
                return Err("unexpected materialized bytes".to_owned());
            }
            let key = (
                delivery.subscription_id.as_uuid(),
                delivery.event_id.as_uuid(),
            );
            if self.applied.insert(key) {
                Err("injected lost acknowledgement after durable apply".to_owned())
            } else {
                Ok(DeliveryOutcome::Acknowledged)
            }
        }
    }

    fn identity() -> RecordIdentity {
        RecordIdentity::new(
            ProducerId::new(),
            "codex",
            "session-a",
            "transcript",
            "primary",
        )
        .unwrap()
    }

    #[test]
    fn idempotent_path_consumer_survives_lost_acknowledgement_without_duplicate_effect() {
        let directory = tempdir().unwrap();
        let source_root = directory.path().join("source");
        let cache_root = directory.path().join("cache");
        let source_path = source_root.join("sessions/a.jsonl");
        fs::create_dir_all(source_path.parent().unwrap()).unwrap();
        fs::write(&source_path, b"fixture bytes\n").unwrap();
        let revision = CanonicalRevision::from_bytes(b"fixture bytes\n");
        let mut catalog = Catalog::open_in_memory().unwrap();
        let subscription = catalog
            .create_subscription(
                SubscriptionConfig::new("fixture", 1, true, true)
                    .unwrap()
                    .with_retry_policy(RetryPolicy::new(8, 0, 0).unwrap()),
                SubscriptionMode::ReplayEvents,
                0,
            )
            .unwrap();
        catalog
            .observe_present_at(
                &identity(),
                &LogicalLocation::new("active", "sessions/a.jsonl").unwrap(),
                revision,
                10,
            )
            .unwrap();
        let roots = DeliveryRoots::new(vec![("active".to_owned(), source_root.clone())]).unwrap();
        let cache = MaterializationCache::new(
            &cache_root,
            MaterializationLimits::new(1024, 1, 1024).unwrap(),
        )
        .unwrap();
        let limits = ControlledRunLimits::new(2, 1024, 100).unwrap();
        let mut consumer = LostAcknowledgementConsumer {
            applied: HashSet::new(),
            source_path,
            calls: 0,
        };

        let first = run_path_consumer(
            &mut catalog,
            subscription.id,
            &roots,
            &cache,
            &mut consumer,
            limits,
            20,
        )
        .unwrap();
        let second = run_path_consumer(
            &mut catalog,
            subscription.id,
            &roots,
            &cache,
            &mut consumer,
            limits,
            30,
        )
        .unwrap();

        assert!(matches!(
            first.attempts.as_slice(),
            [attempt] if matches!(attempt.outcome, ControlledDeliveryOutcome::Retried { .. })
        ));
        assert!(matches!(
            second.attempts.as_slice(),
            [attempt] if attempt.outcome == ControlledDeliveryOutcome::Settled(DeliveryOutcome::Acknowledged)
        ));
        assert_eq!(consumer.calls, 2);
        assert_eq!(consumer.applied.len(), 1);
        assert_eq!(
            catalog
                .delivery_counts(subscription.id)
                .unwrap()
                .acknowledged,
            1
        );
    }

    #[test]
    fn byte_budget_defers_a_lease_without_starting_the_consumer() {
        let directory = tempdir().unwrap();
        let source_root = directory.path().join("source");
        let cache_root = directory.path().join("cache");
        let source_path = source_root.join("a.jsonl");
        fs::create_dir_all(&source_root).unwrap();
        fs::write(&source_path, b"fixture bytes\n").unwrap();
        let revision = CanonicalRevision::from_bytes(b"fixture bytes\n");
        let mut catalog = Catalog::open_in_memory().unwrap();
        let subscription = catalog
            .create_subscription(
                SubscriptionConfig::new("fixture", 1, true, true)
                    .unwrap()
                    .with_retry_policy(RetryPolicy::new(8, 0, 0).unwrap()),
                SubscriptionMode::ReplayEvents,
                0,
            )
            .unwrap();
        catalog
            .observe_present_at(
                &identity(),
                &LogicalLocation::new("active", "a.jsonl").unwrap(),
                revision,
                10,
            )
            .unwrap();
        let roots = DeliveryRoots::new(vec![("active".to_owned(), source_root)]).unwrap();
        let cache = MaterializationCache::new(
            &cache_root,
            MaterializationLimits::new(1024, 1, 1024).unwrap(),
        )
        .unwrap();
        let limits = ControlledRunLimits::new(1, 1, 100).unwrap();
        let mut consumer = LostAcknowledgementConsumer {
            applied: HashSet::new(),
            source_path,
            calls: 0,
        };

        let report = run_path_consumer(
            &mut catalog,
            subscription.id,
            &roots,
            &cache,
            &mut consumer,
            limits,
            20,
        )
        .unwrap();

        assert!(matches!(
            report.attempts.as_slice(),
            [attempt] if attempt.outcome == ControlledDeliveryOutcome::DeferredByByteBudget
        ));
        assert_eq!(consumer.calls, 0);
        assert_eq!(catalog.delivery_counts(subscription.id).unwrap().queued, 1);
    }
}
