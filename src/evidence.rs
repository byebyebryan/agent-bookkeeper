//! A small filesystem evidence adapter for controlled V1.5 cohort proofs.
//!
//! It deliberately stores provenance rather than transcript copies. The output
//! is derived and idempotent on `(subscription_id, event_id)`.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use thiserror::Error;
use uuid::Uuid;

use crate::controller::PathConsumer;
use crate::delivery::DeliveryLease;
use crate::domain::{CanonicalRevision, DeliveryOutcome};

#[derive(Debug)]
pub struct FilesystemEvidenceConsumer {
    root: PathBuf,
}

impl FilesystemEvidenceConsumer {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self, EvidenceConsumerError> {
        let root = root.into();
        if !root.is_absolute() {
            return Err(EvidenceConsumerError::InvalidConfiguration(
                "evidence root must be absolute".to_owned(),
            ));
        }
        fs::create_dir_all(&root)?;
        let metadata = fs::symlink_metadata(&root)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(EvidenceConsumerError::InvalidConfiguration(
                "evidence root must be a real directory".to_owned(),
            ));
        }
        Ok(Self { root })
    }

    pub fn manifest_path(&self, delivery: &DeliveryLease) -> PathBuf {
        self.root
            .join(delivery.subscription_id.as_uuid().hyphenated().to_string())
            .join(format!("{}.json", delivery.event_id.as_uuid().hyphenated()))
    }

    fn apply_inner(
        &mut self,
        delivery: &DeliveryLease,
        payload: Option<&Path>,
    ) -> Result<DeliveryOutcome, EvidenceConsumerError> {
        let payload_revision = match (delivery.revision, payload) {
            (Some(expected), Some(path)) => {
                let actual = CanonicalRevision::from_reader(File::open(path)?)?;
                if actual != expected {
                    return Err(EvidenceConsumerError::ProvenanceMismatch);
                }
                Some(actual)
            }
            (Some(_), None) => return Err(EvidenceConsumerError::MissingPayload),
            (None, Some(_)) => return Err(EvidenceConsumerError::UnexpectedPayload),
            (None, None) => None,
        };
        let manifest = manifest_bytes(delivery, payload_revision);
        let destination = self.manifest_path(delivery);
        if destination.exists() {
            if fs::read(&destination)? != manifest {
                return Err(EvidenceConsumerError::IdempotencyConflict(destination));
            }
            return Ok(DeliveryOutcome::Acknowledged);
        }
        let parent = destination.parent().expect("manifest path has a parent");
        fs::create_dir_all(parent)?;
        let temporary = parent.join(format!(
            ".{}.{}.partial",
            delivery.event_id.as_uuid(),
            Uuid::new_v4()
        ));
        let result = (|| -> Result<(), EvidenceConsumerError> {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)?;
            file.write_all(&manifest)?;
            file.sync_all()?;
            fs::rename(&temporary, &destination)?;
            File::open(parent)?.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result?;
        Ok(DeliveryOutcome::Acknowledged)
    }
}

impl PathConsumer for FilesystemEvidenceConsumer {
    fn apply(
        &mut self,
        delivery: &DeliveryLease,
        payload: Option<&Path>,
    ) -> Result<DeliveryOutcome, String> {
        self.apply_inner(delivery, payload)
            .map_err(|error| error.to_string())
    }
}

fn manifest_bytes(delivery: &DeliveryLease, payload: Option<CanonicalRevision>) -> Vec<u8> {
    let location = delivery.location.as_ref().map(|location| {
        serde_json::json!({
            "root_role": location.root_role(),
            "source_relative_path": location.source_relative_path(),
        })
    });
    let payload = payload.map(|value| {
        serde_json::json!({
            "algorithm": CanonicalRevision::ALGORITHM,
            "byte_length": value.byte_length(),
            "digest": value.digest_hex(),
        })
    });
    let mut output = serde_json::to_vec(&serde_json::json!({
        "format_version": 1,
        "subscription_id": delivery.subscription_id.as_uuid().to_string(),
        "event_id": delivery.event_id.as_uuid().to_string(),
        "record_id": delivery.record_id.as_uuid().to_string(),
        "record_version": delivery.record_version,
        "event_kind": event_kind_name(delivery.kind),
        "event_sequence": delivery.event_sequence,
        "location": location,
        "payload": payload,
    }))
    .expect("JSON values serialize without error");
    output.push(b'\n');
    output
}

fn event_kind_name(kind: crate::domain::EventKind) -> &'static str {
    match kind {
        crate::domain::EventKind::RevisionCommitted => "revision_committed",
        crate::domain::EventKind::LocationChanged => "location_changed",
        crate::domain::EventKind::RecordTombstoned => "record_tombstoned",
        crate::domain::EventKind::RecordRestored => "record_restored",
    }
}

#[derive(Debug, Error)]
pub enum EvidenceConsumerError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Domain(#[from] crate::domain::DomainError),
    #[error("invalid evidence-consumer configuration: {0}")]
    InvalidConfiguration(String),
    #[error("leased payload does not match its declared revision")]
    ProvenanceMismatch,
    #[error("byte-bearing delivery has no materialized payload")]
    MissingPayload,
    #[error("metadata-only delivery unexpectedly has a payload")]
    UnexpectedPayload,
    #[error("existing evidence manifest conflicts with the delivery provenance: {0}")]
    IdempotencyConflict(PathBuf),
}
