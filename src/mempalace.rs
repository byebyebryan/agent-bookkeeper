//! A provenance-preserving MemPalace consumer for controlled V1.5 cohorts.
//!
//! The adapter has no retrieval or learned-memory policy. It gives MemPalace
//! a verified lease-scoped JSONL path and a stable Bookkeeper record identity,
//! then writes an idempotent receipt only after the external command succeeds.

use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;

use thiserror::Error;
use uuid::Uuid;

use crate::controller::PathConsumer;
use crate::delivery::DeliveryLease;
use crate::domain::{CanonicalRevision, DeliveryOutcome, EventKind, LogicalLocation};

/// Complete source and revision provenance sent to one MemPalace invocation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MempalaceIngestRequest {
    pub input_path: PathBuf,
    pub source_id: String,
    pub subscription_id: Uuid,
    pub event_id: Uuid,
    pub event_sequence: Option<u64>,
    pub record_id: Uuid,
    pub record_version: u64,
    pub event_kind: EventKind,
    pub location: Option<LogicalLocation>,
    pub revision: CanonicalRevision,
}

/// Executes one verified MemPalace ingestion request.
///
/// Implementations must not retain or reinterpret the input path after
/// [`MempalaceConsumer::apply`] returns: it is a short-lived Bookkeeper lease.
pub trait MempalaceRunner {
    fn ingest(&mut self, request: &MempalaceIngestRequest) -> Result<(), String>;
}

/// The portable CLI invocation contract for MemPalace's Codex streaming
/// importer. Deployment code chooses the executable, index path, and wing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MempalaceCliConfig {
    executable: PathBuf,
    palace_path: PathBuf,
    wing: String,
    agent: String,
}

impl MempalaceCliConfig {
    pub fn new(
        executable: impl Into<PathBuf>,
        palace_path: impl Into<PathBuf>,
        wing: impl Into<String>,
    ) -> Result<Self, MempalaceConsumerError> {
        let executable = executable.into();
        if executable.as_os_str().is_empty() {
            return Err(MempalaceConsumerError::InvalidConfiguration(
                "MemPalace executable must not be empty".to_owned(),
            ));
        }
        let palace_path = palace_path.into();
        if !palace_path.is_absolute() {
            return Err(MempalaceConsumerError::InvalidConfiguration(
                "MemPalace palace path must be absolute".to_owned(),
            ));
        }
        let wing = required_label("wing", wing.into())?;
        Ok(Self {
            executable,
            palace_path,
            wing,
            agent: "agent-bookkeeper".to_owned(),
        })
    }

    pub fn with_agent(mut self, agent: impl Into<String>) -> Result<Self, MempalaceConsumerError> {
        self.agent = required_label("agent", agent.into())?;
        Ok(self)
    }
}

/// A local process runner for a MemPalace CLI installed beside the controller.
#[derive(Clone, Debug)]
pub struct MempalaceCommandRunner {
    config: MempalaceCliConfig,
}

impl MempalaceCommandRunner {
    pub fn new(config: MempalaceCliConfig) -> Self {
        Self { config }
    }

    fn arguments(&self, request: &MempalaceIngestRequest) -> Vec<OsString> {
        vec![
            "--palace".into(),
            self.config.palace_path.as_os_str().to_owned(),
            "codex-stream".into(),
            request.input_path.as_os_str().to_owned(),
            "--source-id".into(),
            request.source_id.clone().into(),
            "--wing".into(),
            self.config.wing.clone().into(),
            "--agent".into(),
            self.config.agent.clone().into(),
        ]
    }
}

impl MempalaceRunner for MempalaceCommandRunner {
    fn ingest(&mut self, request: &MempalaceIngestRequest) -> Result<(), String> {
        let output = Command::new(&self.config.executable)
            .args(self.arguments(request))
            .output()
            .map_err(|error| format!("could not start MemPalace: {error}"))?;
        if output.status.success() {
            return Ok(());
        }
        let stderr = String::from_utf8_lossy(&output.stderr);
        let summary = stderr.trim();
        let summary = if summary.is_empty() {
            "no stderr output"
        } else {
            summary
        };
        Err(format!(
            "MemPalace exited with {}: {summary}",
            output.status
        ))
    }
}

/// An idempotent Bookkeeper delivery adapter backed by MemPalace's
/// `codex-stream --source-id` contract.
#[derive(Debug)]
pub struct MempalaceConsumer<R> {
    receipt_root: PathBuf,
    runner: R,
}

impl<R> MempalaceConsumer<R> {
    pub fn new(
        receipt_root: impl Into<PathBuf>,
        runner: R,
    ) -> Result<Self, MempalaceConsumerError> {
        let receipt_root = receipt_root.into();
        if !receipt_root.is_absolute() {
            return Err(MempalaceConsumerError::InvalidConfiguration(
                "MemPalace receipt root must be absolute".to_owned(),
            ));
        }
        fs::create_dir_all(&receipt_root)?;
        let metadata = fs::symlink_metadata(&receipt_root)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(MempalaceConsumerError::InvalidConfiguration(
                "MemPalace receipt root must be a real directory".to_owned(),
            ));
        }
        Ok(Self {
            receipt_root,
            runner,
        })
    }

    pub fn receipt_path(&self, delivery: &DeliveryLease) -> PathBuf {
        self.receipt_root
            .join(delivery.subscription_id.as_uuid().hyphenated().to_string())
            .join(format!("{}.json", delivery.event_id.as_uuid().hyphenated()))
    }

    pub fn source_id(delivery: &DeliveryLease) -> String {
        format!("agent-bookkeeper://record/{}", delivery.record_id.as_uuid())
    }
}

impl<R: MempalaceRunner> MempalaceConsumer<R> {
    fn apply_inner(
        &mut self,
        delivery: &DeliveryLease,
        payload: Option<&Path>,
    ) -> Result<DeliveryOutcome, MempalaceConsumerError> {
        let payload_revision = match (delivery.revision, payload) {
            (Some(expected), Some(path)) => {
                let actual = CanonicalRevision::from_reader(File::open(path)?)?;
                if actual != expected {
                    return Err(MempalaceConsumerError::ProvenanceMismatch);
                }
                Some(actual)
            }
            (Some(_), None) => return Err(MempalaceConsumerError::MissingPayload),
            (None, Some(_)) => return Err(MempalaceConsumerError::UnexpectedPayload),
            (None, None) => None,
        };
        let source_id = Self::source_id(delivery);
        let receipt = receipt_bytes(delivery, payload_revision, &source_id);
        let destination = self.receipt_path(delivery);
        if destination.exists() {
            if fs::read(&destination)? != receipt {
                return Err(MempalaceConsumerError::IdempotencyConflict(destination));
            }
            return Ok(receipt_outcome(delivery));
        }

        if let Some(revision) = payload_revision {
            let (input_path, temporary_alias) =
                codex_stream_input_path(payload.expect("payload matched a revision"))?;
            let result = self.runner.ingest(&MempalaceIngestRequest {
                input_path,
                source_id,
                subscription_id: delivery.subscription_id.as_uuid(),
                event_id: delivery.event_id.as_uuid(),
                event_sequence: delivery.event_sequence,
                record_id: delivery.record_id.as_uuid(),
                record_version: delivery.record_version,
                event_kind: delivery.kind,
                location: delivery.location.clone(),
                revision,
            });
            let cleanup = temporary_alias.map(fs::remove_file).transpose();
            result.map_err(MempalaceConsumerError::Runner)?;
            cleanup?;
        }
        write_receipt(&destination, &receipt)?;
        Ok(receipt_outcome(delivery))
    }
}

/// Return a path accepted by MemPalace's Codex importer.
///
/// Bookkeeper's cache intentionally uses an extension-neutral ``.payload``
/// lease name. MemPalace correctly requires an explicit ``.jsonl`` source, so
/// retain the verified lease bytes in place and expose a short-lived hard-link
/// alias only for the child process. The alias is in the same cache directory,
/// incurs no copy, and is removed whether that process succeeds or fails.
fn codex_stream_input_path(payload: &Path) -> Result<(PathBuf, Option<PathBuf>), std::io::Error> {
    if payload.extension().and_then(|value| value.to_str()) == Some("jsonl") {
        return Ok((payload.to_owned(), None));
    }
    let parent = payload.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "materialized payload has no parent directory",
        )
    })?;
    let filename = payload.file_name().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "materialized payload has no filename",
        )
    })?;
    let alias = parent.join(format!(
        ".{}.{}.jsonl",
        filename.to_string_lossy(),
        Uuid::new_v4()
    ));
    fs::hard_link(payload, &alias)?;
    Ok((alias.clone(), Some(alias)))
}

impl<R: MempalaceRunner> PathConsumer for MempalaceConsumer<R> {
    fn apply(
        &mut self,
        delivery: &DeliveryLease,
        payload: Option<&Path>,
    ) -> Result<DeliveryOutcome, String> {
        self.apply_inner(delivery, payload)
            .map_err(|error| error.to_string())
    }
}

fn required_label(name: &str, value: String) -> Result<String, MempalaceConsumerError> {
    if value.is_empty() || value.trim() != value || value.contains('\0') {
        return Err(MempalaceConsumerError::InvalidConfiguration(format!(
            "MemPalace {name} must be non-empty, trimmed, and free of NUL bytes"
        )));
    }
    Ok(value)
}

fn receipt_outcome(delivery: &DeliveryLease) -> DeliveryOutcome {
    if delivery.revision.is_some() {
        DeliveryOutcome::Acknowledged
    } else {
        // The streaming importer cannot remove a record from an existing
        // semantic index. Keep the omission explicit and durable rather than
        // falsely claiming that a tombstone was applied downstream.
        DeliveryOutcome::IgnoredByPolicy
    }
}

fn write_receipt(destination: &Path, receipt: &[u8]) -> Result<(), MempalaceConsumerError> {
    let parent = destination.parent().expect("receipt path has a parent");
    fs::create_dir_all(parent)?;
    let filename = destination
        .file_name()
        .and_then(|value| value.to_str())
        .expect("receipt path has a UTF-8 filename");
    let temporary = parent.join(format!(".{}.{}.partial", filename, Uuid::new_v4()));
    let result = (|| -> Result<(), MempalaceConsumerError> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(receipt)?;
        file.sync_all()?;
        fs::rename(&temporary, destination)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn receipt_bytes(
    delivery: &DeliveryLease,
    payload: Option<CanonicalRevision>,
    source_id: &str,
) -> Vec<u8> {
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
        "consumer": "mempalace",
        "subscription_id": delivery.subscription_id.as_uuid().to_string(),
        "event_id": delivery.event_id.as_uuid().to_string(),
        "event_idempotency_key": format!(
            "{}:{}",
            delivery.subscription_id,
            delivery.event_id.as_uuid()
        ),
        "event_sequence": delivery.event_sequence,
        "record_id": delivery.record_id.as_uuid().to_string(),
        "record_version": delivery.record_version,
        "event_kind": event_kind_name(delivery.kind),
        "source_id": source_id,
        "location": location,
        "payload": payload,
    }))
    .expect("JSON values serialize without error");
    output.push(b'\n');
    output
}

fn event_kind_name(kind: EventKind) -> &'static str {
    match kind {
        EventKind::RevisionCommitted => "revision_committed",
        EventKind::LocationChanged => "location_changed",
        EventKind::RecordTombstoned => "record_tombstoned",
        EventKind::RecordRestored => "record_restored",
    }
}

#[derive(Debug, Error)]
pub enum MempalaceConsumerError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Domain(#[from] crate::domain::DomainError),
    #[error("invalid MemPalace consumer configuration: {0}")]
    InvalidConfiguration(String),
    #[error("leased payload does not match its declared revision")]
    ProvenanceMismatch,
    #[error("byte-bearing delivery has no materialized payload")]
    MissingPayload,
    #[error("metadata-only delivery unexpectedly has a payload")]
    UnexpectedPayload,
    #[error("existing MemPalace receipt conflicts with delivery provenance: {0}")]
    IdempotencyConflict(PathBuf),
    #[error("MemPalace runner failed: {0}")]
    Runner(String),
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::{
        MempalaceCliConfig, MempalaceCommandRunner, MempalaceConsumer, MempalaceIngestRequest,
        MempalaceRunner,
    };
    use crate::catalog::Catalog;
    use crate::controller::{ControlledRunLimits, DeliveryRoots, run_path_consumer};
    use crate::delivery::{RetryPolicy, SubscriptionConfig, SubscriptionMode};
    use crate::domain::{CanonicalRevision, LogicalLocation, ProducerId, RecordIdentity};
    use crate::payload::{MaterializationCache, MaterializationLimits};

    #[derive(Default)]
    struct RecordingRunner {
        requests: Vec<MempalaceIngestRequest>,
    }

    impl MempalaceRunner for RecordingRunner {
        fn ingest(&mut self, request: &MempalaceIngestRequest) -> Result<(), String> {
            self.requests.push(request.clone());
            Ok(())
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
    fn adapter_uses_stable_record_source_id_and_durable_receipt() {
        let directory = tempdir().unwrap();
        let source_root = directory.path().join("source");
        let cache_root = directory.path().join("cache");
        let receipt_root = directory.path().join("receipts");
        let source_path = source_root.join("sessions/a.jsonl");
        let bytes = b"{\"type\":\"session_meta\"}\n";
        fs::create_dir_all(source_path.parent().unwrap()).unwrap();
        fs::write(&source_path, bytes).unwrap();
        let revision = CanonicalRevision::from_bytes(bytes);
        let mut catalog = Catalog::open_in_memory().unwrap();
        let subscription = catalog
            .create_subscription(
                SubscriptionConfig::new("mempalace", 1, true, false)
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
        let roots = DeliveryRoots::new(vec![("active".to_owned(), source_root)]).unwrap();
        let cache = MaterializationCache::new(
            &cache_root,
            MaterializationLimits::new(1024, 1, 1024).unwrap(),
        )
        .unwrap();
        let runner = RecordingRunner::default();
        let mut consumer = MempalaceConsumer::new(&receipt_root, runner).unwrap();

        let report = run_path_consumer(
            &mut catalog,
            subscription.id,
            &roots,
            &cache,
            &mut consumer,
            ControlledRunLimits::new(1, 1024, 100).unwrap(),
            20,
        )
        .unwrap();

        assert_eq!(report.attempts.len(), 1);
        assert_eq!(consumer.runner.requests.len(), 1);
        let request = &consumer.runner.requests[0];
        assert_eq!(request.revision, revision);
        assert!(request.input_path.starts_with(&cache_root));
        assert_ne!(request.input_path, source_path);
        assert_eq!(
            request
                .input_path
                .extension()
                .and_then(|value| value.to_str()),
            Some("jsonl")
        );
        assert!(!request.input_path.exists());
        assert_eq!(
            request.source_id,
            format!("agent-bookkeeper://record/{}", request.record_id)
        );
        let subscription_directory = fs::read_dir(&receipt_root)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let receipt_path = fs::read_dir(subscription_directory)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let receipt = fs::read_to_string(receipt_path).unwrap();
        assert!(receipt.contains("\"source_relative_path\":\"sessions/a.jsonl\""));
        assert!(receipt.contains(&revision.digest_hex()));
        assert!(receipt.contains(&request.source_id));
    }

    #[test]
    fn command_runner_uses_the_mem_palace_source_id_contract() {
        let config =
            MempalaceCliConfig::new("/usr/local/bin/mempalace", "/data", "archive").unwrap();
        let runner = MempalaceCommandRunner::new(config);
        let request = MempalaceIngestRequest {
            input_path: "/cache/lease.payload".into(),
            source_id: "agent-bookkeeper://record/abc".to_owned(),
            subscription_id: uuid::Uuid::nil(),
            event_id: uuid::Uuid::nil(),
            event_sequence: Some(7),
            record_id: uuid::Uuid::nil(),
            record_version: 3,
            event_kind: crate::domain::EventKind::RevisionCommitted,
            location: Some(LogicalLocation::new("active", "a.jsonl").unwrap()),
            revision: CanonicalRevision::from_bytes(b"fixture"),
        };

        let arguments = runner
            .arguments(&request)
            .into_iter()
            .map(|value| value.into_string().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            arguments,
            vec![
                "--palace",
                "/data",
                "codex-stream",
                "/cache/lease.payload",
                "--source-id",
                "agent-bookkeeper://record/abc",
                "--wing",
                "archive",
                "--agent",
                "agent-bookkeeper",
            ]
        );
    }
}
