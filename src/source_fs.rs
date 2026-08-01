//! Read-only, guarded filesystem discovery for V1.5 sources.
//!
//! This module owns filesystem mechanics only. Provider plugins establish stable
//! record identity, while [`Catalog`] remains the authority for revisions and
//! ordered events.

use std::collections::HashSet;
use std::ffi::{CString, OsStr};
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::{Component, Path, PathBuf};

use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

use crate::catalog::{
    Catalog, CatalogError, SourceFingerprint, SourceRegistration, TombstoneGrace,
};
use crate::domain::{
    ArchiveEvent, CanonicalRevision, DomainError, LogicalLocation, ProducerId, RecordIdentity,
};

const CODEX_IDENTITY_SCHEMA_NAME: &str = "codex-rollout";
const CODEX_IDENTITY_SCHEMA_VERSION: u32 = 1;
const MAX_SESSION_METADATA_LINE_BYTES: u64 = 64 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IdentitySchema {
    name: String,
    version: u32,
}

impl IdentitySchema {
    pub fn new(name: impl Into<String>, version: u32) -> Result<Self, SourceError> {
        let name = name.into();
        if name.is_empty() || name.trim() != name || name.contains('\0') || version == 0 {
            return Err(SourceError::InvalidConfiguration(
                "identity schema requires a non-empty trimmed name and non-zero version".to_owned(),
            ));
        }
        Ok(Self { name, version })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn version(&self) -> u32 {
        self.version
    }
}

/// The stable provider-specific identity parser. It may inspect only metadata
/// needed for source identity; semantic extraction belongs to consumers.
pub trait LayoutPlugin {
    fn identity_schema(&self) -> IdentitySchema;

    /// Cheap filename-level filter run before stability bookkeeping or file I/O.
    /// A plugin may still reject a matching candidate after reading its identity
    /// metadata.
    fn is_candidate_path(&self, _relative_path: &Path) -> bool {
        true
    }

    fn parse_record(
        &self,
        producer_id: ProducerId,
        root_role: &str,
        relative_path: &Path,
        file: &mut File,
    ) -> Result<Option<ParsedRecord>, SourceError>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedRecord {
    identity: RecordIdentity,
    location: LogicalLocation,
}

impl ParsedRecord {
    pub fn identity(&self) -> &RecordIdentity {
        &self.identity
    }

    pub fn location(&self) -> &LogicalLocation {
        &self.location
    }
}

/// The initial Codex schema accepts a JSONL rollout only when its first line is
/// `session_meta` and its metadata UUID matches the filename UUID.
#[derive(Clone, Copy, Debug, Default)]
pub struct CodexRolloutLayout;

impl LayoutPlugin for CodexRolloutLayout {
    fn identity_schema(&self) -> IdentitySchema {
        IdentitySchema::new(CODEX_IDENTITY_SCHEMA_NAME, CODEX_IDENTITY_SCHEMA_VERSION)
            .expect("static Codex identity schema is valid")
    }

    fn is_candidate_path(&self, relative_path: &Path) -> bool {
        relative_path
            .file_name()
            .and_then(OsStr::to_str)
            .is_some_and(|filename| {
                filename.starts_with("rollout-") && filename.ends_with(".jsonl")
            })
    }

    fn parse_record(
        &self,
        producer_id: ProducerId,
        root_role: &str,
        relative_path: &Path,
        file: &mut File,
    ) -> Result<Option<ParsedRecord>, SourceError> {
        let Some(filename_session_id) = rollout_filename_session_id(relative_path)? else {
            return Ok(None);
        };
        let metadata_session_id = session_metadata_id(file)?;
        if metadata_session_id != filename_session_id {
            return Err(SourceError::InvalidProviderRecord {
                relative_path: relative_path.to_owned(),
                reason: "session_meta payload.id does not match rollout filename UUID".to_owned(),
            });
        }

        let identity = RecordIdentity::new(
            producer_id,
            "codex",
            metadata_session_id.hyphenated().to_string(),
            "transcript",
            "primary",
        )?;
        let location = LogicalLocation::new(root_role, normalized_relative_path(relative_path)?)?;
        Ok(Some(ParsedRecord { identity, location }))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StabilityPolicy {
    min_unchanged_observations: u32,
}

impl StabilityPolicy {
    pub fn new(min_unchanged_observations: u32) -> Result<Self, SourceError> {
        if min_unchanged_observations == 0 {
            return Err(SourceError::InvalidConfiguration(
                "min_unchanged_observations must be at least one".to_owned(),
            ));
        }
        Ok(Self {
            min_unchanged_observations,
        })
    }

    pub fn min_unchanged_observations(self) -> u32 {
        self.min_unchanged_observations
    }
}

impl Default for StabilityPolicy {
    fn default() -> Self {
        Self {
            min_unchanged_observations: 2,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RootGuard {
    marker_relative_path: PathBuf,
    expected_revision: CanonicalRevision,
}

impl RootGuard {
    pub fn from_marker_bytes(
        marker_relative_path: impl Into<PathBuf>,
        expected_bytes: &[u8],
    ) -> Result<Self, SourceError> {
        let marker_relative_path = marker_relative_path.into();
        relative_components(&marker_relative_path)?;
        Ok(Self {
            marker_relative_path,
            expected_revision: CanonicalRevision::from_bytes(expected_bytes),
        })
    }

    pub fn expected_revision(&self) -> CanonicalRevision {
        self.expected_revision
    }
}

#[derive(Clone, Debug)]
pub struct SourceRoot {
    root_role: String,
    path: PathBuf,
    guard: Option<RootGuard>,
}

impl SourceRoot {
    pub fn new(
        root_role: impl Into<String>,
        path: impl Into<PathBuf>,
        guard: Option<RootGuard>,
    ) -> Result<Self, SourceError> {
        let root_role = root_role.into();
        LogicalLocation::new(&root_role, "placeholder").map_err(SourceError::Domain)?;
        let path = path.into();
        if !path.is_absolute() {
            return Err(SourceError::InvalidConfiguration(
                "source root must be an absolute path".to_owned(),
            ));
        }
        Ok(Self {
            root_role,
            path,
            guard,
        })
    }

    pub fn root_role(&self) -> &str {
        &self.root_role
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeletionMode {
    /// A source may observe present records, but absence never becomes deletion.
    Disabled,
    /// Guard checks plus the configured complete-scan/time grace are mandatory
    /// before absence may become a tombstone.
    Guarded(TombstoneGrace),
}

#[derive(Clone, Debug)]
pub struct SourceConfig {
    source_id: String,
    producer_id: ProducerId,
    roots: Vec<SourceRoot>,
    deletion_mode: DeletionMode,
    stability_policy: StabilityPolicy,
}

impl SourceConfig {
    pub fn new(
        source_id: impl Into<String>,
        producer_id: ProducerId,
        roots: Vec<SourceRoot>,
        deletion_mode: DeletionMode,
        stability_policy: StabilityPolicy,
    ) -> Result<Self, SourceError> {
        let source_id = source_id.into();
        if source_id.is_empty() || source_id.trim() != source_id || source_id.contains('\0') {
            return Err(SourceError::InvalidConfiguration(
                "source_id must be non-empty, trimmed, and free of NUL bytes".to_owned(),
            ));
        }
        if roots.is_empty() {
            return Err(SourceError::InvalidConfiguration(
                "a source requires at least one root".to_owned(),
            ));
        }
        let unique_roles = roots
            .iter()
            .map(|root| root.root_role.as_str())
            .collect::<HashSet<_>>();
        if unique_roles.len() != roots.len() {
            return Err(SourceError::InvalidConfiguration(
                "source root roles must be unique".to_owned(),
            ));
        }
        if matches!(deletion_mode, DeletionMode::Guarded(_))
            && roots.iter().any(|root| root.guard.is_none())
        {
            return Err(SourceError::InvalidConfiguration(
                "guarded deletion requires a root guard for every root".to_owned(),
            ));
        }

        Ok(Self {
            source_id,
            producer_id,
            roots,
            deletion_mode,
            stability_policy,
        })
    }

    pub fn source_id(&self) -> &str {
        &self.source_id
    }

    pub fn deletion_mode(&self) -> DeletionMode {
        self.deletion_mode
    }

    fn registration(
        &self,
        identity_schema: &IdentitySchema,
    ) -> Result<SourceRegistration, SourceError> {
        SourceRegistration::new(
            &self.source_id,
            self.producer_id,
            identity_schema.name(),
            identity_schema.version(),
        )
        .map_err(SourceError::Catalog)
    }
}

pub trait RevisionHasher {
    fn hash(&self, file: &mut File) -> Result<CanonicalRevision, SourceError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Blake3RevisionHasher;

impl RevisionHasher for Blake3RevisionHasher {
    fn hash(&self, file: &mut File) -> Result<CanonicalRevision, SourceError> {
        file.seek(SeekFrom::Start(0)).map_err(SourceError::Read)?;
        CanonicalRevision::from_reader(file).map_err(SourceError::Domain)
    }
}

/// A source scanner delegates durable stability and deletion state to the
/// catalog; it retains no correctness-relevant observations in process memory.
pub struct Reconciler<L, H = Blake3RevisionHasher> {
    config: SourceConfig,
    layout: L,
    hasher: H,
}

impl<L> Reconciler<L, Blake3RevisionHasher>
where
    L: LayoutPlugin,
{
    pub fn new(config: SourceConfig, layout: L) -> Self {
        Self::with_hasher(config, layout, Blake3RevisionHasher)
    }
}

impl<L, H> Reconciler<L, H>
where
    L: LayoutPlugin,
    H: RevisionHasher,
{
    pub fn with_hasher(config: SourceConfig, layout: L, hasher: H) -> Self {
        Self {
            config,
            layout,
            hasher,
        }
    }

    pub fn identity_schema(&self) -> IdentitySchema {
        self.layout.identity_schema()
    }

    pub fn scan(&mut self, catalog: &mut Catalog) -> Result<ReconcileReport, SourceError> {
        self.verify_roots()?;
        let registration = self.config.registration(&self.layout.identity_schema())?;
        let scan = catalog
            .begin_source_scan(&registration)
            .map_err(SourceError::Catalog)?;
        let raw_candidates = self.enumerate_candidates()?;
        let mut report = ReconcileReport {
            files_enumerated: raw_candidates.len() as u64,
            ..ReconcileReport::default()
        };

        let mut parsed_candidates = Vec::new();
        for candidate in raw_candidates {
            if !self.layout.is_candidate_path(&candidate.relative_path) {
                continue;
            }
            let root = &self.config.roots[candidate.root_index];
            let mut file = open_file_beneath(&root.path, &candidate.relative_path)?;
            let current_fingerprint =
                SourceFingerprint::from_metadata(&file.metadata().map_err(SourceError::Read)?)
                    .map_err(SourceError::Read)?;
            if current_fingerprint != candidate.fingerprint {
                report.changed_during_scan += 1;
                continue;
            }
            let Some(parsed) = self.layout.parse_record(
                self.config.producer_id,
                root.root_role(),
                &candidate.relative_path,
                &mut file,
            )?
            else {
                continue;
            };
            parsed_candidates.push(StableCandidate { candidate, parsed });
        }

        let mut identities = HashSet::new();
        for candidate in &parsed_candidates {
            if !identities.insert(candidate.parsed.identity.clone()) {
                return Err(SourceError::DuplicateLiveIdentity {
                    source_id: self.config.source_id.clone(),
                    identity: Box::new(candidate.parsed.identity.clone()),
                });
            }
        }

        let mut accepted = Vec::new();
        for candidate in parsed_candidates {
            let unchanged_observations = catalog
                .observe_source_fingerprint(
                    &scan,
                    candidate.parsed.location(),
                    &candidate.candidate.fingerprint,
                )
                .map_err(SourceError::Catalog)?;
            catalog
                .mark_source_record_seen(&scan, &registration, candidate.parsed.identity())
                .map_err(SourceError::Catalog)?;
            if unchanged_observations < self.config.stability_policy.min_unchanged_observations() {
                report.awaiting_stability += 1;
                continue;
            }
            let root = &self.config.roots[candidate.candidate.root_index];
            let mut file = open_file_beneath(&root.path, &candidate.candidate.relative_path)?;
            let before =
                SourceFingerprint::from_metadata(&file.metadata().map_err(SourceError::Read)?)
                    .map_err(SourceError::Read)?;
            if before != candidate.candidate.fingerprint {
                report.changed_during_scan += 1;
                continue;
            }
            let parsed_again = self.layout.parse_record(
                self.config.producer_id,
                root.root_role(),
                &candidate.candidate.relative_path,
                &mut file,
            )?;
            if parsed_again.as_ref() != Some(&candidate.parsed) {
                report.changed_during_scan += 1;
                continue;
            }

            let revision = self.hasher.hash(&mut file)?;
            let after =
                SourceFingerprint::from_metadata(&file.metadata().map_err(SourceError::Read)?)
                    .map_err(SourceError::Read)?;
            if before != after {
                report.changed_during_scan += 1;
                continue;
            }
            report.bytes_hashed = report
                .bytes_hashed
                .checked_add(revision.byte_length())
                .ok_or(SourceError::CounterOverflow)?;
            accepted.push((candidate.parsed, revision));
        }

        // A missing/replaced guard is never treated as an empty source. Verify
        // again before creating any ledger event from this scan.
        self.verify_roots()?;
        for (parsed, revision) in accepted {
            let events = catalog
                .observe_present_from_source(
                    &scan,
                    &registration,
                    parsed.identity(),
                    parsed.location(),
                    revision,
                )
                .map_err(SourceError::Catalog)?;
            report.events.extend(events);
        }
        let tombstone_grace = match self.config.deletion_mode {
            DeletionMode::Disabled => None,
            DeletionMode::Guarded(grace) => Some(grace),
        };
        let tombstone_events = catalog
            .complete_source_scan_now(&scan, tombstone_grace)
            .map_err(SourceError::Catalog)?;
        report.events.extend(tombstone_events);
        Ok(report)
    }

    fn verify_roots(&self) -> Result<(), SourceError> {
        for root in &self.config.roots {
            let metadata = fs::symlink_metadata(&root.path).map_err(|source| {
                SourceError::RootUnavailable {
                    root_role: root.root_role.clone(),
                    source,
                }
            })?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(SourceError::UnhealthyRoot {
                    root_role: root.root_role.clone(),
                    reason: "root is not a real directory".to_owned(),
                });
            }
            let _root_directory = open_directory_no_follow(&root.path)?;
            if let Some(guard) = &root.guard {
                verify_guard(root, guard)?;
            }
        }
        Ok(())
    }

    fn enumerate_candidates(&self) -> Result<Vec<FileCandidate>, SourceError> {
        let mut candidates = Vec::new();
        for (root_index, root) in self.config.roots.iter().enumerate() {
            enumerate_root(root_index, root, Path::new(""), &mut candidates)?;
        }
        Ok(candidates)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReconcileReport {
    pub files_enumerated: u64,
    pub awaiting_stability: u64,
    pub changed_during_scan: u64,
    pub bytes_hashed: u64,
    pub events: Vec<ArchiveEvent>,
}

#[derive(Clone, Debug)]
struct FileCandidate {
    root_index: usize,
    relative_path: PathBuf,
    fingerprint: SourceFingerprint,
}

#[derive(Clone, Debug)]
struct StableCandidate {
    candidate: FileCandidate,
    parsed: ParsedRecord,
}

fn enumerate_root(
    root_index: usize,
    root: &SourceRoot,
    relative_directory: &Path,
    candidates: &mut Vec<FileCandidate>,
) -> Result<(), SourceError> {
    let directory = root.path.join(relative_directory);
    let entries = fs::read_dir(&directory).map_err(SourceError::Read)?;
    for entry in entries {
        let entry = entry.map_err(SourceError::Read)?;
        let file_name = entry.file_name();
        let relative_path = relative_directory.join(&file_name);
        let metadata = fs::symlink_metadata(entry.path()).map_err(SourceError::Read)?;
        if metadata.file_type().is_symlink() {
            continue;
        }
        if metadata.is_dir() {
            enumerate_root(root_index, root, &relative_path, candidates)?;
        } else if metadata.is_file() {
            candidates.push(FileCandidate {
                root_index,
                relative_path,
                fingerprint: SourceFingerprint::from_metadata(&metadata)
                    .map_err(SourceError::Read)?,
            });
        }
    }
    Ok(())
}

fn verify_guard(root: &SourceRoot, guard: &RootGuard) -> Result<(), SourceError> {
    let mut marker = open_file_beneath(&root.path, &guard.marker_relative_path)?;
    let actual = CanonicalRevision::from_reader(&mut marker).map_err(SourceError::Domain)?;
    if actual != guard.expected_revision {
        return Err(SourceError::UnhealthyRoot {
            root_role: root.root_role.clone(),
            reason: "root marker did not match its configured revision".to_owned(),
        });
    }
    Ok(())
}

fn rollout_filename_session_id(relative_path: &Path) -> Result<Option<Uuid>, SourceError> {
    let Some(filename) = relative_path.file_name() else {
        return Ok(None);
    };
    let Some(filename) = filename.to_str() else {
        return Err(SourceError::InvalidProviderRecord {
            relative_path: relative_path.to_owned(),
            reason: "rollout filename is not UTF-8".to_owned(),
        });
    };
    let Some(stem) = filename
        .strip_prefix("rollout-")
        .and_then(|value| value.strip_suffix(".jsonl"))
    else {
        return Ok(None);
    };
    let Some(uuid_start) = stem.len().checked_sub(36) else {
        return Ok(None);
    };
    if uuid_start == 0 || stem.as_bytes().get(uuid_start - 1) != Some(&b'-') {
        return Ok(None);
    }
    Uuid::parse_str(&stem[uuid_start..])
        .map(Some)
        .map_err(|_| SourceError::InvalidProviderRecord {
            relative_path: relative_path.to_owned(),
            reason: "rollout filename has no valid trailing UUID".to_owned(),
        })
}

fn session_metadata_id(file: &mut File) -> Result<Uuid, SourceError> {
    file.seek(SeekFrom::Start(0)).map_err(SourceError::Read)?;
    let mut reader = BufReader::new(file.take(MAX_SESSION_METADATA_LINE_BYTES));
    let mut line = Vec::new();
    let read = reader
        .read_until(b'\n', &mut line)
        .map_err(SourceError::Read)?;
    if read == MAX_SESSION_METADATA_LINE_BYTES as usize && !line.ends_with(b"\n") {
        return Err(SourceError::InvalidProviderRecord {
            relative_path: PathBuf::from("<opened rollout>"),
            reason: "first metadata line exceeds the provider limit".to_owned(),
        });
    }
    let value: Value =
        serde_json::from_slice(&line).map_err(|error| SourceError::InvalidProviderRecord {
            relative_path: PathBuf::from("<opened rollout>"),
            reason: format!("invalid first JSONL record: {error}"),
        })?;
    if value.get("type").and_then(Value::as_str) != Some("session_meta") {
        return Err(SourceError::InvalidProviderRecord {
            relative_path: PathBuf::from("<opened rollout>"),
            reason: "first JSONL record is not session_meta".to_owned(),
        });
    }
    let Some(id) = value
        .get("payload")
        .and_then(|payload| payload.get("id"))
        .and_then(Value::as_str)
    else {
        return Err(SourceError::InvalidProviderRecord {
            relative_path: PathBuf::from("<opened rollout>"),
            reason: "session_meta has no payload.id".to_owned(),
        });
    };
    Uuid::parse_str(id).map_err(|_| SourceError::InvalidProviderRecord {
        relative_path: PathBuf::from("<opened rollout>"),
        reason: "session_meta payload.id is not a UUID".to_owned(),
    })
}

fn normalized_relative_path(path: &Path) -> Result<String, SourceError> {
    let components = relative_components(path)?;
    components
        .into_iter()
        .map(|component| {
            component.to_str().map(str::to_owned).ok_or_else(|| {
                SourceError::InvalidProviderRecord {
                    relative_path: path.to_owned(),
                    reason: "source-relative path is not UTF-8".to_owned(),
                }
            })
        })
        .collect::<Result<Vec<_>, _>>()
        .map(|components| components.join("/"))
}

fn relative_components(path: &Path) -> Result<Vec<&OsStr>, SourceError> {
    let mut components = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(value) => components.push(value),
            _ => {
                return Err(SourceError::InvalidConfiguration(
                    "relative path must contain only normal components".to_owned(),
                ));
            }
        }
    }
    if components.is_empty() {
        return Err(SourceError::InvalidConfiguration(
            "relative path must not be empty".to_owned(),
        ));
    }
    Ok(components)
}

#[cfg(unix)]
fn open_directory_no_follow(path: &Path) -> Result<File, SourceError> {
    use std::os::fd::FromRawFd;
    use std::os::unix::ffi::OsStrExt;

    let path = CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        SourceError::InvalidConfiguration("filesystem path contains a NUL byte".to_owned())
    })?;
    let descriptor = unsafe {
        libc::open(
            path.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW,
        )
    };
    if descriptor < 0 {
        return Err(SourceError::Read(io::Error::last_os_error()));
    }
    Ok(unsafe { File::from_raw_fd(descriptor) })
}

#[cfg(unix)]
fn open_file_beneath(root: &Path, relative_path: &Path) -> Result<File, SourceError> {
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::ffi::OsStrExt;

    let components = relative_components(relative_path)?;
    let mut directory = open_directory_no_follow(root)?;
    for component in &components[..components.len() - 1] {
        let component = CString::new(component.as_bytes()).map_err(|_| {
            SourceError::InvalidConfiguration("filesystem path contains a NUL byte".to_owned())
        })?;
        let descriptor = unsafe {
            libc::openat(
                directory.as_raw_fd(),
                component.as_ptr(),
                libc::O_RDONLY | libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW,
            )
        };
        if descriptor < 0 {
            return Err(SourceError::Read(io::Error::last_os_error()));
        }
        directory = unsafe { File::from_raw_fd(descriptor) };
    }
    let filename = CString::new(
        components
            .last()
            .expect("relative path has one component")
            .as_bytes(),
    )
    .map_err(|_| {
        SourceError::InvalidConfiguration("filesystem path contains a NUL byte".to_owned())
    })?;
    let descriptor = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            filename.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };
    if descriptor < 0 {
        return Err(SourceError::Read(io::Error::last_os_error()));
    }
    Ok(unsafe { File::from_raw_fd(descriptor) })
}

#[cfg(not(unix))]
fn open_directory_no_follow(_path: &Path) -> Result<File, SourceError> {
    Err(SourceError::UnsupportedPlatform)
}

#[cfg(not(unix))]
fn open_file_beneath(_root: &Path, _relative_path: &Path) -> Result<File, SourceError> {
    Err(SourceError::UnsupportedPlatform)
}

#[derive(Debug, Error)]
pub enum SourceError {
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Catalog(#[from] CatalogError),
    #[error("invalid source configuration: {0}")]
    InvalidConfiguration(String),
    #[error("source root {root_role:?} is unavailable: {source}")]
    RootUnavailable {
        root_role: String,
        #[source]
        source: io::Error,
    },
    #[error("source root {root_role:?} is unhealthy: {reason}")]
    UnhealthyRoot { root_role: String, reason: String },
    #[error("failed to inspect source filesystem: {0}")]
    Read(#[source] io::Error),
    #[error("source entry is not a regular file")]
    UnexpectedFileType,
    #[error("invalid provider record {relative_path:?}: {reason}")]
    InvalidProviderRecord {
        relative_path: PathBuf,
        reason: String,
    },
    #[error("source {source_id:?} has duplicate live identity {identity:?}")]
    DuplicateLiveIdentity {
        source_id: String,
        identity: Box<RecordIdentity>,
    },
    #[error("source counters overflowed")]
    CounterOverflow,
    #[error("guarded filesystem access requires Unix openat support")]
    UnsupportedPlatform,
}

#[cfg(test)]
mod tests {
    use std::fs::{self, File};
    use std::io::Write;
    use std::path::{Path, PathBuf};

    use tempfile::tempdir;
    use uuid::Uuid;

    use super::{
        Blake3RevisionHasher, CodexRolloutLayout, DeletionMode, Reconciler, RevisionHasher,
        RootGuard, SourceConfig, SourceError, SourceRoot, StabilityPolicy, TombstoneGrace,
    };
    use crate::catalog::Catalog;
    use crate::domain::{CanonicalRevision, EventKind, ProducerId};

    fn session_file(root: &Path, id: Uuid, body: &str) -> PathBuf {
        let sessions = root.join("sessions");
        fs::create_dir_all(&sessions).unwrap();
        let path = sessions.join(format!("rollout-2026-08-01T00-00-00-{id}.jsonl"));
        let mut file = File::create(&path).unwrap();
        writeln!(
            file,
            "{{\"type\":\"session_meta\",\"payload\":{{\"id\":\"{id}\"}}}}"
        )
        .unwrap();
        file.write_all(body.as_bytes()).unwrap();
        path
    }

    fn guarded_config(root: &Path, deletion_mode: DeletionMode) -> SourceConfig {
        fs::write(root.join(".bookkeeper-root"), b"bookkeeper test root\n").unwrap();
        let guard =
            RootGuard::from_marker_bytes(".bookkeeper-root", b"bookkeeper test root\n").unwrap();
        let source_root = SourceRoot::new("active", root, Some(guard)).unwrap();
        SourceConfig::new(
            "fixture-source",
            ProducerId::new(),
            vec![source_root],
            deletion_mode,
            StabilityPolicy::new(2).unwrap(),
        )
        .unwrap()
    }

    fn guarded_deletion() -> DeletionMode {
        DeletionMode::Guarded(TombstoneGrace::default())
    }

    #[test]
    fn codex_source_waits_for_stability_then_tracks_rewrite() {
        let directory = tempdir().unwrap();
        let root = directory.path();
        let id = Uuid::new_v4();
        let path = session_file(root, id, "first\n");
        let mut reconciler =
            Reconciler::new(guarded_config(root, guarded_deletion()), CodexRolloutLayout);
        let mut catalog = Catalog::open_in_memory().unwrap();

        let first = reconciler.scan(&mut catalog).unwrap();
        let second = reconciler.scan(&mut catalog).unwrap();
        fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"second\n")
            .unwrap();
        let changed = reconciler.scan(&mut catalog).unwrap();
        let stable_rewrite = reconciler.scan(&mut catalog).unwrap();

        assert_eq!(first.events.len(), 0);
        assert_eq!(first.awaiting_stability, 1);
        assert_eq!(second.events.len(), 1);
        assert_eq!(second.events[0].kind, EventKind::RevisionCommitted);
        assert_eq!(changed.events.len(), 0);
        assert_eq!(stable_rewrite.events.len(), 1);
        assert_eq!(stable_rewrite.events[0].kind, EventKind::RevisionCommitted);
        assert_eq!(catalog.events_after(0).unwrap().len(), 2);
    }

    #[test]
    fn stability_observations_survive_reconciler_restart() {
        let directory = tempdir().unwrap();
        let root = directory.path().join("source");
        fs::create_dir_all(&root).unwrap();
        session_file(&root, Uuid::new_v4(), "first\n");
        let config = guarded_config(&root, guarded_deletion());
        let database = directory.path().join("catalog.sqlite3");

        let first = {
            let mut catalog = Catalog::open(&database).unwrap();
            Reconciler::new(config.clone(), CodexRolloutLayout)
                .scan(&mut catalog)
                .unwrap()
        };
        let second = {
            let mut catalog = Catalog::open(&database).unwrap();
            Reconciler::new(config, CodexRolloutLayout)
                .scan(&mut catalog)
                .unwrap()
        };

        assert!(first.events.is_empty());
        assert_eq!(second.events.len(), 1);
        assert_eq!(second.events[0].kind, EventKind::RevisionCommitted);
    }

    #[test]
    fn guarded_absence_requires_two_complete_scans_before_tombstone() {
        let directory = tempdir().unwrap();
        let root = directory.path();
        let path = session_file(root, Uuid::new_v4(), "first\n");
        let mut reconciler =
            Reconciler::new(guarded_config(root, guarded_deletion()), CodexRolloutLayout);
        let mut catalog = Catalog::open_in_memory().unwrap();

        reconciler.scan(&mut catalog).unwrap();
        reconciler.scan(&mut catalog).unwrap();
        fs::remove_file(path).unwrap();
        let first_missing = reconciler.scan(&mut catalog).unwrap();
        let second_missing = reconciler.scan(&mut catalog).unwrap();

        assert!(first_missing.events.is_empty());
        assert_eq!(second_missing.events.len(), 1);
        assert_eq!(second_missing.events[0].kind, EventKind::RecordTombstoned);
    }

    #[test]
    fn disabled_deletion_never_turns_absence_into_a_tombstone() {
        let directory = tempdir().unwrap();
        let root = directory.path();
        let path = session_file(root, Uuid::new_v4(), "first\n");
        let mut reconciler = Reconciler::new(
            guarded_config(root, DeletionMode::Disabled),
            CodexRolloutLayout,
        );
        let mut catalog = Catalog::open_in_memory().unwrap();

        reconciler.scan(&mut catalog).unwrap();
        reconciler.scan(&mut catalog).unwrap();
        fs::remove_file(path).unwrap();
        let first_missing = reconciler.scan(&mut catalog).unwrap();
        let second_missing = reconciler.scan(&mut catalog).unwrap();

        assert!(first_missing.events.is_empty());
        assert!(second_missing.events.is_empty());
        assert_eq!(catalog.events_after(0).unwrap().len(), 1);
    }

    #[test]
    fn a_cross_root_move_is_seen_before_tombstone_grace() {
        let directory = tempdir().unwrap();
        let active = directory.path().join("active");
        let archived = directory.path().join("archived");
        fs::create_dir_all(&active).unwrap();
        fs::create_dir_all(&archived).unwrap();
        fs::write(active.join(".bookkeeper-root"), b"active root\n").unwrap();
        fs::write(archived.join(".bookkeeper-root"), b"archived root\n").unwrap();
        let active_guard =
            RootGuard::from_marker_bytes(".bookkeeper-root", b"active root\n").unwrap();
        let archived_guard =
            RootGuard::from_marker_bytes(".bookkeeper-root", b"archived root\n").unwrap();
        let active_root = SourceRoot::new("active", &active, Some(active_guard)).unwrap();
        let archived_root = SourceRoot::new("archived", &archived, Some(archived_guard)).unwrap();
        let config = SourceConfig::new(
            "moving-source",
            ProducerId::new(),
            vec![active_root, archived_root],
            guarded_deletion(),
            StabilityPolicy::new(2).unwrap(),
        )
        .unwrap();
        let path = session_file(&active, Uuid::new_v4(), "first\n");
        let archived_sessions = archived.join("sessions");
        fs::create_dir_all(&archived_sessions).unwrap();
        let moved = archived_sessions.join(path.file_name().unwrap());
        let mut reconciler = Reconciler::new(config, CodexRolloutLayout);
        let mut catalog = Catalog::open_in_memory().unwrap();

        reconciler.scan(&mut catalog).unwrap();
        reconciler.scan(&mut catalog).unwrap();
        fs::rename(&path, &moved).unwrap();
        let first_move_scan = reconciler.scan(&mut catalog).unwrap();
        let stable_move = reconciler.scan(&mut catalog).unwrap();

        assert!(first_move_scan.events.is_empty());
        assert_eq!(stable_move.events.len(), 1);
        assert_eq!(stable_move.events[0].kind, EventKind::LocationChanged);
        assert!(
            catalog
                .events_after(0)
                .unwrap()
                .iter()
                .all(|event| event.kind != EventKind::RecordTombstoned)
        );
    }

    #[test]
    fn guard_failure_creates_no_catalog_event() {
        let directory = tempdir().unwrap();
        let root = directory.path();
        session_file(root, Uuid::new_v4(), "body\n");
        let config = guarded_config(root, guarded_deletion());
        fs::write(root.join(".bookkeeper-root"), b"wrong root\n").unwrap();
        let mut reconciler = Reconciler::new(config, CodexRolloutLayout);
        let mut catalog = Catalog::open_in_memory().unwrap();

        assert!(matches!(
            reconciler.scan(&mut catalog),
            Err(SourceError::UnhealthyRoot { .. })
        ));
        assert!(catalog.events_after(0).unwrap().is_empty());
    }

    #[test]
    fn a_later_guard_failure_cannot_tombstone_a_known_record() {
        let directory = tempdir().unwrap();
        let root = directory.path();
        session_file(root, Uuid::new_v4(), "body\n");
        let mut reconciler =
            Reconciler::new(guarded_config(root, guarded_deletion()), CodexRolloutLayout);
        let mut catalog = Catalog::open_in_memory().unwrap();

        reconciler.scan(&mut catalog).unwrap();
        reconciler.scan(&mut catalog).unwrap();
        fs::write(root.join(".bookkeeper-root"), b"wrong root\n").unwrap();
        assert!(matches!(
            reconciler.scan(&mut catalog),
            Err(SourceError::UnhealthyRoot { .. })
        ));
        let events = catalog.events_after(0).unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, EventKind::RevisionCommitted);
    }

    #[test]
    fn metadata_filename_mismatch_is_rejected_without_guessing_identity() {
        let directory = tempdir().unwrap();
        let root = directory.path();
        let filename_id = Uuid::new_v4();
        let path = session_file(root, filename_id, "body\n");
        let different_id = Uuid::new_v4();
        fs::write(
            &path,
            format!(
                "{{\"type\":\"session_meta\",\"payload\":{{\"id\":\"{different_id}\"}}}}\nbody\n"
            ),
        )
        .unwrap();
        let mut reconciler = Reconciler::new(
            guarded_config(root, DeletionMode::Disabled),
            CodexRolloutLayout,
        );
        let mut catalog = Catalog::open_in_memory().unwrap();

        assert!(matches!(
            reconciler.scan(&mut catalog),
            Err(SourceError::InvalidProviderRecord { .. })
        ));
        assert!(catalog.events_after(0).unwrap().is_empty());
    }

    #[test]
    fn duplicate_live_session_identity_blocks_the_whole_scan_before_commit() {
        let directory = tempdir().unwrap();
        let root = directory.path();
        let id = Uuid::new_v4();
        session_file(root, id, "first\n");
        let duplicate_directory = root.join("duplicate");
        fs::create_dir_all(&duplicate_directory).unwrap();
        let duplicate = duplicate_directory.join(format!("rollout-2026-08-01T00-00-01-{id}.jsonl"));
        fs::write(
            duplicate,
            format!("{{\"type\":\"session_meta\",\"payload\":{{\"id\":\"{id}\"}}}}\nsecond\n"),
        )
        .unwrap();
        let mut reconciler = Reconciler::new(
            guarded_config(root, DeletionMode::Disabled),
            CodexRolloutLayout,
        );
        let mut catalog = Catalog::open_in_memory().unwrap();

        assert!(matches!(
            reconciler.scan(&mut catalog),
            Err(SourceError::DuplicateLiveIdentity { .. })
        ));
        assert!(catalog.events_after(0).unwrap().is_empty());
    }

    struct MutatingHasher {
        path: PathBuf,
    }

    impl RevisionHasher for MutatingHasher {
        fn hash(&self, file: &mut File) -> Result<CanonicalRevision, SourceError> {
            fs::write(&self.path, b"changed during hash\n").unwrap();
            Blake3RevisionHasher.hash(file)
        }
    }

    struct GuardMutatingHasher {
        marker: PathBuf,
    }

    impl RevisionHasher for GuardMutatingHasher {
        fn hash(&self, file: &mut File) -> Result<CanonicalRevision, SourceError> {
            fs::write(&self.marker, b"replaced guard\n").unwrap();
            Blake3RevisionHasher.hash(file)
        }
    }

    #[test]
    fn changed_file_during_hash_never_creates_a_revision() {
        let directory = tempdir().unwrap();
        let root = directory.path();
        let path = session_file(root, Uuid::new_v4(), "body\n");
        let config = guarded_config(root, DeletionMode::Disabled);
        let mut reconciler =
            Reconciler::with_hasher(config, CodexRolloutLayout, MutatingHasher { path });
        let mut catalog = Catalog::open_in_memory().unwrap();

        reconciler.scan(&mut catalog).unwrap();
        let report = reconciler.scan(&mut catalog).unwrap();

        assert_eq!(report.changed_during_scan, 1);
        assert!(catalog.events_after(0).unwrap().is_empty());
    }

    #[test]
    fn a_guard_changed_after_hash_blocks_all_pending_events() {
        let directory = tempdir().unwrap();
        let root = directory.path();
        session_file(root, Uuid::new_v4(), "body\n");
        let config = guarded_config(root, guarded_deletion());
        let mut reconciler = Reconciler::with_hasher(
            config,
            CodexRolloutLayout,
            GuardMutatingHasher {
                marker: root.join(".bookkeeper-root"),
            },
        );
        let mut catalog = Catalog::open_in_memory().unwrap();

        reconciler.scan(&mut catalog).unwrap();
        assert!(matches!(
            reconciler.scan(&mut catalog),
            Err(SourceError::UnhealthyRoot { .. })
        ));
        assert!(catalog.events_after(0).unwrap().is_empty());
    }
}
