use std::fmt;
use std::io::{self, Read};

use thiserror::Error;
use uuid::Uuid;

/// A random, stable archive identity for one authorized record producer.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ProducerId(Uuid);

impl ProducerId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn from_uuid(value: Uuid) -> Self {
        Self(value)
    }

    pub fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl Default for ProducerId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for ProducerId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct RecordId(Uuid);

impl RecordId {
    pub(crate) fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub(crate) fn from_uuid(value: Uuid) -> Self {
        Self(value)
    }

    pub fn as_uuid(self) -> Uuid {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct RevisionId(Uuid);

impl RevisionId {
    pub(crate) fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub(crate) fn from_uuid(value: Uuid) -> Self {
        Self(value)
    }

    pub fn as_uuid(self) -> Uuid {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct EventId(Uuid);

impl EventId {
    pub(crate) fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub(crate) fn from_uuid(value: Uuid) -> Self {
        Self(value)
    }

    pub fn as_uuid(self) -> Uuid {
        self.0
    }
}

/// The path-independent five-part identity of one raw agent record.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RecordIdentity {
    producer_id: ProducerId,
    agent_namespace: String,
    session_id: String,
    record_kind: String,
    record_key: String,
}

impl RecordIdentity {
    pub fn new(
        producer_id: ProducerId,
        agent_namespace: impl Into<String>,
        session_id: impl Into<String>,
        record_kind: impl Into<String>,
        record_key: impl Into<String>,
    ) -> Result<Self, DomainError> {
        Ok(Self {
            producer_id,
            agent_namespace: required_identifier("agent_namespace", agent_namespace.into())?,
            session_id: required_identifier("session_id", session_id.into())?,
            record_kind: required_identifier("record_kind", record_kind.into())?,
            record_key: required_identifier("record_key", record_key.into())?,
        })
    }

    pub fn producer_id(&self) -> ProducerId {
        self.producer_id
    }

    pub fn agent_namespace(&self) -> &str {
        &self.agent_namespace
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    pub fn record_kind(&self) -> &str {
        &self.record_kind
    }

    pub fn record_key(&self) -> &str {
        &self.record_key
    }
}

/// A provider-normalized current location. It is deliberately not record identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalLocation {
    root_role: String,
    source_relative_path: String,
}

impl LogicalLocation {
    pub fn new(
        root_role: impl Into<String>,
        source_relative_path: impl Into<String>,
    ) -> Result<Self, DomainError> {
        let root_role = required_identifier("root_role", root_role.into())?;
        if root_role.contains('/') || root_role.contains('\\') {
            return Err(DomainError::InvalidLocation(
                "root_role must be one normalized component".to_owned(),
            ));
        }

        let source_relative_path = source_relative_path.into();
        validate_relative_path(&source_relative_path)?;
        Ok(Self {
            root_role,
            source_relative_path,
        })
    }

    pub fn root_role(&self) -> &str {
        &self.root_role
    }

    pub fn source_relative_path(&self) -> &str {
        &self.source_relative_path
    }
}

/// BLAKE3-256 and byte length over the complete raw record.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct CanonicalRevision {
    byte_length: u64,
    digest: [u8; blake3::OUT_LEN],
}

impl CanonicalRevision {
    pub const ALGORITHM: &'static str = "blake3-256";

    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self {
            byte_length: bytes.len() as u64,
            digest: *blake3::hash(bytes).as_bytes(),
        }
    }

    pub fn from_reader(reader: impl Read) -> Result<Self, DomainError> {
        let mut reader = reader;
        let mut hasher = blake3::Hasher::new();
        let mut byte_length = 0_u64;
        let mut buffer = [0_u8; 128 * 1024];

        loop {
            let count = reader.read(&mut buffer).map_err(DomainError::Read)?;
            if count == 0 {
                break;
            }
            byte_length = byte_length
                .checked_add(count as u64)
                .ok_or(DomainError::RecordTooLarge)?;
            hasher.update(&buffer[..count]);
        }

        Ok(Self {
            byte_length,
            digest: *hasher.finalize().as_bytes(),
        })
    }

    pub fn byte_length(self) -> u64 {
        self.byte_length
    }

    pub fn digest(self) -> [u8; blake3::OUT_LEN] {
        self.digest
    }

    pub fn digest_hex(self) -> String {
        self.digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    pub(crate) fn from_parts(byte_length: u64, digest: [u8; blake3::OUT_LEN]) -> Self {
        Self {
            byte_length,
            digest,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecordState {
    Active,
    Tombstoned,
}

impl RecordState {
    pub(crate) fn as_db(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Tombstoned => "tombstoned",
        }
    }

    pub(crate) fn from_db(value: &str) -> Result<Self, DomainError> {
        match value {
            "active" => Ok(Self::Active),
            "tombstoned" => Ok(Self::Tombstoned),
            _ => Err(DomainError::CorruptState(value.to_owned())),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventKind {
    RevisionCommitted,
    LocationChanged,
    RecordTombstoned,
    RecordRestored,
}

impl EventKind {
    pub(crate) fn as_db(self) -> &'static str {
        match self {
            Self::RevisionCommitted => "revision_committed",
            Self::LocationChanged => "location_changed",
            Self::RecordTombstoned => "record_tombstoned",
            Self::RecordRestored => "record_restored",
        }
    }

    pub(crate) fn from_db(value: &str) -> Result<Self, DomainError> {
        match value {
            "revision_committed" => Ok(Self::RevisionCommitted),
            "location_changed" => Ok(Self::LocationChanged),
            "record_tombstoned" => Ok(Self::RecordTombstoned),
            "record_restored" => Ok(Self::RecordRestored),
            _ => Err(DomainError::CorruptState(value.to_owned())),
        }
    }
}

/// Consumer outcomes. A dead letter stops automatic retry but deliberately does
/// not release ordering for a later version of the same record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeliveryOutcome {
    Acknowledged,
    Superseded,
    IgnoredByPolicy,
    DeadLettered,
}

impl DeliveryOutcome {
    pub fn advances_record_order(self) -> bool {
        !matches!(self, Self::DeadLettered)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArchiveEvent {
    pub id: EventId,
    pub sequence: u64,
    pub record_id: RecordId,
    pub record_version: u64,
    pub kind: EventKind,
    pub revision_id: Option<RevisionId>,
    pub committed_at_ms: i64,
}

#[derive(Debug, Error)]
pub enum DomainError {
    #[error("{field} must be non-empty, trimmed, and free of NUL bytes")]
    InvalidIdentifier { field: &'static str },
    #[error("invalid source-relative path: {0}")]
    InvalidLocation(String),
    #[error("record byte length exceeds u64")]
    RecordTooLarge,
    #[error("failed to read raw record: {0}")]
    Read(#[source] io::Error),
    #[error("catalog contains an unknown state value: {0}")]
    CorruptState(String),
}

fn required_identifier(field: &'static str, value: String) -> Result<String, DomainError> {
    if value.is_empty() || value.trim() != value || value.contains('\0') {
        return Err(DomainError::InvalidIdentifier { field });
    }
    Ok(value)
}

fn validate_relative_path(value: &str) -> Result<(), DomainError> {
    if value.is_empty() || value.starts_with('/') || value.starts_with('\\') || value.contains('\0')
    {
        return Err(DomainError::InvalidLocation(
            "path must be a non-empty relative path".to_owned(),
        ));
    }

    if value
        .split(['/', '\\'])
        .any(|component| component.is_empty() || component == "." || component == "..")
    {
        return Err(DomainError::InvalidLocation(
            "path may not contain empty, dot, or parent components".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::{CanonicalRevision, DomainError, LogicalLocation, ProducerId, RecordIdentity};

    #[test]
    fn canonical_revision_is_independent_of_reader_chunking() {
        let bytes = b"one\ntwo\nthree\n";
        let from_bytes = CanonicalRevision::from_bytes(bytes);
        let from_reader = CanonicalRevision::from_reader(Cursor::new(bytes)).unwrap();

        assert_eq!(from_bytes, from_reader);
        assert_eq!(from_bytes.byte_length(), bytes.len() as u64);
        assert_eq!(from_bytes.digest_hex().len(), 64);
    }

    #[test]
    fn identity_is_path_independent_and_location_is_confined() {
        let identity = RecordIdentity::new(
            ProducerId::new(),
            "codex",
            "session-1",
            "transcript",
            "primary",
        )
        .unwrap();
        assert_eq!(identity.session_id(), "session-1");

        assert!(LogicalLocation::new("active", "sessions/session-1.jsonl").is_ok());
        assert!(matches!(
            LogicalLocation::new("active", "../outside.jsonl"),
            Err(DomainError::InvalidLocation(_))
        ));
    }
}
