use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use thiserror::Error;
use uuid::Uuid;

use crate::domain::{
    ArchiveEvent, CanonicalRevision, DomainError, EventId, EventKind, LogicalLocation, RecordId,
    RecordIdentity, RecordState, RevisionId,
};

const SCHEMA_VERSION: i64 = 1;

/// The durable V1.5 catalog. It is intentionally single-process and local-FS
/// oriented; deployment code is responsible for placing its database only on a
/// filesystem with the locking and `fsync` behavior required by the contract.
pub struct Catalog {
    connection: Connection,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CurrentRecord {
    pub id: RecordId,
    pub identity: RecordIdentity,
    pub location: LogicalLocation,
    pub record_version: u64,
    pub state: RecordState,
    pub revision_id: Option<RevisionId>,
    pub revision: Option<CanonicalRevision>,
}

#[derive(Debug, Error)]
pub enum CatalogError {
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("catalog schema version {found} is newer than this binary supports ({supported})")]
    NewerSchema { found: i64, supported: i64 },
    #[error("catalog data is corrupt: {0}")]
    Corrupt(String),
    #[error("system clock is before the Unix epoch")]
    ClockBeforeEpoch,
}

impl Catalog {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, CatalogError> {
        let mut connection = Connection::open(path)?;
        configure_connection(&mut connection)?;
        migrate(&mut connection)?;
        Ok(Self { connection })
    }

    pub fn open_in_memory() -> Result<Self, CatalogError> {
        let mut connection = Connection::open_in_memory()?;
        configure_connection(&mut connection)?;
        migrate(&mut connection)?;
        Ok(Self { connection })
    }

    pub fn schema_version(&self) -> Result<i64, CatalogError> {
        self.connection
            .query_row(
                "SELECT COALESCE(MAX(version), 0) FROM schema_migrations",
                [],
                |row| row.get(0),
            )
            .map_err(Into::into)
    }

    /// Observe one verified present record. The caller owns source safety and
    /// stability; this transaction owns identity, version, and event ordering.
    pub fn observe_present(
        &mut self,
        identity: &RecordIdentity,
        location: &LogicalLocation,
        revision: CanonicalRevision,
    ) -> Result<Vec<ArchiveEvent>, CatalogError> {
        self.observe_present_at(identity, location, revision, now_ms()?)
    }

    pub fn observe_present_at(
        &mut self,
        identity: &RecordIdentity,
        location: &LogicalLocation,
        revision: CanonicalRevision,
        committed_at_ms: i64,
    ) -> Result<Vec<ArchiveEvent>, CatalogError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut record = find_record(&transaction, identity)?;
        let mut events = Vec::new();

        if record.is_none() {
            let id = RecordId::new();
            transaction.execute(
                "INSERT INTO records (
                    id, producer_id, agent_namespace, session_id, record_kind,
                    record_key, root_role, source_relative_path, record_version,
                    state, current_revision_id, created_at_ms, updated_at_ms
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 0, 'active', NULL, ?9, ?9)",
                params![
                    id.as_uuid().as_bytes(),
                    identity.producer_id().as_uuid().as_bytes(),
                    identity.agent_namespace(),
                    identity.session_id(),
                    identity.record_kind(),
                    identity.record_key(),
                    location.root_role(),
                    location.source_relative_path(),
                    committed_at_ms,
                ],
            )?;
            record = Some(RecordRow::new(id, identity.clone(), location.clone()));
        }

        let mut record = record.expect("record was inserted or found");

        if record.state == RecordState::Tombstoned {
            let current_revision_id = record.current_revision_id;
            let event = transition(
                &transaction,
                &mut record,
                EventKind::RecordRestored,
                current_revision_id,
                Some(location),
                RecordState::Active,
                committed_at_ms,
            )?;
            events.push(event);
        }

        let revision_id = ensure_revision(&transaction, record.id, revision, committed_at_ms)?;
        let same_revision = record.current_revision == Some(revision);
        let same_location = record.location == *location;

        if !same_revision {
            let event = transition(
                &transaction,
                &mut record,
                EventKind::RevisionCommitted,
                Some(revision_id),
                Some(location),
                RecordState::Active,
                committed_at_ms,
            )?;
            events.push(event);
        } else if !same_location {
            let current_revision_id = record.current_revision_id;
            let event = transition(
                &transaction,
                &mut record,
                EventKind::LocationChanged,
                current_revision_id,
                Some(location),
                RecordState::Active,
                committed_at_ms,
            )?;
            events.push(event);
        }

        transaction.commit()?;
        Ok(events)
    }

    /// Record a deletion only after the reconciler's guarded root and grace
    /// policy has already accepted it. Absence by itself never calls this API.
    pub fn tombstone(
        &mut self,
        identity: &RecordIdentity,
    ) -> Result<Option<ArchiveEvent>, CatalogError> {
        self.tombstone_at(identity, now_ms()?)
    }

    pub fn tombstone_at(
        &mut self,
        identity: &RecordIdentity,
        committed_at_ms: i64,
    ) -> Result<Option<ArchiveEvent>, CatalogError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(mut record) = find_record(&transaction, identity)? else {
            transaction.commit()?;
            return Ok(None);
        };

        if record.state == RecordState::Tombstoned {
            transaction.commit()?;
            return Ok(None);
        }

        let current_revision_id = record.current_revision_id;
        let event = transition(
            &transaction,
            &mut record,
            EventKind::RecordTombstoned,
            current_revision_id,
            None,
            RecordState::Tombstoned,
            committed_at_ms,
        )?;
        transaction.commit()?;
        Ok(Some(event))
    }

    pub fn current_record(
        &self,
        identity: &RecordIdentity,
    ) -> Result<Option<CurrentRecord>, CatalogError> {
        find_record(&self.connection, identity)?
            .map(CurrentRecord::try_from)
            .transpose()
    }

    pub fn events_after(&self, sequence: u64) -> Result<Vec<ArchiveEvent>, CatalogError> {
        let mut statement = self.connection.prepare(
            "SELECT id, sequence, record_id, record_version, kind, revision_id, committed_at_ms
             FROM events
             WHERE sequence > ?1
             ORDER BY sequence ASC",
        )?;
        let rows =
            statement.query_map(params![as_i64(sequence, "event sequence")?], event_from_row)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }
}

fn find_record(
    connection: &Connection,
    identity: &RecordIdentity,
) -> Result<Option<RecordRow>, CatalogError> {
    connection
        .query_row(
            "SELECT r.id, r.record_version, r.state, r.root_role, r.source_relative_path,
                    r.current_revision_id, rv.byte_length, rv.digest
             FROM records AS r
             LEFT JOIN revisions AS rv ON rv.id = r.current_revision_id
             WHERE r.producer_id = ?1
               AND r.agent_namespace = ?2
               AND r.session_id = ?3
               AND r.record_kind = ?4
               AND r.record_key = ?5",
            params![
                identity.producer_id().as_uuid().as_bytes(),
                identity.agent_namespace(),
                identity.session_id(),
                identity.record_kind(),
                identity.record_key(),
            ],
            |row| {
                let id = record_id_from_blob(row.get::<_, Vec<u8>>(0)?)
                    .map_err(to_sql_conversion_error)?;
                let record_version = as_u64(row.get::<_, i64>(1)?, "record version")
                    .map_err(to_sql_conversion_error)?;
                let state = RecordState::from_db(&row.get::<_, String>(2)?)
                    .map_err(|error| to_sql_conversion_error(error.into()))?;
                let location =
                    LogicalLocation::new(row.get::<_, String>(3)?, row.get::<_, String>(4)?)
                        .map_err(|error| to_sql_conversion_error(error.into()))?;
                let revision_id = row
                    .get::<_, Option<Vec<u8>>>(5)?
                    .map(revision_id_from_blob)
                    .transpose()
                    .map_err(to_sql_conversion_error)?;
                let byte_length = row
                    .get::<_, Option<i64>>(6)?
                    .map(|value| as_u64(value, "revision byte length"))
                    .transpose()
                    .map_err(to_sql_conversion_error)?;
                let digest = row
                    .get::<_, Option<Vec<u8>>>(7)?
                    .map(digest_from_blob)
                    .transpose()
                    .map_err(to_sql_conversion_error)?;
                let current_revision = match (byte_length, digest) {
                    (Some(byte_length), Some(digest)) => {
                        Some(CanonicalRevision::from_parts(byte_length, digest))
                    }
                    (None, None) => None,
                    _ => {
                        return Err(to_sql_conversion_error(CatalogError::Corrupt(
                            "incomplete current revision".to_owned(),
                        )));
                    }
                };

                Ok(RecordRow {
                    id,
                    identity: identity.clone(),
                    location,
                    record_version,
                    state,
                    current_revision_id: revision_id,
                    current_revision,
                })
            },
        )
        .optional()
        .map_err(Into::into)
}

fn ensure_revision(
    transaction: &Transaction<'_>,
    record_id: RecordId,
    revision: CanonicalRevision,
    observed_at_ms: i64,
) -> Result<RevisionId, CatalogError> {
    let existing = transaction
        .query_row(
            "SELECT id FROM revisions WHERE record_id = ?1 AND byte_length = ?2 AND digest = ?3",
            params![
                record_id.as_uuid().as_bytes(),
                as_i64(revision.byte_length(), "revision byte length")?,
                revision.digest().as_slice(),
            ],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()?;

    if let Some(id) = existing {
        transaction.execute(
            "UPDATE revisions SET last_observed_at_ms = ?1 WHERE id = ?2",
            params![observed_at_ms, id],
        )?;
        return revision_id_from_blob(id);
    }

    let id = RevisionId::new();
    transaction.execute(
        "INSERT INTO revisions (
            id, record_id, byte_length, digest, availability, first_observed_at_ms,
            last_observed_at_ms
         ) VALUES (?1, ?2, ?3, ?4, 'current_external', ?5, ?5)",
        params![
            id.as_uuid().as_bytes(),
            record_id.as_uuid().as_bytes(),
            as_i64(revision.byte_length(), "revision byte length")?,
            revision.digest().as_slice(),
            observed_at_ms,
        ],
    )?;
    Ok(id)
}

fn transition(
    transaction: &Transaction<'_>,
    record: &mut RecordRow,
    kind: EventKind,
    revision_id: Option<RevisionId>,
    location: Option<&LogicalLocation>,
    state: RecordState,
    committed_at_ms: i64,
) -> Result<ArchiveEvent, CatalogError> {
    let next_version = record
        .record_version
        .checked_add(1)
        .ok_or_else(|| CatalogError::Corrupt("record version overflow".to_owned()))?;
    let location = location.unwrap_or(&record.location);
    let event_id = EventId::new();

    transaction.execute(
        "UPDATE records
         SET root_role = ?1,
             source_relative_path = ?2,
             record_version = ?3,
             state = ?4,
             current_revision_id = ?5,
             updated_at_ms = ?6
         WHERE id = ?7",
        params![
            location.root_role(),
            location.source_relative_path(),
            as_i64(next_version, "record version")?,
            state.as_db(),
            revision_id.map(|id| id.as_uuid().as_bytes().to_vec()),
            committed_at_ms,
            record.id.as_uuid().as_bytes(),
        ],
    )?;
    transaction.execute(
        "INSERT INTO events (id, record_id, record_version, kind, revision_id, committed_at_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            event_id.as_uuid().as_bytes(),
            record.id.as_uuid().as_bytes(),
            as_i64(next_version, "record version")?,
            kind.as_db(),
            revision_id.map(|id| id.as_uuid().as_bytes().to_vec()),
            committed_at_ms,
        ],
    )?;

    let sequence = as_u64(transaction.last_insert_rowid(), "event sequence")?;
    record.record_version = next_version;
    record.location = location.clone();
    record.state = state;
    record.current_revision_id = revision_id;
    Ok(ArchiveEvent {
        id: event_id,
        sequence,
        record_id: record.id,
        record_version: next_version,
        kind,
        revision_id,
        committed_at_ms,
    })
}

#[derive(Clone, Debug)]
struct RecordRow {
    id: RecordId,
    identity: RecordIdentity,
    location: LogicalLocation,
    record_version: u64,
    state: RecordState,
    current_revision_id: Option<RevisionId>,
    current_revision: Option<CanonicalRevision>,
}

impl RecordRow {
    fn new(id: RecordId, identity: RecordIdentity, location: LogicalLocation) -> Self {
        Self {
            id,
            identity,
            location,
            record_version: 0,
            state: RecordState::Active,
            current_revision_id: None,
            current_revision: None,
        }
    }
}

impl TryFrom<RecordRow> for CurrentRecord {
    type Error = CatalogError;

    fn try_from(record: RecordRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: record.id,
            identity: record.identity,
            location: record.location,
            record_version: record.record_version,
            state: record.state,
            revision_id: record.current_revision_id,
            revision: record.current_revision,
        })
    }
}

fn event_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ArchiveEvent> {
    let id = row.get::<_, Vec<u8>>(0)?;
    let sequence = row.get::<_, i64>(1)?;
    let record_id = row.get::<_, Vec<u8>>(2)?;
    let record_version = row.get::<_, i64>(3)?;
    let kind = row.get::<_, String>(4)?;
    let revision_id = row.get::<_, Option<Vec<u8>>>(5)?;
    let committed_at_ms = row.get::<_, i64>(6)?;

    Ok(ArchiveEvent {
        id: EventId::from_uuid(uuid_from_blob(&id).map_err(to_sql_conversion_error)?),
        sequence: as_u64(sequence, "event sequence").map_err(to_sql_conversion_error)?,
        record_id: RecordId::from_uuid(
            uuid_from_blob(&record_id).map_err(to_sql_conversion_error)?,
        ),
        record_version: as_u64(record_version, "record version")
            .map_err(to_sql_conversion_error)?,
        kind: EventKind::from_db(&kind).map_err(|error| to_sql_conversion_error(error.into()))?,
        revision_id: revision_id
            .as_deref()
            .map(uuid_from_blob)
            .transpose()
            .map_err(to_sql_conversion_error)?
            .map(RevisionId::from_uuid),
        committed_at_ms,
    })
}

fn configure_connection(connection: &mut Connection) -> Result<(), CatalogError> {
    connection.execute_batch(
        "PRAGMA foreign_keys = ON;
         PRAGMA journal_mode = WAL;
         PRAGMA synchronous = FULL;",
    )?;
    connection.busy_timeout(std::time::Duration::from_secs(5))?;
    Ok(())
}

fn migrate(connection: &mut Connection) -> Result<(), CatalogError> {
    let transaction = connection.transaction()?;
    transaction.execute_batch(
        "CREATE TABLE IF NOT EXISTS schema_migrations (
             version INTEGER PRIMARY KEY NOT NULL,
             applied_at_ms INTEGER NOT NULL
         );",
    )?;
    let found: i64 = transaction.query_row(
        "SELECT COALESCE(MAX(version), 0) FROM schema_migrations",
        [],
        |row| row.get(0),
    )?;
    if found > SCHEMA_VERSION {
        return Err(CatalogError::NewerSchema {
            found,
            supported: SCHEMA_VERSION,
        });
    }

    if found < 1 {
        transaction.execute_batch(
            "CREATE TABLE records (
                 id BLOB PRIMARY KEY NOT NULL CHECK(length(id) = 16),
                 producer_id BLOB NOT NULL CHECK(length(producer_id) = 16),
                 agent_namespace TEXT NOT NULL,
                 session_id TEXT NOT NULL,
                 record_kind TEXT NOT NULL,
                 record_key TEXT NOT NULL,
                 root_role TEXT NOT NULL,
                 source_relative_path TEXT NOT NULL,
                 record_version INTEGER NOT NULL CHECK(record_version >= 0),
                 state TEXT NOT NULL CHECK(state IN ('active', 'tombstoned')),
                 current_revision_id BLOB NULL REFERENCES revisions(id),
                 created_at_ms INTEGER NOT NULL,
                 updated_at_ms INTEGER NOT NULL,
                 UNIQUE(producer_id, agent_namespace, session_id, record_kind, record_key)
             );
             CREATE TABLE revisions (
                 id BLOB PRIMARY KEY NOT NULL CHECK(length(id) = 16),
                 record_id BLOB NOT NULL REFERENCES records(id),
                 byte_length INTEGER NOT NULL CHECK(byte_length >= 0),
                 digest BLOB NOT NULL CHECK(length(digest) = 32),
                 availability TEXT NOT NULL CHECK(availability IN ('current_external', 'retained_chunks', 'unavailable_historical')),
                 first_observed_at_ms INTEGER NOT NULL,
                 last_observed_at_ms INTEGER NOT NULL,
                 UNIQUE(record_id, byte_length, digest)
             );
             CREATE TABLE events (
                 sequence INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                 id BLOB NOT NULL UNIQUE CHECK(length(id) = 16),
                 record_id BLOB NOT NULL REFERENCES records(id),
                 record_version INTEGER NOT NULL CHECK(record_version > 0),
                 kind TEXT NOT NULL CHECK(kind IN ('revision_committed', 'location_changed', 'record_tombstoned', 'record_restored')),
                 revision_id BLOB NULL REFERENCES revisions(id),
                 committed_at_ms INTEGER NOT NULL,
                 UNIQUE(record_id, record_version)
             );
             CREATE INDEX events_record_order ON events(record_id, record_version);
             CREATE INDEX revisions_record_observed ON revisions(record_id, last_observed_at_ms);",
        )?;
        transaction.execute(
            "INSERT INTO schema_migrations (version, applied_at_ms) VALUES (?1, ?2)",
            params![SCHEMA_VERSION, now_ms()?],
        )?;
    }
    transaction.commit()?;
    Ok(())
}

fn now_ms() -> Result<i64, CatalogError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| CatalogError::ClockBeforeEpoch)?;
    i64::try_from(duration.as_millis())
        .map_err(|_| CatalogError::Corrupt("Unix timestamp exceeds i64 milliseconds".to_owned()))
}

fn as_i64(value: u64, field: &'static str) -> Result<i64, CatalogError> {
    i64::try_from(value)
        .map_err(|_| CatalogError::Corrupt(format!("{field} exceeds SQLite INTEGER range")))
}

fn as_u64(value: i64, field: &'static str) -> Result<u64, CatalogError> {
    u64::try_from(value).map_err(|_| CatalogError::Corrupt(format!("negative {field}")))
}

fn uuid_from_blob(value: &[u8]) -> Result<Uuid, CatalogError> {
    Uuid::from_slice(value).map_err(|_| CatalogError::Corrupt("invalid UUID blob".to_owned()))
}

fn record_id_from_blob(value: Vec<u8>) -> Result<RecordId, CatalogError> {
    Ok(RecordId::from_uuid(uuid_from_blob(&value)?))
}

fn revision_id_from_blob(value: Vec<u8>) -> Result<RevisionId, CatalogError> {
    Ok(RevisionId::from_uuid(uuid_from_blob(&value)?))
}

fn digest_from_blob(value: Vec<u8>) -> Result<[u8; blake3::OUT_LEN], CatalogError> {
    value
        .try_into()
        .map_err(|_| CatalogError::Corrupt("invalid BLAKE3 digest length".to_owned()))
}

fn to_sql_conversion_error(error: CatalogError) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Blob, Box::new(error))
}

#[cfg(test)]
mod tests {
    use super::Catalog;
    use crate::domain::{
        CanonicalRevision, EventKind, LogicalLocation, ProducerId, RecordIdentity, RecordState,
    };
    use tempfile::tempdir;

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

    fn location(path: &str) -> LogicalLocation {
        LogicalLocation::new("active", path).unwrap()
    }

    #[test]
    fn schema_and_unchanged_observation_are_stable() {
        let mut catalog = Catalog::open_in_memory().unwrap();
        assert_eq!(catalog.schema_version().unwrap(), 1);
        let identity = identity();
        let revision = CanonicalRevision::from_bytes(b"first\n");

        let first = catalog
            .observe_present_at(&identity, &location("sessions/a.jsonl"), revision, 100)
            .unwrap();
        let unchanged = catalog
            .observe_present_at(&identity, &location("sessions/a.jsonl"), revision, 200)
            .unwrap();

        assert_eq!(first.len(), 1);
        assert_eq!(first[0].kind, EventKind::RevisionCommitted);
        assert!(unchanged.is_empty());
        assert_eq!(catalog.events_after(0).unwrap().len(), 1);
    }

    #[test]
    fn moves_and_rewrites_preserve_identity_and_increment_versions() {
        let mut catalog = Catalog::open_in_memory().unwrap();
        let identity = identity();
        let first_revision = CanonicalRevision::from_bytes(b"first\n");
        let second_revision = CanonicalRevision::from_bytes(b"first\nsecond\n");

        let initial = catalog
            .observe_present_at(&identity, &location("active/a.jsonl"), first_revision, 100)
            .unwrap();
        let moved = catalog
            .observe_present_at(
                &identity,
                &location("archived/a.jsonl"),
                first_revision,
                200,
            )
            .unwrap();
        let rewritten = catalog
            .observe_present_at(
                &identity,
                &location("archived/a.jsonl"),
                second_revision,
                300,
            )
            .unwrap();
        let events = catalog.events_after(0).unwrap();
        let current = catalog.current_record(&identity).unwrap().unwrap();

        assert_eq!(initial[0].kind, EventKind::RevisionCommitted);
        assert_eq!(moved[0].kind, EventKind::LocationChanged);
        assert_eq!(rewritten[0].kind, EventKind::RevisionCommitted);
        assert_eq!(
            events
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(current.record_version, 3);
        assert_eq!(current.location.source_relative_path(), "archived/a.jsonl");
        assert_eq!(current.revision, Some(second_revision));
    }

    #[test]
    fn tombstone_and_restore_cannot_reuse_an_old_record_version() {
        let mut catalog = Catalog::open_in_memory().unwrap();
        let identity = identity();
        let revision = CanonicalRevision::from_bytes(b"first\n");

        catalog
            .observe_present_at(&identity, &location("active/a.jsonl"), revision, 100)
            .unwrap();
        let tombstone = catalog.tombstone_at(&identity, 200).unwrap().unwrap();
        let restore = catalog
            .observe_present_at(&identity, &location("archived/a.jsonl"), revision, 300)
            .unwrap();
        let current = catalog.current_record(&identity).unwrap().unwrap();

        assert_eq!(tombstone.kind, EventKind::RecordTombstoned);
        assert_eq!(tombstone.record_version, 2);
        assert_eq!(restore.len(), 1);
        assert_eq!(restore[0].kind, EventKind::RecordRestored);
        assert_eq!(restore[0].record_version, 3);
        assert_eq!(current.state, RecordState::Active);
        assert_eq!(current.record_version, 3);
    }

    #[test]
    fn one_session_can_have_multiple_stable_record_keys() {
        let mut catalog = Catalog::open_in_memory().unwrap();
        let producer = ProducerId::new();
        let transcript =
            RecordIdentity::new(producer, "codex", "session-a", "transcript", "primary").unwrap();
        let sidecar =
            RecordIdentity::new(producer, "codex", "session-a", "metadata", "primary").unwrap();
        let revision = CanonicalRevision::from_bytes(b"same bytes");

        catalog
            .observe_present_at(&transcript, &location("a.jsonl"), revision, 100)
            .unwrap();
        catalog
            .observe_present_at(&sidecar, &location("a.meta.json"), revision, 200)
            .unwrap();

        assert_eq!(catalog.events_after(0).unwrap().len(), 2);
        assert_ne!(
            catalog.current_record(&transcript).unwrap().unwrap().id,
            catalog.current_record(&sidecar).unwrap().unwrap().id
        );
    }

    #[test]
    fn file_catalog_reopens_with_its_ordered_event_history() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("catalog.sqlite3");
        let identity = identity();
        let revision = CanonicalRevision::from_bytes(b"durable\n");

        {
            let mut catalog = Catalog::open(&path).unwrap();
            catalog
                .observe_present_at(&identity, &location("sessions/a.jsonl"), revision, 100)
                .unwrap();
            let foreign_keys: i64 = catalog
                .connection
                .query_row("PRAGMA foreign_keys", [], |row| row.get(0))
                .unwrap();
            assert_eq!(foreign_keys, 1);
        }

        let catalog = Catalog::open(&path).unwrap();
        let events = catalog.events_after(0).unwrap();
        let current = catalog.current_record(&identity).unwrap().unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].sequence, 1);
        assert_eq!(current.revision, Some(revision));
    }
}
