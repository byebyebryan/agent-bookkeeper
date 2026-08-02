use std::fs::{self, File, Metadata, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{
    Connection, MAIN_DB, OpenFlags, OptionalExtension, Transaction, TransactionBehavior, params,
};
use thiserror::Error;
use uuid::Uuid;

use crate::domain::{
    ArchiveEvent, CanonicalRevision, DomainError, EventId, EventKind, LogicalLocation, ProducerId,
    RecordId, RecordIdentity, RecordState, RevisionId,
};

const SCHEMA_VERSION: i64 = 7;

/// Deployment-scoped provenance for one reconciled filesystem source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceRegistration {
    source_id: String,
    producer_id: ProducerId,
    identity_schema_name: String,
    identity_schema_version: u32,
}

impl SourceRegistration {
    pub fn new(
        source_id: impl Into<String>,
        producer_id: ProducerId,
        identity_schema_name: impl Into<String>,
        identity_schema_version: u32,
    ) -> Result<Self, CatalogError> {
        let source_id = valid_source_field("source_id", source_id.into())?;
        let identity_schema_name =
            valid_source_field("identity_schema_name", identity_schema_name.into())?;
        if identity_schema_version == 0 {
            return Err(CatalogError::InvalidSourceRegistration(
                "identity_schema_version must be non-zero".to_owned(),
            ));
        }
        Ok(Self {
            source_id,
            producer_id,
            identity_schema_name,
            identity_schema_version,
        })
    }

    pub fn source_id(&self) -> &str {
        &self.source_id
    }

    pub fn producer_id(&self) -> ProducerId {
        self.producer_id
    }

    pub fn identity_schema_name(&self) -> &str {
        &self.identity_schema_name
    }

    pub fn identity_schema_version(&self) -> u32 {
        self.identity_schema_version
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceScan {
    source_id: String,
    generation: u64,
}

/// Durable, source-local position for the byte-budgeted integrity scrub. The
/// position is a logical location, never an absolute source path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceScrubProgress {
    pub source_id: String,
    pub next_after: Option<LogicalLocation>,
    pub completed_cycles: u64,
    pub last_completed_at_ms: Option<i64>,
}

impl SourceScan {
    pub fn source_id(&self) -> &str {
        &self.source_id
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }
}

/// Cheap metadata used only to decide whether a full hash is required.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceFingerprint {
    byte_length: u64,
    modified_seconds: i64,
    modified_nanoseconds: u32,
    device: Option<u64>,
    inode: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SourceObservation {
    pub unchanged_observations: u32,
    pub needs_hash: bool,
}

/// Metadata bound to the SQLite-aware backup artifact set. The caller supplies
/// the digest of its reviewed source configuration because roots and guards are
/// deployment-owned rather than catalog rows.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackupMetadata {
    pub format_version: u32,
    pub created_at_ms: i64,
    pub schema_version: i64,
    pub latest_event_sequence: u64,
    pub source_configuration_digest: [u8; blake3::OUT_LEN],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackupArtifact {
    pub catalog_path: PathBuf,
    pub manifest_path: PathBuf,
    pub metadata: BackupMetadata,
}

impl SourceFingerprint {
    pub fn from_metadata(metadata: &Metadata) -> std::io::Result<Self> {
        if !metadata.is_file() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "source candidate is not a regular file",
            ));
        }
        let (modified_seconds, modified_nanoseconds) = unix_time_parts(metadata.modified()?);
        Ok(Self {
            byte_length: metadata.len(),
            modified_seconds,
            modified_nanoseconds,
            #[cfg(unix)]
            device: {
                use std::os::unix::fs::MetadataExt;
                Some(metadata.dev())
            },
            #[cfg(not(unix))]
            device: None,
            #[cfg(unix)]
            inode: {
                use std::os::unix::fs::MetadataExt;
                Some(metadata.ino())
            },
            #[cfg(not(unix))]
            inode: None,
        })
    }

    pub fn byte_length(&self) -> u64 {
        self.byte_length
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TombstoneGrace {
    min_complete_scans: u32,
    min_elapsed_ms: u64,
}

impl TombstoneGrace {
    pub fn new(min_complete_scans: u32, min_elapsed_ms: u64) -> Result<Self, CatalogError> {
        if min_complete_scans == 0 {
            return Err(CatalogError::InvalidTombstoneGrace(
                "min_complete_scans must be at least one".to_owned(),
            ));
        }
        Ok(Self {
            min_complete_scans,
            min_elapsed_ms,
        })
    }

    pub fn min_complete_scans(self) -> u32 {
        self.min_complete_scans
    }

    pub fn min_elapsed_ms(self) -> u64 {
        self.min_elapsed_ms
    }
}

impl Default for TombstoneGrace {
    fn default() -> Self {
        Self {
            min_complete_scans: 2,
            min_elapsed_ms: 0,
        }
    }
}

/// The durable V1.5 catalog. It is intentionally single-process and local-FS
/// oriented; deployment code is responsible for placing its database only on a
/// filesystem with the locking and `fsync` behavior required by the contract.
pub struct Catalog {
    pub(crate) connection: Connection,
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
    #[error("filesystem error: {0}")]
    Io(#[from] io::Error),
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("catalog schema version {found} is newer than this binary supports ({supported})")]
    NewerSchema { found: i64, supported: i64 },
    #[error("catalog data is corrupt: {0}")]
    Corrupt(String),
    #[error("system clock is before the Unix epoch")]
    ClockBeforeEpoch,
    #[error("invalid source registration: {0}")]
    InvalidSourceRegistration(String),
    #[error("source registration conflict: {0}")]
    SourceRegistrationConflict(String),
    #[error("invalid tombstone grace: {0}")]
    InvalidTombstoneGrace(String),
    #[error("source scan is stale or not active: {0}")]
    SourceScanConflict(String),
    #[error("invalid backup destination: {0}")]
    InvalidBackupDestination(String),
    #[error("backup destination already exists: {0}")]
    BackupDestinationExists(PathBuf),
    #[error("backup manifest is invalid: {0}")]
    BackupManifestInvalid(String),
    #[error("backup manifest does not match its SQLite artifact")]
    BackupManifestMismatch,
    #[error(
        "backup source configuration digest does not match the expected deployment configuration"
    )]
    BackupSourceConfigurationMismatch,
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

    pub fn latest_event_sequence(&self) -> Result<u64, CatalogError> {
        let sequence: i64 = self.connection.query_row(
            "SELECT COALESCE(MAX(sequence), 0) FROM events",
            [],
            |row| row.get(0),
        )?;
        as_u64(sequence, "latest event sequence")
    }

    pub fn source_scrub_progress(
        &self,
        source_id: &str,
    ) -> Result<SourceScrubProgress, CatalogError> {
        let registered: Option<i64> = self
            .connection
            .query_row(
                "SELECT 1 FROM source_state WHERE source_id = ?1",
                params![source_id],
                |row| row.get(0),
            )
            .optional()?;
        if registered.is_none() {
            return Err(CatalogError::SourceScanConflict(
                "source is not registered".to_owned(),
            ));
        }
        let row = self
            .connection
            .query_row(
                "SELECT cursor_root_role, cursor_source_relative_path,
                        completed_cycles, last_completed_at_ms
                 FROM source_scrub_state WHERE source_id = ?1",
                params![source_id],
                |row| {
                    Ok((
                        row.get::<_, Option<String>>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, Option<i64>>(3)?,
                    ))
                },
            )
            .optional()?;
        let Some((root_role, source_relative_path, completed_cycles, last_completed_at_ms)) = row
        else {
            return Ok(SourceScrubProgress {
                source_id: source_id.to_owned(),
                next_after: None,
                completed_cycles: 0,
                last_completed_at_ms: None,
            });
        };
        let next_after = match (root_role, source_relative_path) {
            (Some(root_role), Some(source_relative_path)) => {
                Some(LogicalLocation::new(root_role, source_relative_path)?)
            }
            (None, None) => None,
            _ => {
                return Err(CatalogError::Corrupt(
                    "incomplete source scrub cursor".to_owned(),
                ));
            }
        };
        Ok(SourceScrubProgress {
            source_id: source_id.to_owned(),
            next_after,
            completed_cycles: as_u64(completed_cycles, "source scrub completed cycles")?,
            last_completed_at_ms,
        })
    }

    pub(crate) fn verify_source_registration(
        &self,
        registration: &SourceRegistration,
    ) -> Result<(), CatalogError> {
        let existing = self
            .connection
            .query_row(
                "SELECT producer_id, identity_schema_name, identity_schema_version
                 FROM source_state WHERE source_id = ?1",
                params![registration.source_id()],
                |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| {
                CatalogError::SourceScanConflict("source is not registered".to_owned())
            })?;
        if existing.0 != registration.producer_id().as_uuid().as_bytes()
            || existing.1 != registration.identity_schema_name()
            || existing.2 != i64::from(registration.identity_schema_version())
        {
            return Err(CatalogError::SourceRegistrationConflict(
                registration.source_id().to_owned(),
            ));
        }
        Ok(())
    }

    /// Commits scrub progress only after one candidate has been fully observed
    /// or intentionally deferred for the next full cycle. A complete cycle
    /// clears the cursor and increments the durable cycle count.
    pub(crate) fn advance_source_scrub(
        &mut self,
        source_id: &str,
        next_after: Option<&LogicalLocation>,
        cycle_completed: bool,
        completed_at_ms: i64,
    ) -> Result<(), CatalogError> {
        if cycle_completed != next_after.is_none() {
            return Err(CatalogError::Corrupt(
                "source scrub completion must clear its cursor".to_owned(),
            ));
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let registered: Option<i64> = transaction
            .query_row(
                "SELECT 1 FROM source_state WHERE source_id = ?1",
                params![source_id],
                |row| row.get(0),
            )
            .optional()?;
        if registered.is_none() {
            return Err(CatalogError::SourceScanConflict(
                "source is not registered".to_owned(),
            ));
        }
        transaction.execute(
            "INSERT INTO source_scrub_state (
                source_id, cursor_root_role, cursor_source_relative_path,
                completed_cycles, last_completed_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(source_id) DO UPDATE SET
                cursor_root_role = excluded.cursor_root_role,
                cursor_source_relative_path = excluded.cursor_source_relative_path,
                completed_cycles = source_scrub_state.completed_cycles + ?4,
                last_completed_at_ms = CASE WHEN ?4 = 1
                    THEN excluded.last_completed_at_ms
                    ELSE source_scrub_state.last_completed_at_ms END",
            params![
                source_id,
                next_after.map(LogicalLocation::root_role),
                next_after.map(LogicalLocation::source_relative_path),
                i64::from(cycle_completed),
                if cycle_completed {
                    Some(completed_at_ms)
                } else {
                    None
                },
            ],
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Updates a record only when the scrub can prove it belongs to the named
    /// source. A scrub never discovers new identities or changes source scan
    /// presence; normal reconciliation remains responsible for those actions.
    pub(crate) fn observe_scrubbed_present_at(
        &mut self,
        source_id: &str,
        identity: &RecordIdentity,
        location: &LogicalLocation,
        revision: CanonicalRevision,
        committed_at_ms: i64,
    ) -> Result<Option<Vec<ArchiveEvent>>, CatalogError> {
        let record = find_record(&self.connection, identity)?;
        let Some(record) = record else {
            return Ok(None);
        };
        let owned: Option<i64> = self
            .connection
            .query_row(
                "SELECT 1 FROM source_records WHERE source_id = ?1 AND record_id = ?2",
                params![source_id, record.id.as_uuid().as_bytes()],
                |row| row.get(0),
            )
            .optional()?;
        if owned.is_none() {
            return Ok(None);
        }
        self.observe_present_at(identity, location, revision, committed_at_ms)
            .map(Some)
    }

    /// Creates a consistent SQLite backup and an adjacent manifest. The target
    /// paths must be new: a backup rotation chooses a new destination rather
    /// than overwriting the last known-good recovery set.
    pub fn backup_to(
        &self,
        destination: impl AsRef<Path>,
        source_configuration_digest: [u8; blake3::OUT_LEN],
        created_at_ms: i64,
    ) -> Result<BackupArtifact, CatalogError> {
        let destination = validate_backup_destination(destination.as_ref())?;
        let manifest_path = backup_manifest_path(&destination)?;
        if destination.exists() || manifest_path.exists() {
            return Err(CatalogError::BackupDestinationExists(destination));
        }
        let metadata = BackupMetadata {
            format_version: 1,
            created_at_ms,
            schema_version: self.schema_version()?,
            latest_event_sequence: self.latest_event_sequence()?,
            source_configuration_digest,
        };
        let temporary_catalog = temporary_backup_path(&destination, "catalog")?;
        let temporary_manifest = temporary_backup_path(&manifest_path, "manifest")?;
        let result = (|| -> Result<(), CatalogError> {
            self.connection.backup(MAIN_DB, &temporary_catalog, None)?;
            File::open(&temporary_catalog)?.sync_all()?;
            write_backup_manifest(&temporary_manifest, &metadata)?;
            fs::rename(&temporary_catalog, &destination)?;
            sync_parent_directory(&destination)?;
            fs::rename(&temporary_manifest, &manifest_path)?;
            sync_parent_directory(&manifest_path)?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary_catalog);
            let _ = fs::remove_file(&temporary_manifest);
        }
        result?;
        Ok(BackupArtifact {
            catalog_path: destination,
            manifest_path,
            metadata,
        })
    }

    /// Validates the adjacent manifest and its relation to the read-only
    /// SQLite artifact before an operator restores it.
    pub fn validate_backup(
        artifact: impl AsRef<Path>,
        expected_source_configuration_digest: [u8; blake3::OUT_LEN],
    ) -> Result<BackupArtifact, CatalogError> {
        let catalog_path = validate_backup_destination(artifact.as_ref())?;
        let manifest_path = backup_manifest_path(&catalog_path)?;
        let metadata = read_backup_manifest(&manifest_path)?;
        if metadata.source_configuration_digest != expected_source_configuration_digest {
            return Err(CatalogError::BackupSourceConfigurationMismatch);
        }
        let connection = Connection::open_with_flags(
            &catalog_path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        let found_schema: i64 = connection.query_row(
            "SELECT COALESCE(MAX(version), 0) FROM schema_migrations",
            [],
            |row| row.get(0),
        )?;
        let latest_event: i64 =
            connection.query_row("SELECT COALESCE(MAX(sequence), 0) FROM events", [], |row| {
                row.get(0)
            })?;
        if metadata.schema_version != found_schema
            || metadata.latest_event_sequence != as_u64(latest_event, "backup event sequence")?
        {
            return Err(CatalogError::BackupManifestMismatch);
        }
        Ok(BackupArtifact {
            catalog_path,
            manifest_path,
            metadata,
        })
    }

    /// Restores an already validated SQLite backup to a new control-database
    /// path using SQLite's backup API. It never overwrites a live destination.
    pub fn restore_backup_to(
        artifact: impl AsRef<Path>,
        destination: impl AsRef<Path>,
        expected_source_configuration_digest: [u8; blake3::OUT_LEN],
    ) -> Result<BackupArtifact, CatalogError> {
        let artifact = Self::validate_backup(artifact, expected_source_configuration_digest)?;
        let destination = validate_backup_destination(destination.as_ref())?;
        let manifest_path = backup_manifest_path(&destination)?;
        if destination.exists() || manifest_path.exists() {
            return Err(CatalogError::BackupDestinationExists(destination));
        }
        let temporary_catalog = temporary_backup_path(&destination, "restore")?;
        let temporary_manifest = temporary_backup_path(&manifest_path, "restore-manifest")?;
        let result = (|| -> Result<(), CatalogError> {
            let source = Connection::open_with_flags(
                &artifact.catalog_path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )?;
            source.backup(MAIN_DB, &temporary_catalog, None)?;
            File::open(&temporary_catalog)?.sync_all()?;
            write_backup_manifest(&temporary_manifest, &artifact.metadata)?;
            fs::rename(&temporary_catalog, &destination)?;
            sync_parent_directory(&destination)?;
            fs::rename(&temporary_manifest, &manifest_path)?;
            sync_parent_directory(&manifest_path)?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary_catalog);
            let _ = fs::remove_file(&temporary_manifest);
        }
        result?;
        Self::validate_backup(destination, expected_source_configuration_digest)
    }

    /// Begins a source generation after the filesystem adapter's initial root
    /// guard has passed. An unfinished generation can never cause tombstones.
    pub fn begin_source_scan(
        &mut self,
        registration: &SourceRegistration,
    ) -> Result<SourceScan, CatalogError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing = transaction
            .query_row(
                "SELECT producer_id, identity_schema_name, identity_schema_version, next_generation
                 FROM source_state WHERE source_id = ?1",
                params![registration.source_id()],
                |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .optional()?;
        let generation =
            if let Some((producer_id, schema_name, schema_version, next_generation)) = existing {
                if producer_id != registration.producer_id().as_uuid().as_bytes()
                    || schema_name != registration.identity_schema_name()
                    || schema_version != i64::from(registration.identity_schema_version())
                {
                    return Err(CatalogError::SourceRegistrationConflict(
                        registration.source_id().to_owned(),
                    ));
                }
                let generation = as_u64(next_generation, "source scan generation")?
                    .checked_add(1)
                    .ok_or_else(|| {
                        CatalogError::Corrupt("source scan generation overflow".to_owned())
                    })?;
                transaction.execute(
                    "UPDATE source_state SET next_generation = ?1 WHERE source_id = ?2",
                    params![
                        as_i64(generation, "source scan generation")?,
                        registration.source_id()
                    ],
                )?;
                generation
            } else {
                transaction.execute(
                    "INSERT INTO source_state (
                    source_id, producer_id, identity_schema_name, identity_schema_version,
                    next_generation, last_complete_generation
                 ) VALUES (?1, ?2, ?3, ?4, 1, 0)",
                    params![
                        registration.source_id(),
                        registration.producer_id().as_uuid().as_bytes(),
                        registration.identity_schema_name(),
                        i64::from(registration.identity_schema_version()),
                    ],
                )?;
                1
            };
        transaction.commit()?;
        Ok(SourceScan {
            source_id: registration.source_id().to_owned(),
            generation,
        })
    }

    /// Persists one cheap fingerprint and says whether this stable fingerprint
    /// still requires a full hash. An admitted hash is marked separately only
    /// after source/root verification allows its catalog commit.
    pub fn observe_source_fingerprint(
        &mut self,
        scan: &SourceScan,
        location: &LogicalLocation,
        fingerprint: &SourceFingerprint,
    ) -> Result<SourceObservation, CatalogError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        ensure_current_source_scan(&transaction, scan)?;
        let existing = transaction
            .query_row(
                "SELECT byte_length, modified_seconds, modified_nanoseconds, device, inode,
                        unchanged_observations, hash_admitted
                 FROM source_observations
                 WHERE source_id = ?1 AND root_role = ?2 AND source_relative_path = ?3",
                params![
                    scan.source_id(),
                    location.root_role(),
                    location.source_relative_path()
                ],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, Option<i64>>(3)?,
                        row.get::<_, Option<i64>>(4)?,
                        row.get::<_, i64>(5)?,
                        row.get::<_, i64>(6)?,
                    ))
                },
            )
            .optional()?;
        let (unchanged_observations, hash_admitted) = if let Some(existing) = existing {
            let matches = existing.0 == as_i64(fingerprint.byte_length, "source byte length")?
                && existing.1 == fingerprint.modified_seconds
                && existing.2 == i64::from(fingerprint.modified_nanoseconds)
                && existing.3 == optional_as_i64(fingerprint.device, "source device")?
                && existing.4 == optional_as_i64(fingerprint.inode, "source inode")?;
            if matches {
                (
                    as_u64(existing.5, "unchanged observations")?
                        .checked_add(1)
                        .ok_or_else(|| {
                            CatalogError::Corrupt("unchanged observation overflow".to_owned())
                        })?,
                    existing.6 != 0,
                )
            } else {
                (1, false)
            }
        } else {
            (1, false)
        };
        transaction.execute(
            "INSERT INTO source_observations (
                source_id, root_role, source_relative_path, byte_length,
                modified_seconds, modified_nanoseconds, device, inode,
                unchanged_observations, hash_admitted, last_seen_generation
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
             ON CONFLICT(source_id, root_role, source_relative_path) DO UPDATE SET
                 byte_length = excluded.byte_length,
                 modified_seconds = excluded.modified_seconds,
                 modified_nanoseconds = excluded.modified_nanoseconds,
                 device = excluded.device,
                 inode = excluded.inode,
                 unchanged_observations = excluded.unchanged_observations,
                 hash_admitted = CASE WHEN excluded.unchanged_observations = 1
                     THEN 0 ELSE source_observations.hash_admitted END,
                 last_seen_generation = excluded.last_seen_generation",
            params![
                scan.source_id(),
                location.root_role(),
                location.source_relative_path(),
                as_i64(fingerprint.byte_length, "source byte length")?,
                fingerprint.modified_seconds,
                i64::from(fingerprint.modified_nanoseconds),
                optional_as_i64(fingerprint.device, "source device")?,
                optional_as_i64(fingerprint.inode, "source inode")?,
                as_i64(unchanged_observations, "unchanged observations")?,
                i64::from(hash_admitted),
                as_i64(scan.generation(), "source scan generation")?,
            ],
        )?;
        transaction.commit()?;
        Ok(SourceObservation {
            unchanged_observations: u32::try_from(unchanged_observations).map_err(|_| {
                CatalogError::Corrupt("unchanged observations exceed u32".to_owned())
            })?,
            needs_hash: !hash_admitted,
        })
    }

    pub(crate) fn mark_source_fingerprint_hashed(
        &mut self,
        scan: &SourceScan,
        location: &LogicalLocation,
    ) -> Result<(), CatalogError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        ensure_current_source_scan(&transaction, scan)?;
        let changed = transaction.execute(
            "UPDATE source_observations
             SET hash_admitted = 1
             WHERE source_id = ?1 AND root_role = ?2 AND source_relative_path = ?3
               AND last_seen_generation = ?4",
            params![
                scan.source_id(),
                location.root_role(),
                location.source_relative_path(),
                as_i64(scan.generation(), "source scan generation")?,
            ],
        )?;
        if changed != 1 {
            return Err(CatalogError::SourceScanConflict(
                "source fingerprint disappeared before hash admission".to_owned(),
            ));
        }
        transaction.commit()?;
        Ok(())
    }

    /// Marks an already cataloged record as present in this scan. Calling it for
    /// a new-but-not-yet-stable record is intentionally a no-op.
    pub fn mark_source_record_seen(
        &mut self,
        scan: &SourceScan,
        registration: &SourceRegistration,
        identity: &RecordIdentity,
    ) -> Result<(), CatalogError> {
        if identity.producer_id() != registration.producer_id() {
            return Err(CatalogError::SourceRegistrationConflict(
                "record producer differs from source registration".to_owned(),
            ));
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        ensure_current_source_scan(&transaction, scan)?;
        let Some(record) = find_record(&transaction, identity)? else {
            transaction.commit()?;
            return Ok(());
        };
        bind_source_record(&transaction, scan, record.id)?;
        transaction.commit()?;
        Ok(())
    }

    pub fn observe_present_from_source(
        &mut self,
        scan: &SourceScan,
        registration: &SourceRegistration,
        identity: &RecordIdentity,
        location: &LogicalLocation,
        revision: CanonicalRevision,
    ) -> Result<Vec<ArchiveEvent>, CatalogError> {
        if identity.producer_id() != registration.producer_id() {
            return Err(CatalogError::SourceRegistrationConflict(
                "record producer differs from source registration".to_owned(),
            ));
        }
        self.ensure_current_source_scan(scan)?;
        let events = self.observe_present(identity, location, revision)?;
        self.mark_source_record_seen(scan, registration, identity)?;
        Ok(events)
    }

    /// Completes a full, guard-verified scan. Passing `None` records a complete
    /// scan and clears stale fingerprints, but never converts absence to a
    /// tombstone. `Some(grace)` is allowed only after the adapter has verified
    /// every configured root both before and after discovery/hashing.
    pub fn complete_source_scan(
        &mut self,
        scan: &SourceScan,
        tombstone_grace: Option<TombstoneGrace>,
        committed_at_ms: i64,
    ) -> Result<Vec<ArchiveEvent>, CatalogError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        ensure_current_source_scan(&transaction, scan)?;

        let mut events = Vec::new();
        if let Some(grace) = tombstone_grace {
            let mut statement = transaction.prepare(
                "SELECT record_id FROM source_records
                 WHERE source_id = ?1 AND last_seen_generation < ?2",
            )?;
            let missing_record_ids = statement
                .query_map(
                    params![
                        scan.source_id(),
                        as_i64(scan.generation(), "source scan generation")?
                    ],
                    |row| row.get::<_, Vec<u8>>(0),
                )?
                .collect::<Result<Vec<_>, _>>()?;
            drop(statement);

            for record_id in missing_record_ids {
                let record_id = record_id_from_blob(record_id)?;
                let missing = transaction
                    .query_row(
                        "SELECT first_missing_at_ms, complete_guarded_scans
                     FROM tombstone_candidates WHERE record_id = ?1",
                        params![record_id.as_uuid().as_bytes()],
                        |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
                    )
                    .optional()?;
                let (first_missing_at_ms, complete_guarded_scans) = if let Some((first, scans)) =
                    missing
                {
                    (
                        first,
                        as_u64(scans, "complete guarded scans")?
                            .checked_add(1)
                            .ok_or_else(|| {
                                CatalogError::Corrupt("tombstone scan count overflow".to_owned())
                            })?,
                    )
                } else {
                    (committed_at_ms, 1)
                };
                transaction.execute(
                    "INSERT INTO tombstone_candidates (
                        record_id, source_id, first_missing_at_ms, complete_guarded_scans
                     ) VALUES (?1, ?2, ?3, ?4)
                     ON CONFLICT(record_id) DO UPDATE SET
                         source_id = excluded.source_id,
                         complete_guarded_scans = excluded.complete_guarded_scans",
                    params![
                        record_id.as_uuid().as_bytes(),
                        scan.source_id(),
                        first_missing_at_ms,
                        as_i64(complete_guarded_scans, "complete guarded scans")?,
                    ],
                )?;
                let elapsed_ms =
                    u64::try_from(committed_at_ms.saturating_sub(first_missing_at_ms)).unwrap_or(0);
                if complete_guarded_scans >= u64::from(grace.min_complete_scans())
                    && elapsed_ms >= grace.min_elapsed_ms()
                {
                    let Some(mut record) = find_record_by_id(&transaction, record_id)? else {
                        return Err(CatalogError::Corrupt(
                            "source record disappeared from catalog".to_owned(),
                        ));
                    };
                    if record.state == RecordState::Active {
                        let revision_id = record.current_revision_id;
                        events.push(transition(
                            &transaction,
                            &mut record,
                            EventKind::RecordTombstoned,
                            revision_id,
                            None,
                            RecordState::Tombstoned,
                            committed_at_ms,
                        )?);
                    }
                    transaction.execute(
                        "DELETE FROM tombstone_candidates WHERE record_id = ?1",
                        params![record_id.as_uuid().as_bytes()],
                    )?;
                }
            }
        }

        transaction.execute(
            "UPDATE source_state SET last_complete_generation = ?1 WHERE source_id = ?2",
            params![
                as_i64(scan.generation(), "source scan generation")?,
                scan.source_id()
            ],
        )?;
        transaction.execute(
            "DELETE FROM source_observations
             WHERE source_id = ?1 AND last_seen_generation < ?2",
            params![
                scan.source_id(),
                as_i64(scan.generation(), "source scan generation")?
            ],
        )?;
        transaction.commit()?;
        Ok(events)
    }

    pub fn complete_source_scan_now(
        &mut self,
        scan: &SourceScan,
        tombstone_grace: Option<TombstoneGrace>,
    ) -> Result<Vec<ArchiveEvent>, CatalogError> {
        self.complete_source_scan(scan, tombstone_grace, now_ms()?)
    }

    fn ensure_current_source_scan(&self, scan: &SourceScan) -> Result<(), CatalogError> {
        ensure_current_source_scan(&self.connection, scan)
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
            "SELECT id, sequence, record_id, record_version, kind, revision_id,
                    root_role, source_relative_path, committed_at_ms
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

fn find_record_by_id(
    connection: &Connection,
    record_id: RecordId,
) -> Result<Option<RecordRow>, CatalogError> {
    let identity = connection
        .query_row(
            "SELECT producer_id, agent_namespace, session_id, record_kind, record_key
             FROM records WHERE id = ?1",
            params![record_id.as_uuid().as_bytes()],
            |row| {
                let producer_id = ProducerId::from_uuid(
                    uuid_from_blob(&row.get::<_, Vec<u8>>(0)?).map_err(to_sql_conversion_error)?,
                );
                RecordIdentity::new(
                    producer_id,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                )
                .map_err(|error| to_sql_conversion_error(error.into()))
            },
        )
        .optional()?;
    identity
        .map(|identity| find_record(connection, &identity))
        .transpose()
        .map(Option::flatten)
}

fn bind_source_record(
    transaction: &Transaction<'_>,
    scan: &SourceScan,
    record_id: RecordId,
) -> Result<(), CatalogError> {
    let existing_source = transaction
        .query_row(
            "SELECT source_id FROM source_records WHERE record_id = ?1",
            params![record_id.as_uuid().as_bytes()],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    if let Some(existing_source) = existing_source {
        if existing_source != scan.source_id() {
            return Err(CatalogError::SourceRegistrationConflict(format!(
                "record is already owned by source {existing_source:?}"
            )));
        }
    }
    transaction.execute(
        "INSERT INTO source_records (source_id, record_id, last_seen_generation)
         VALUES (?1, ?2, ?3)
         ON CONFLICT(record_id) DO UPDATE SET
             last_seen_generation = excluded.last_seen_generation",
        params![
            scan.source_id(),
            record_id.as_uuid().as_bytes(),
            as_i64(scan.generation(), "source scan generation")?,
        ],
    )?;
    transaction.execute(
        "DELETE FROM tombstone_candidates WHERE record_id = ?1",
        params![record_id.as_uuid().as_bytes()],
    )?;
    Ok(())
}

fn ensure_current_source_scan(
    connection: &Connection,
    scan: &SourceScan,
) -> Result<(), CatalogError> {
    let (next_generation, last_complete_generation) = connection
        .query_row(
            "SELECT next_generation, last_complete_generation
             FROM source_state WHERE source_id = ?1",
            params![scan.source_id()],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()?
        .ok_or_else(|| CatalogError::SourceScanConflict("source is not registered".to_owned()))?;
    let next_generation = as_u64(next_generation, "source scan generation")?;
    let last_complete_generation = as_u64(last_complete_generation, "source scan generation")?;
    if scan.generation() != next_generation || scan.generation() <= last_complete_generation {
        return Err(CatalogError::SourceScanConflict(format!(
            "{} generation {} is not current",
            scan.source_id(),
            scan.generation()
        )));
    }
    Ok(())
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
    let location = location.unwrap_or(&record.location).clone();
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
        "INSERT INTO events (
                id, record_id, record_version, kind, revision_id, root_role,
                source_relative_path, committed_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            event_id.as_uuid().as_bytes(),
            record.id.as_uuid().as_bytes(),
            as_i64(next_version, "record version")?,
            kind.as_db(),
            revision_id.map(|id| id.as_uuid().as_bytes().to_vec()),
            location.root_role(),
            location.source_relative_path(),
            committed_at_ms,
        ],
    )?;

    let sequence = as_u64(transaction.last_insert_rowid(), "event sequence")?;
    record.record_version = next_version;
    record.location = location.clone();
    record.state = state;
    record.current_revision_id = revision_id;
    let event = ArchiveEvent {
        id: event_id,
        sequence,
        record_id: record.id,
        record_version: next_version,
        kind,
        revision_id,
        location: Some(location),
        committed_at_ms,
    };
    crate::delivery::enqueue_event_for_active_subscriptions(transaction, &event)?;
    Ok(event)
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
    let root_role = row.get::<_, Option<String>>(6)?;
    let source_relative_path = row.get::<_, Option<String>>(7)?;
    let committed_at_ms = row.get::<_, i64>(8)?;
    let location = match (root_role, source_relative_path) {
        (Some(root_role), Some(source_relative_path)) => Some(
            LogicalLocation::new(root_role, source_relative_path)
                .map_err(|error| to_sql_conversion_error(error.into()))?,
        ),
        (None, None) => None,
        _ => {
            return Err(to_sql_conversion_error(CatalogError::Corrupt(
                "incomplete event location".to_owned(),
            )));
        }
    };

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
        location,
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
            params![1, now_ms()?],
        )?;
    }
    if found < 2 {
        transaction.execute_batch(
            "CREATE TABLE source_state (
                 source_id TEXT PRIMARY KEY NOT NULL,
                 producer_id BLOB NOT NULL CHECK(length(producer_id) = 16),
                 identity_schema_name TEXT NOT NULL,
                 identity_schema_version INTEGER NOT NULL CHECK(identity_schema_version > 0),
                 next_generation INTEGER NOT NULL CHECK(next_generation >= 0),
                 last_complete_generation INTEGER NOT NULL CHECK(last_complete_generation >= 0)
             );
             CREATE TABLE source_observations (
                 source_id TEXT NOT NULL REFERENCES source_state(source_id),
                 root_role TEXT NOT NULL,
                 source_relative_path TEXT NOT NULL,
                 byte_length INTEGER NOT NULL CHECK(byte_length >= 0),
                 modified_seconds INTEGER NOT NULL,
                 modified_nanoseconds INTEGER NOT NULL CHECK(modified_nanoseconds >= 0 AND modified_nanoseconds < 1000000000),
                 device INTEGER NULL,
                 inode INTEGER NULL,
                 unchanged_observations INTEGER NOT NULL CHECK(unchanged_observations > 0),
                 last_seen_generation INTEGER NOT NULL CHECK(last_seen_generation > 0),
                 PRIMARY KEY(source_id, root_role, source_relative_path)
             );
             CREATE TABLE source_records (
                 source_id TEXT NOT NULL REFERENCES source_state(source_id),
                 record_id BLOB NOT NULL UNIQUE REFERENCES records(id),
                 last_seen_generation INTEGER NOT NULL CHECK(last_seen_generation > 0),
                 PRIMARY KEY(source_id, record_id)
             );
             CREATE TABLE tombstone_candidates (
                 record_id BLOB PRIMARY KEY NOT NULL REFERENCES records(id),
                 source_id TEXT NOT NULL REFERENCES source_state(source_id),
                 first_missing_at_ms INTEGER NOT NULL,
                 complete_guarded_scans INTEGER NOT NULL CHECK(complete_guarded_scans > 0)
             );
             CREATE INDEX source_records_generation ON source_records(source_id, last_seen_generation);
             CREATE INDEX source_observations_generation ON source_observations(source_id, last_seen_generation);",
        )?;
        transaction.execute(
            "INSERT INTO schema_migrations (version, applied_at_ms) VALUES (?1, ?2)",
            params![2, now_ms()?],
        )?;
    }
    if found < 3 {
        transaction.execute_batch(
            "ALTER TABLE events ADD COLUMN root_role TEXT NULL;
             ALTER TABLE events ADD COLUMN source_relative_path TEXT NULL;",
        )?;
        transaction.execute(
            "INSERT INTO schema_migrations (version, applied_at_ms) VALUES (?1, ?2)",
            params![3, now_ms()?],
        )?;
    }
    if found < 4 {
        transaction.execute_batch(
            "CREATE TABLE subscriptions (
                 id BLOB PRIMARY KEY NOT NULL CHECK(length(id) = 16),
                 consumer_id TEXT NOT NULL,
                 mode TEXT NOT NULL CHECK(mode IN ('replay_events', 'rebuild_current')),
                 max_active_leases INTEGER NOT NULL CHECK(max_active_leases > 0),
                 accepts_moves INTEGER NOT NULL CHECK(accepts_moves IN (0, 1)),
                 accepts_tombstones INTEGER NOT NULL CHECK(accepts_tombstones IN (0, 1)),
                 created_at_ms INTEGER NOT NULL
             );
             CREATE TABLE deliveries (
                 id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                 subscription_id BLOB NOT NULL REFERENCES subscriptions(id),
                 event_id BLOB NOT NULL CHECK(length(event_id) = 16),
                 event_sequence INTEGER NULL,
                 record_id BLOB NOT NULL REFERENCES records(id),
                 record_version INTEGER NOT NULL CHECK(record_version > 0),
                 kind TEXT NOT NULL CHECK(kind IN ('revision_committed', 'location_changed', 'record_tombstoned', 'record_restored')),
                 revision_id BLOB NULL REFERENCES revisions(id),
                 root_role TEXT NULL,
                 source_relative_path TEXT NULL,
                 is_snapshot INTEGER NOT NULL CHECK(is_snapshot IN (0, 1)),
                 state TEXT NOT NULL CHECK(state IN ('queued', 'leased', 'blocked', 'acknowledged', 'superseded', 'ignored_by_policy', 'dead_lettered')),
                 attempts INTEGER NOT NULL CHECK(attempts >= 0),
                 lease_token BLOB NULL CHECK(lease_token IS NULL OR length(lease_token) = 16),
                 lease_expires_at_ms INTEGER NULL,
                 settled_at_ms INTEGER NULL,
                 settlement_reason TEXT NULL,
                 created_at_ms INTEGER NOT NULL,
                 UNIQUE(subscription_id, event_id)
             );
             CREATE INDEX deliveries_next_lease ON deliveries(subscription_id, state, event_sequence, id);
             CREATE INDEX deliveries_record_order ON deliveries(subscription_id, record_id, record_version);",
        )?;
        transaction.execute(
            "INSERT INTO schema_migrations (version, applied_at_ms) VALUES (?1, ?2)",
            params![4, now_ms()?],
        )?;
    }
    if found < 5 {
        transaction.execute_batch(
            "ALTER TABLE subscriptions
             ADD COLUMN enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0, 1));",
        )?;
        transaction.execute(
            "INSERT INTO schema_migrations (version, applied_at_ms) VALUES (?1, ?2)",
            params![5, now_ms()?],
        )?;
    }
    if found < 6 {
        transaction.execute_batch(
            "CREATE TABLE source_scrub_state (
                 source_id TEXT PRIMARY KEY NOT NULL REFERENCES source_state(source_id),
                 cursor_root_role TEXT NULL,
                 cursor_source_relative_path TEXT NULL,
                 completed_cycles INTEGER NOT NULL CHECK(completed_cycles >= 0),
                 last_completed_at_ms INTEGER NULL,
                 CHECK(
                     (cursor_root_role IS NULL AND cursor_source_relative_path IS NULL)
                     OR (cursor_root_role IS NOT NULL AND cursor_source_relative_path IS NOT NULL)
                 )
             );",
        )?;
        transaction.execute(
            "INSERT INTO schema_migrations (version, applied_at_ms) VALUES (?1, ?2)",
            params![6, now_ms()?],
        )?;
    }
    if found < 7 {
        transaction.execute_batch(
            "ALTER TABLE source_observations
             ADD COLUMN hash_admitted INTEGER NOT NULL DEFAULT 0 CHECK(hash_admitted IN (0, 1));",
        )?;
        transaction.execute(
            "INSERT INTO schema_migrations (version, applied_at_ms) VALUES (?1, ?2)",
            params![7, now_ms()?],
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

fn validate_backup_destination(path: &Path) -> Result<PathBuf, CatalogError> {
    if !path.is_absolute() {
        return Err(CatalogError::InvalidBackupDestination(
            "backup destination must be an absolute path".to_owned(),
        ));
    }
    let Some(parent) = path.parent() else {
        return Err(CatalogError::InvalidBackupDestination(
            "backup destination must have a parent directory".to_owned(),
        ));
    };
    let Some(filename) = path.file_name().and_then(|name| name.to_str()) else {
        return Err(CatalogError::InvalidBackupDestination(
            "backup destination filename must be UTF-8".to_owned(),
        ));
    };
    if filename.is_empty() {
        return Err(CatalogError::InvalidBackupDestination(
            "backup destination filename must not be empty".to_owned(),
        ));
    }
    let metadata = fs::symlink_metadata(parent)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(CatalogError::InvalidBackupDestination(
            "backup destination parent must be a real directory".to_owned(),
        ));
    }
    Ok(path.to_owned())
}

fn backup_manifest_path(catalog_path: &Path) -> Result<PathBuf, CatalogError> {
    let parent = catalog_path.parent().ok_or_else(|| {
        CatalogError::InvalidBackupDestination(
            "backup artifact must have a parent directory".to_owned(),
        )
    })?;
    let filename = catalog_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            CatalogError::InvalidBackupDestination(
                "backup artifact filename must be UTF-8".to_owned(),
            )
        })?;
    Ok(parent.join(format!("{filename}.metadata.json")))
}

fn temporary_backup_path(destination: &Path, label: &str) -> Result<PathBuf, CatalogError> {
    let parent = destination.parent().ok_or_else(|| {
        CatalogError::InvalidBackupDestination(
            "backup artifact must have a parent directory".to_owned(),
        )
    })?;
    let filename = destination
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            CatalogError::InvalidBackupDestination(
                "backup artifact filename must be UTF-8".to_owned(),
            )
        })?;
    Ok(parent.join(format!(".{filename}.{label}-{}.partial", Uuid::new_v4())))
}

fn write_backup_manifest(path: &Path, metadata: &BackupMetadata) -> Result<(), CatalogError> {
    let contents = format!(
        concat!(
            "{{\"format_version\":{},\"created_at_ms\":{},\"schema_version\":{},",
            "\"latest_event_sequence\":{},\"source_configuration_digest\":\"{}\"}}\n"
        ),
        metadata.format_version,
        metadata.created_at_ms,
        metadata.schema_version,
        metadata.latest_event_sequence,
        hex_encode(&metadata.source_configuration_digest),
    );
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(contents.as_bytes())?;
    file.sync_all()?;
    Ok(())
}

fn read_backup_manifest(path: &Path) -> Result<BackupMetadata, CatalogError> {
    let contents = fs::read(path)?;
    let value: serde_json::Value = serde_json::from_slice(&contents)
        .map_err(|error| CatalogError::BackupManifestInvalid(error.to_string()))?;
    let object = value.as_object().ok_or_else(|| {
        CatalogError::BackupManifestInvalid("manifest must be a JSON object".to_owned())
    })?;
    let format_version = object
        .get("format_version")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| {
            CatalogError::BackupManifestInvalid("manifest format_version is missing".to_owned())
        })?;
    let created_at_ms = object
        .get("created_at_ms")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| {
            CatalogError::BackupManifestInvalid("manifest created_at_ms is missing".to_owned())
        })?;
    let schema_version = object
        .get("schema_version")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| {
            CatalogError::BackupManifestInvalid("manifest schema_version is missing".to_owned())
        })?;
    let latest_event_sequence = object
        .get("latest_event_sequence")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| {
            CatalogError::BackupManifestInvalid(
                "manifest latest_event_sequence is missing".to_owned(),
            )
        })?;
    let source_configuration_digest = object
        .get("source_configuration_digest")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            CatalogError::BackupManifestInvalid(
                "manifest source_configuration_digest is missing".to_owned(),
            )
        })?;
    if format_version != 1 {
        return Err(CatalogError::BackupManifestInvalid(format!(
            "unsupported backup manifest version {format_version}"
        )));
    }
    Ok(BackupMetadata {
        format_version: u32::try_from(format_version).map_err(|_| {
            CatalogError::BackupManifestInvalid("manifest format_version exceeds u32".to_owned())
        })?,
        created_at_ms,
        schema_version,
        latest_event_sequence,
        source_configuration_digest: hex_decode_32(source_configuration_digest)?,
    })
}

fn sync_parent_directory(path: &Path) -> Result<(), CatalogError> {
    let parent = path.parent().ok_or_else(|| {
        CatalogError::InvalidBackupDestination(
            "backup artifact must have a parent directory".to_owned(),
        )
    })?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn hex_decode_32(value: &str) -> Result<[u8; blake3::OUT_LEN], CatalogError> {
    if value.len() != blake3::OUT_LEN * 2 {
        return Err(CatalogError::BackupManifestInvalid(
            "manifest digest has the wrong length".to_owned(),
        ));
    }
    let mut bytes = [0_u8; blake3::OUT_LEN];
    for (index, output) in bytes.iter_mut().enumerate() {
        let start = index * 2;
        *output = u8::from_str_radix(&value[start..start + 2], 16).map_err(|_| {
            CatalogError::BackupManifestInvalid("manifest digest is not hexadecimal".to_owned())
        })?;
    }
    Ok(bytes)
}

fn unix_time_parts(time: SystemTime) -> (i64, u32) {
    match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => (
            i64::try_from(duration.as_secs()).unwrap_or(i64::MAX),
            duration.subsec_nanos(),
        ),
        Err(error) => {
            let duration = error.duration();
            let seconds = i64::try_from(duration.as_secs()).unwrap_or(i64::MAX);
            if duration.subsec_nanos() == 0 {
                (-seconds, 0)
            } else {
                (
                    seconds.saturating_add(1).saturating_neg(),
                    1_000_000_000 - duration.subsec_nanos(),
                )
            }
        }
    }
}

fn valid_source_field(field: &str, value: String) -> Result<String, CatalogError> {
    if value.is_empty() || value.trim() != value || value.contains('\0') {
        return Err(CatalogError::InvalidSourceRegistration(format!(
            "{field} must be non-empty, trimmed, and free of NUL bytes"
        )));
    }
    Ok(value)
}

fn as_i64(value: u64, field: &'static str) -> Result<i64, CatalogError> {
    i64::try_from(value)
        .map_err(|_| CatalogError::Corrupt(format!("{field} exceeds SQLite INTEGER range")))
}

fn optional_as_i64(value: Option<u64>, field: &'static str) -> Result<Option<i64>, CatalogError> {
    value.map(|value| as_i64(value, field)).transpose()
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
    use super::{SourceRegistration, TombstoneGrace};
    use crate::delivery::{SubscriptionConfig, SubscriptionMode};
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
        assert_eq!(catalog.schema_version().unwrap(), 7);
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

    #[test]
    fn sqlite_aware_backup_and_restore_preserve_delivery_state() {
        let directory = tempdir().unwrap();
        let live_path = directory.path().join("live.sqlite3");
        let backup_path = directory.path().join("backup.sqlite3");
        let restored_path = directory.path().join("restored.sqlite3");
        let source_configuration_digest = [42_u8; blake3::OUT_LEN];
        let identity = identity();
        let revision = CanonicalRevision::from_bytes(b"durable backup\n");
        let subscription;

        {
            let mut catalog = Catalog::open(&live_path).unwrap();
            subscription = catalog
                .create_subscription(
                    SubscriptionConfig::new("backup-fixture", 1, true, true).unwrap(),
                    SubscriptionMode::ReplayEvents,
                    0,
                )
                .unwrap();
            catalog
                .observe_present_at(&identity, &location("sessions/a.jsonl"), revision, 100)
                .unwrap();
            catalog
                .lease_next(subscription.id, 110, 100)
                .unwrap()
                .unwrap();

            let artifact = catalog
                .backup_to(&backup_path, source_configuration_digest, 120)
                .unwrap();
            assert!(artifact.catalog_path.exists());
            assert!(artifact.manifest_path.exists());
            assert_eq!(artifact.metadata.latest_event_sequence, 1);
        }

        assert!(matches!(
            Catalog::validate_backup(&backup_path, [0_u8; blake3::OUT_LEN]),
            Err(super::CatalogError::BackupSourceConfigurationMismatch)
        ));
        let restored =
            Catalog::restore_backup_to(&backup_path, &restored_path, source_configuration_digest)
                .unwrap();
        assert_eq!(restored.catalog_path, restored_path);
        assert!(restored.manifest_path.exists());

        let restored_catalog = Catalog::open(&restored_path).unwrap();
        assert_eq!(restored_catalog.events_after(0).unwrap().len(), 1);
        assert_eq!(
            restored_catalog
                .current_record(&identity)
                .unwrap()
                .unwrap()
                .revision,
            Some(revision)
        );
        let counts = restored_catalog.delivery_counts(subscription.id).unwrap();
        assert_eq!(counts.leased, 1);
    }

    #[test]
    fn source_tombstone_requires_both_complete_scan_and_elapsed_grace() {
        let mut catalog = Catalog::open_in_memory().unwrap();
        let producer = ProducerId::new();
        let registration = SourceRegistration::new("source-a", producer, "fixture", 1).unwrap();
        let identity =
            RecordIdentity::new(producer, "codex", "session-a", "transcript", "primary").unwrap();
        let location = location("active/a.jsonl");
        let revision = CanonicalRevision::from_bytes(b"first\n");
        let grace = TombstoneGrace::new(2, 100).unwrap();

        let first = catalog.begin_source_scan(&registration).unwrap();
        catalog
            .observe_present_from_source(&first, &registration, &identity, &location, revision)
            .unwrap();
        assert!(
            catalog
                .complete_source_scan(&first, Some(grace), 100)
                .unwrap()
                .is_empty()
        );

        let second = catalog.begin_source_scan(&registration).unwrap();
        assert!(
            catalog
                .complete_source_scan(&second, Some(grace), 110)
                .unwrap()
                .is_empty()
        );
        let third = catalog.begin_source_scan(&registration).unwrap();
        assert!(
            catalog
                .complete_source_scan(&third, Some(grace), 199)
                .unwrap()
                .is_empty()
        );
        let fourth = catalog.begin_source_scan(&registration).unwrap();
        let tombstones = catalog
            .complete_source_scan(&fourth, Some(grace), 210)
            .unwrap();

        assert_eq!(tombstones.len(), 1);
        assert_eq!(tombstones[0].kind, EventKind::RecordTombstoned);
    }

    #[test]
    fn stale_source_scan_cannot_complete_after_a_newer_generation_starts() {
        let mut catalog = Catalog::open_in_memory().unwrap();
        let registration =
            SourceRegistration::new("source-a", ProducerId::new(), "fixture", 1).unwrap();
        let first = catalog.begin_source_scan(&registration).unwrap();
        let second = catalog.begin_source_scan(&registration).unwrap();

        assert!(matches!(
            catalog.complete_source_scan(&first, None, 100),
            Err(super::CatalogError::SourceScanConflict(_))
        ));
        assert!(
            catalog
                .complete_source_scan(&second, None, 100)
                .unwrap()
                .is_empty()
        );
    }
}
