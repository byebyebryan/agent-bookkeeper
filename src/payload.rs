//! Verified access to raw revision bytes.
//!
//! V1.5 borrows current bytes from an operator-managed mirror. A consumer must
//! therefore receive a held descriptor, not a mutable pathname, and validate
//! that descriptor before acknowledging downstream work.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use thiserror::Error;

use crate::catalog::SourceFingerprint;
use crate::domain::{CanonicalRevision, LogicalLocation};
use crate::source_fs::{SourceError, open_file_beneath};

#[derive(Clone, Debug)]
pub struct CurrentExternalRevision {
    root: PathBuf,
    relative_path: PathBuf,
    expected: CanonicalRevision,
}

impl CurrentExternalRevision {
    pub fn new(
        root: impl Into<PathBuf>,
        relative_path: impl Into<PathBuf>,
        expected: CanonicalRevision,
    ) -> Result<Self, PayloadError> {
        let root = root.into();
        if !root.is_absolute() {
            return Err(PayloadError::InvalidConfiguration(
                "external revision root must be an absolute path".to_owned(),
            ));
        }
        let relative_path = relative_path.into();
        let location = relative_path.to_str().ok_or_else(|| {
            PayloadError::InvalidConfiguration("relative path is not UTF-8".to_owned())
        })?;
        LogicalLocation::new("payload", location).map_err(PayloadError::Domain)?;
        Ok(Self {
            root,
            relative_path,
            expected,
        })
    }

    pub fn expected(&self) -> CanonicalRevision {
        self.expected
    }

    pub fn open(&self) -> Result<VerifiedReader, PayloadError> {
        let file =
            open_file_beneath(&self.root, &self.relative_path).map_err(PayloadError::Source)?;
        let metadata = file.metadata().map_err(PayloadError::Read)?;
        if !metadata.is_file() {
            return Err(PayloadError::UnexpectedFileType);
        }
        if metadata.len() != self.expected.byte_length() {
            return Err(PayloadError::StaleRevision {
                reason: "opened descriptor length differs from the expected revision".to_owned(),
            });
        }
        let fingerprint =
            SourceFingerprint::from_metadata(&metadata).map_err(PayloadError::Read)?;
        Ok(VerifiedReader {
            file,
            expected: self.expected,
            initial_fingerprint: fingerprint,
            hasher: blake3::Hasher::new(),
            bytes_read: 0,
            finished: false,
        })
    }
}

/// Limits for an in-process cache of verified, lease-scoped materializations.
///
/// The cache is derived state. Its capacity limits are admission controls, not
/// archival retention: an entry disappears when its lease is released.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MaterializationLimits {
    max_entry_bytes: u64,
    max_active_entries: u32,
    max_active_bytes: u64,
}

impl MaterializationLimits {
    pub fn new(
        max_entry_bytes: u64,
        max_active_entries: u32,
        max_active_bytes: u64,
    ) -> Result<Self, PayloadError> {
        if max_entry_bytes == 0 || max_active_entries == 0 || max_active_bytes == 0 {
            return Err(PayloadError::InvalidConfiguration(
                "materialization limits must all be greater than zero".to_owned(),
            ));
        }
        if max_entry_bytes > max_active_bytes {
            return Err(PayloadError::InvalidConfiguration(
                "max_entry_bytes must not exceed max_active_bytes".to_owned(),
            ));
        }
        Ok(Self {
            max_entry_bytes,
            max_active_entries,
            max_active_bytes,
        })
    }

    pub fn max_entry_bytes(self) -> u64 {
        self.max_entry_bytes
    }

    pub fn max_active_entries(self) -> u32 {
        self.max_active_entries
    }

    pub fn max_active_bytes(self) -> u64 {
        self.max_active_bytes
    }
}

/// A Bookkeeper-owned directory for temporary, verified files needed by
/// adapters that cannot consume a stream or inherited file descriptor.
///
/// A cache instance is intended to be owned by one controller process. Each
/// successful materialization gets a distinct immutable path and its capacity
/// reservation lasts until the returned [`MaterializedLease`] is released or
/// dropped. The directory is derived state and must not be used as an archive
/// or as an input-discovery surface.
#[derive(Clone, Debug)]
pub struct MaterializationCache {
    root: PathBuf,
    limits: MaterializationLimits,
    state: Arc<Mutex<MaterializationState>>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct MaterializationState {
    active_entries: u32,
    active_bytes: u64,
}

impl MaterializationCache {
    pub fn new(
        root: impl Into<PathBuf>,
        limits: MaterializationLimits,
    ) -> Result<Self, PayloadError> {
        let root = root.into();
        if !root.is_absolute() {
            return Err(PayloadError::InvalidConfiguration(
                "materialization cache root must be an absolute path".to_owned(),
            ));
        }
        fs::create_dir_all(&root).map_err(PayloadError::CacheIo)?;
        let metadata = fs::symlink_metadata(&root).map_err(PayloadError::CacheIo)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(PayloadError::InvalidConfiguration(
                "materialization cache root must be a real directory".to_owned(),
            ));
        }
        Ok(Self {
            root,
            limits,
            state: Arc::new(Mutex::new(MaterializationState::default())),
        })
    }

    pub fn root(&self) -> &std::path::Path {
        &self.root
    }

    pub fn limits(&self) -> MaterializationLimits {
        self.limits
    }

    /// Copies one fully verified external revision into a unique read-only
    /// file. The returned path is never the mutable source path.
    pub fn materialize(
        &self,
        external: &CurrentExternalRevision,
    ) -> Result<MaterializedLease, PayloadError> {
        let expected = external.expected();
        self.reserve(expected.byte_length())?;

        let nonce = uuid::Uuid::new_v4();
        let stem = format!("lease-{}-{nonce}", expected.digest_hex());
        let temporary_path = self.root.join(format!(".{stem}.partial"));
        let final_path = self.root.join(format!("{stem}.payload"));
        let result = (|| -> Result<(), PayloadError> {
            let mut output = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary_path)
                .map_err(PayloadError::CacheIo)?;
            let mut input = external.open()?;
            let mut buffer = [0_u8; 128 * 1024];
            loop {
                let count = input.read(&mut buffer).map_err(PayloadError::Read)?;
                if count == 0 {
                    break;
                }
                use std::io::Write;
                output
                    .write_all(&buffer[..count])
                    .map_err(PayloadError::CacheIo)?;
            }
            input.finish()?;
            output.sync_all().map_err(PayloadError::CacheIo)?;
            drop(output);

            let mut permissions = fs::metadata(&temporary_path)
                .map_err(PayloadError::CacheIo)?
                .permissions();
            permissions.set_readonly(true);
            fs::set_permissions(&temporary_path, permissions).map_err(PayloadError::CacheIo)?;
            fs::rename(&temporary_path, &final_path).map_err(PayloadError::CacheIo)?;
            File::open(&self.root)
                .and_then(|directory| directory.sync_all())
                .map_err(PayloadError::CacheIo)?;
            Ok(())
        })();

        if let Err(error) = result {
            let _ = fs::remove_file(&temporary_path);
            let _ = fs::remove_file(&final_path);
            self.release_reservation(expected.byte_length());
            return Err(error);
        }

        Ok(MaterializedLease {
            path: final_path,
            expected,
            reserved_bytes: expected.byte_length(),
            state: Arc::clone(&self.state),
            released: false,
        })
    }

    fn reserve(&self, bytes: u64) -> Result<(), PayloadError> {
        if bytes > self.limits.max_entry_bytes {
            return Err(PayloadError::MaterializationLimit {
                requested_bytes: bytes,
                limit_bytes: self.limits.max_entry_bytes,
            });
        }
        let mut state = self.state.lock().map_err(|_| PayloadError::CachePoisoned)?;
        let next_entries = state
            .active_entries
            .checked_add(1)
            .ok_or(PayloadError::CacheCapacityExceeded)?;
        let next_bytes = state
            .active_bytes
            .checked_add(bytes)
            .ok_or(PayloadError::CacheCapacityExceeded)?;
        if next_entries > self.limits.max_active_entries
            || next_bytes > self.limits.max_active_bytes
        {
            return Err(PayloadError::CacheCapacityExceeded);
        }
        state.active_entries = next_entries;
        state.active_bytes = next_bytes;
        Ok(())
    }

    fn release_reservation(&self, bytes: u64) {
        release_reservation(&self.state, bytes);
    }
}

/// A unique verified file owned by one consumer-delivery lease.
#[derive(Debug)]
pub struct MaterializedLease {
    path: PathBuf,
    expected: CanonicalRevision,
    reserved_bytes: u64,
    state: Arc<Mutex<MaterializationState>>,
    released: bool,
}

impl MaterializedLease {
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    pub fn expected(&self) -> CanonicalRevision {
        self.expected
    }

    /// Removes the derived file and returns its capacity reservation. A failed
    /// removal intentionally keeps the reservation consumed in this process,
    /// so a cache accounting error cannot hide unbounded disk use.
    pub fn release(mut self) -> Result<(), PayloadError> {
        self.remove_and_release()
    }

    fn remove_and_release(&mut self) -> Result<(), PayloadError> {
        if self.released {
            return Ok(());
        }
        match fs::remove_file(&self.path) {
            Ok(()) => {
                release_reservation(&self.state, self.reserved_bytes);
                self.released = true;
                Ok(())
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                release_reservation(&self.state, self.reserved_bytes);
                self.released = true;
                Ok(())
            }
            Err(error) => Err(PayloadError::CacheIo(error)),
        }
    }
}

impl Drop for MaterializedLease {
    fn drop(&mut self) {
        let _ = self.remove_and_release();
    }
}

fn release_reservation(state: &Arc<Mutex<MaterializationState>>, bytes: u64) {
    if let Ok(mut state) = state.lock() {
        state.active_entries = state.active_entries.saturating_sub(1);
        state.active_bytes = state.active_bytes.saturating_sub(bytes);
    }
}

/// A reader over one held, no-follow descriptor. The consumer may stream its
/// contents normally, but must call [`VerifiedReader::finish`] before it treats
/// the payload as successfully delivered.
pub struct VerifiedReader {
    file: File,
    expected: CanonicalRevision,
    initial_fingerprint: SourceFingerprint,
    hasher: blake3::Hasher,
    bytes_read: u64,
    finished: bool,
}

impl VerifiedReader {
    pub fn finish(&mut self) -> Result<(), PayloadError> {
        if self.finished {
            return Ok(());
        }
        let mut buffer = [0_u8; 128 * 1024];
        while self.read(&mut buffer).map_err(PayloadError::Read)? != 0 {}

        let fingerprint =
            SourceFingerprint::from_metadata(&self.file.metadata().map_err(PayloadError::Read)?)
                .map_err(PayloadError::Read)?;
        if fingerprint != self.initial_fingerprint {
            return Err(PayloadError::StaleRevision {
                reason: "descriptor metadata changed while it was streamed".to_owned(),
            });
        }
        let actual =
            CanonicalRevision::from_parts(self.bytes_read, *self.hasher.finalize().as_bytes());
        if actual != self.expected {
            return Err(PayloadError::StaleRevision {
                reason: "descriptor bytes do not match the expected canonical revision".to_owned(),
            });
        }
        self.finished = true;
        Ok(())
    }

    pub fn is_finished(&self) -> bool {
        self.finished
    }
}

impl Read for VerifiedReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if self.finished {
            return Ok(0);
        }
        let count = self.file.read(buffer)?;
        if count > 0 {
            self.bytes_read = self
                .bytes_read
                .checked_add(count as u64)
                .ok_or_else(|| io::Error::other("revision byte count overflow"))?;
            self.hasher.update(&buffer[..count]);
        }
        Ok(count)
    }
}

#[derive(Debug, Error)]
pub enum PayloadError {
    #[error(transparent)]
    Domain(#[from] crate::domain::DomainError),
    #[error(transparent)]
    Source(#[from] SourceError),
    #[error("invalid external payload configuration: {0}")]
    InvalidConfiguration(String),
    #[error("failed to read external payload: {0}")]
    Read(#[source] io::Error),
    #[error("failed to manage materialization cache: {0}")]
    CacheIo(#[source] io::Error),
    #[error("materialization cache accounting lock was poisoned")]
    CachePoisoned,
    #[error(
        "revision of {requested_bytes} bytes exceeds the materialization per-entry limit of {limit_bytes} bytes"
    )]
    MaterializationLimit {
        requested_bytes: u64,
        limit_bytes: u64,
    },
    #[error("materialization cache has no remaining active entry or byte capacity")]
    CacheCapacityExceeded,
    #[error("external payload is not a regular file")]
    UnexpectedFileType,
    #[error("external payload no longer represents the expected revision: {reason}")]
    StaleRevision { reason: String },
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::{Read, Write};

    use tempfile::tempdir;

    use super::{
        CurrentExternalRevision, MaterializationCache, MaterializationLimits, PayloadError,
    };
    use crate::domain::CanonicalRevision;

    #[test]
    fn held_descriptor_survives_atomic_path_replacement() {
        let directory = tempdir().unwrap();
        let root = directory.path();
        let relative = "sessions/record.jsonl";
        let path = root.join(relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, b"original\n").unwrap();
        let expected = CanonicalRevision::from_bytes(b"original\n");
        let external = CurrentExternalRevision::new(root, relative, expected).unwrap();
        let mut reader = external.open().unwrap();

        let replacement = root.join("sessions/replacement.tmp");
        fs::write(&replacement, b"replacement\n").unwrap();
        fs::rename(replacement, &path).unwrap();
        let mut received = Vec::new();
        reader.read_to_end(&mut received).unwrap();

        assert_eq!(received, b"original\n");
        reader.finish().unwrap();
        assert!(reader.is_finished());
    }

    #[test]
    fn in_place_mutation_fails_finish_validation() {
        let directory = tempdir().unwrap();
        let root = directory.path();
        let relative = "sessions/record.jsonl";
        let path = root.join(relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, b"original\n").unwrap();
        let expected = CanonicalRevision::from_bytes(b"original\n");
        let external = CurrentExternalRevision::new(root, relative, expected).unwrap();
        let mut reader = external.open().unwrap();

        fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&path)
            .unwrap()
            .write_all(b"changed\n")
            .unwrap();

        assert!(matches!(
            reader.finish(),
            Err(PayloadError::StaleRevision { .. })
        ));
    }

    #[test]
    fn wrong_length_is_rejected_before_a_consumer_can_read() {
        let directory = tempdir().unwrap();
        let root = directory.path();
        let path = root.join("record.jsonl");
        fs::write(&path, b"actual\n").unwrap();
        let expected = CanonicalRevision::from_bytes(b"different bytes\n");
        let external = CurrentExternalRevision::new(root, "record.jsonl", expected).unwrap();

        assert!(matches!(
            external.open(),
            Err(PayloadError::StaleRevision { .. })
        ));
    }

    #[test]
    fn materialization_publishes_a_verified_lease_scoped_copy() {
        let directory = tempdir().unwrap();
        let source_root = directory.path().join("source");
        let cache_root = directory.path().join("cache");
        let source_path = source_root.join("sessions/record.jsonl");
        fs::create_dir_all(source_path.parent().unwrap()).unwrap();
        fs::write(&source_path, b"verified source bytes\n").unwrap();
        let expected = CanonicalRevision::from_bytes(b"verified source bytes\n");
        let external =
            CurrentExternalRevision::new(&source_root, "sessions/record.jsonl", expected).unwrap();
        let cache = MaterializationCache::new(
            &cache_root,
            MaterializationLimits::new(1024, 1, 1024).unwrap(),
        )
        .unwrap();

        let lease = cache.materialize(&external).unwrap();
        assert_ne!(lease.path(), source_path);
        assert_eq!(fs::read(lease.path()).unwrap(), b"verified source bytes\n");
        assert_eq!(lease.expected(), expected);
        let materialized_path = lease.path().to_owned();
        lease.release().unwrap();

        assert!(!materialized_path.exists());
    }

    #[test]
    fn materialization_enforces_active_capacity_and_releases_it_after_drop() {
        let directory = tempdir().unwrap();
        let source_root = directory.path().join("source");
        let cache_root = directory.path().join("cache");
        let source_path = source_root.join("record.jsonl");
        fs::create_dir_all(&source_root).unwrap();
        fs::write(&source_path, b"small\n").unwrap();
        let expected = CanonicalRevision::from_bytes(b"small\n");
        let external =
            CurrentExternalRevision::new(&source_root, "record.jsonl", expected).unwrap();
        let cache =
            MaterializationCache::new(&cache_root, MaterializationLimits::new(64, 1, 64).unwrap())
                .unwrap();

        let lease = cache.materialize(&external).unwrap();
        assert!(matches!(
            cache.materialize(&external),
            Err(PayloadError::CacheCapacityExceeded)
        ));
        drop(lease);

        cache.materialize(&external).unwrap().release().unwrap();
    }

    #[test]
    fn failed_materialization_returns_its_capacity_reservation() {
        let directory = tempdir().unwrap();
        let source_root = directory.path().join("source");
        let cache_root = directory.path().join("cache");
        let source_path = source_root.join("record.jsonl");
        fs::create_dir_all(&source_root).unwrap();
        fs::write(&source_path, b"original\n").unwrap();
        let expected = CanonicalRevision::from_bytes(b"original\n");
        let external =
            CurrentExternalRevision::new(&source_root, "record.jsonl", expected).unwrap();
        let cache =
            MaterializationCache::new(&cache_root, MaterializationLimits::new(64, 1, 64).unwrap())
                .unwrap();

        fs::write(&source_path, b"changed\n").unwrap();
        assert!(matches!(
            cache.materialize(&external),
            Err(PayloadError::StaleRevision { .. })
        ));

        fs::write(&source_path, b"original\n").unwrap();
        cache.materialize(&external).unwrap().release().unwrap();
    }
}
