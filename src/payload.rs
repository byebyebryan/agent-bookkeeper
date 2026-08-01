//! Verified access to raw revision bytes.
//!
//! V1.5 borrows current bytes from an operator-managed mirror. A consumer must
//! therefore receive a held descriptor, not a mutable pathname, and validate
//! that descriptor before acknowledging downstream work.

use std::fs::File;
use std::io::{self, Read};
use std::path::PathBuf;

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

    use super::{CurrentExternalRevision, PayloadError};
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
}
