//! Agent Bookkeeper's V1.5 core.
//!
//! This crate deliberately starts below transport and semantic-memory policy.
//! It provides the stable domain, canonical revision hashing, and durable event
//! ledger on which a guarded filesystem reconciler and consumer delivery layer
//! can be built.

pub mod catalog;
pub mod domain;
pub mod source_fs;

pub use catalog::{Catalog, CatalogError, CurrentRecord};
pub use domain::{
    ArchiveEvent, CanonicalRevision, DeliveryOutcome, DomainError, EventKind, LogicalLocation,
    ProducerId, RecordIdentity, RecordState,
};
pub use source_fs::{
    Blake3RevisionHasher, CodexRolloutLayout, DeletionMode, IdentitySchema, LayoutPlugin,
    ReconcileReport, Reconciler, RootGuard, SourceConfig, SourceError, SourceRoot, StabilityPolicy,
};
