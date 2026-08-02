//! Agent Bookkeeper's V1.5 core.
//!
//! This crate deliberately starts below transport and semantic-memory policy.
//! It provides the stable domain, canonical revision hashing, and durable event
//! ledger on which a guarded filesystem reconciler and consumer delivery layer
//! can be built.

pub mod admin;
pub mod catalog;
pub mod controller;
pub mod delivery;
pub mod domain;
pub mod payload;
pub mod source_fs;

pub use admin::{AdminError, CatalogStatus, SourceStatus, SubscriptionStatus, catalog_status};
pub use catalog::{
    BackupArtifact, BackupMetadata, Catalog, CatalogError, CurrentRecord, SourceFingerprint,
    SourceObservation, SourceRegistration, SourceScan, SourceScrubProgress, TombstoneGrace,
};
pub use controller::{
    ControlledDeliveryAttempt, ControlledDeliveryOutcome, ControlledRunLimits, ControlledRunReport,
    ControllerError, DeliveryRoots, PathConsumer, run_path_consumer,
};
pub use delivery::{
    DeliveryCounts, DeliveryError, DeliveryLease, DeliveryState, LeaseToken, Subscription,
    SubscriptionConfig, SubscriptionId, SubscriptionMode,
};
pub use domain::{
    ArchiveEvent, CanonicalRevision, DeliveryOutcome, DomainError, EventKind, LogicalLocation,
    ProducerId, RecordIdentity, RecordState,
};
pub use payload::{
    CurrentExternalRevision, MaterializationCache, MaterializationLimits, MaterializedLease,
    PayloadError, VerifiedReader,
};
pub use source_fs::{
    Blake3RevisionHasher, CodexRolloutLayout, DeletionMode, IdentitySchema, LayoutPlugin,
    ReconcileReport, Reconciler, RootGuard, ScrubReport, SourceConfig, SourceError, SourceRoot,
    StabilityPolicy,
};
