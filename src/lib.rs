//! Agent Bookkeeper's V1.5 core.
//!
//! This crate deliberately starts below transport and semantic-memory policy.
//! It provides the stable domain, canonical revision hashing, and durable event
//! ledger on which a guarded filesystem reconciler and consumer delivery layer
//! can be built.

pub mod catalog;
pub mod domain;

pub use catalog::{Catalog, CatalogError, CurrentRecord};
pub use domain::{
    ArchiveEvent, CanonicalRevision, DeliveryOutcome, DomainError, EventKind, LogicalLocation,
    ProducerId, RecordIdentity, RecordState,
};
