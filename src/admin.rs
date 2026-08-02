//! Content-free operational status for the V1.5 controller.

use rusqlite::params;
use thiserror::Error;
use uuid::Uuid;

use crate::catalog::{Catalog, CatalogError};
use crate::delivery::{DeliveryCounts, DeliveryError, SubscriptionId, SubscriptionMode};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogStatus {
    pub schema_version: i64,
    pub latest_event_sequence: u64,
    pub active_records: u64,
    pub tombstoned_records: u64,
    pub revisions: u64,
    pub tombstone_candidates: u64,
    pub sources: Vec<SourceStatus>,
    pub subscriptions: Vec<SubscriptionStatus>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceStatus {
    pub source_id: String,
    pub next_generation: u64,
    pub last_complete_generation: u64,
    pub tracked_records: u64,
    pub fingerprint_observations: u64,
    pub tombstone_candidates: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubscriptionStatus {
    pub id: SubscriptionId,
    pub consumer_id: String,
    pub mode: SubscriptionMode,
    pub enabled: bool,
    pub deliveries: DeliveryCounts,
    pub oldest_unresolved_age_ms: Option<u64>,
    pub last_acknowledged_at_ms: Option<i64>,
}

/// Reads catalog health without opening a transcript or mutable source file.
/// `now_ms` is supplied by the caller so the status output is reproducible in
/// controlled runs and does not give a wall-clock value any authority.
pub fn catalog_status(catalog: &Catalog, now_ms: i64) -> Result<CatalogStatus, AdminError> {
    let schema_version = catalog.schema_version()?;
    let latest_event_sequence = catalog.latest_event_sequence()?;
    let active_records = count(
        catalog,
        "SELECT COUNT(*) FROM records WHERE state = 'active'",
    )?;
    let tombstoned_records = count(
        catalog,
        "SELECT COUNT(*) FROM records WHERE state = 'tombstoned'",
    )?;
    let revisions = count(catalog, "SELECT COUNT(*) FROM revisions")?;
    let tombstone_candidates = count(catalog, "SELECT COUNT(*) FROM tombstone_candidates")?;

    let mut source_statement = catalog.connection.prepare(
        "SELECT s.source_id, s.next_generation, s.last_complete_generation,
                (SELECT COUNT(*) FROM source_records AS r WHERE r.source_id = s.source_id),
                (SELECT COUNT(*) FROM source_observations AS o WHERE o.source_id = s.source_id),
                (SELECT COUNT(*) FROM tombstone_candidates AS t WHERE t.source_id = s.source_id)
         FROM source_state AS s
         ORDER BY s.source_id ASC",
    )?;
    let sources = source_statement
        .query_map([], |row| {
            Ok(SourceStatus {
                source_id: row.get(0)?,
                next_generation: as_u64(row.get::<_, i64>(1)?, "next source generation")
                    .map_err(to_sql_error)?,
                last_complete_generation: as_u64(
                    row.get::<_, i64>(2)?,
                    "last complete source generation",
                )
                .map_err(to_sql_error)?,
                tracked_records: as_u64(row.get::<_, i64>(3)?, "tracked records")
                    .map_err(to_sql_error)?,
                fingerprint_observations: as_u64(row.get::<_, i64>(4)?, "fingerprint observations")
                    .map_err(to_sql_error)?,
                tombstone_candidates: as_u64(row.get::<_, i64>(5)?, "source tombstone candidates")
                    .map_err(to_sql_error)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    drop(source_statement);

    let mut subscription_statement = catalog.connection.prepare(
        "SELECT id, consumer_id, mode, enabled FROM subscriptions ORDER BY created_at_ms ASC, id ASC",
    )?;
    let subscription_rows = subscription_statement
        .query_map([], |row| {
            let id = uuid_from_blob(&row.get::<_, Vec<u8>>(0)?).map_err(to_sql_error)?;
            let enabled = row.get::<_, i64>(3)?;
            let enabled = match enabled {
                0 => false,
                1 => true,
                _ => {
                    return Err(to_sql_error(AdminError::Corrupt(
                        "invalid enabled value".to_owned(),
                    )));
                }
            };
            Ok((
                SubscriptionId::from_uuid(id),
                row.get::<_, String>(1)?,
                SubscriptionMode::from_db(&row.get::<_, String>(2)?).map_err(to_sql_error)?,
                enabled,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    drop(subscription_statement);

    let mut subscriptions = Vec::with_capacity(subscription_rows.len());
    for (id, consumer_id, mode, enabled) in subscription_rows {
        let deliveries = catalog.delivery_counts(id)?;
        let oldest_unresolved_created_at_ms: Option<i64> = catalog.connection.query_row(
            "SELECT MIN(created_at_ms) FROM deliveries
             WHERE subscription_id = ?1
               AND state IN ('queued', 'leased', 'blocked')",
            params![id.as_uuid().as_bytes()],
            |row| row.get(0),
        )?;
        let last_acknowledged_at_ms: Option<i64> = catalog.connection.query_row(
            "SELECT MAX(settled_at_ms) FROM deliveries
             WHERE subscription_id = ?1 AND state = 'acknowledged'",
            params![id.as_uuid().as_bytes()],
            |row| row.get(0),
        )?;
        subscriptions.push(SubscriptionStatus {
            id,
            consumer_id,
            mode,
            enabled,
            deliveries,
            oldest_unresolved_age_ms: oldest_unresolved_created_at_ms
                .map(|created_at_ms| now_ms.saturating_sub(created_at_ms) as u64),
            last_acknowledged_at_ms,
        });
    }

    Ok(CatalogStatus {
        schema_version,
        latest_event_sequence,
        active_records,
        tombstoned_records,
        revisions,
        tombstone_candidates,
        sources,
        subscriptions,
    })
}

fn count(catalog: &Catalog, sql: &str) -> Result<u64, AdminError> {
    let count: i64 = catalog.connection.query_row(sql, [], |row| row.get(0))?;
    as_u64(count, "count")
}

fn as_u64(value: i64, field: &str) -> Result<u64, AdminError> {
    u64::try_from(value).map_err(|_| AdminError::Corrupt(format!("negative {field}")))
}

fn uuid_from_blob(value: &[u8]) -> Result<Uuid, AdminError> {
    Uuid::from_slice(value).map_err(|_| AdminError::Corrupt("invalid UUID blob".to_owned()))
}

fn to_sql_error(error: impl std::error::Error + Send + Sync + 'static) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Blob, Box::new(error))
}

#[derive(Debug, Error)]
pub enum AdminError {
    #[error(transparent)]
    Catalog(#[from] CatalogError),
    #[error(transparent)]
    Delivery(#[from] DeliveryError),
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("catalog status is corrupt: {0}")]
    Corrupt(String),
}

#[cfg(test)]
mod tests {
    use super::catalog_status;
    use crate::catalog::{Catalog, SourceRegistration};
    use crate::delivery::{SubscriptionConfig, SubscriptionMode};
    use crate::domain::{CanonicalRevision, LogicalLocation, ProducerId, RecordIdentity};

    #[test]
    fn status_reports_source_progress_and_independent_subscription_backlog() {
        let producer = ProducerId::new();
        let identity =
            RecordIdentity::new(producer, "codex", "session-a", "transcript", "primary").unwrap();
        let mut catalog = Catalog::open_in_memory().unwrap();
        let registration = SourceRegistration::new("fixture", producer, "fixture", 1).unwrap();
        let scan = catalog.begin_source_scan(&registration).unwrap();
        let subscription = catalog
            .create_subscription(
                SubscriptionConfig::new("status-fixture", 1, true, true).unwrap(),
                SubscriptionMode::ReplayEvents,
                0,
            )
            .unwrap();
        catalog
            .observe_present_at(
                &identity,
                &LogicalLocation::new("active", "sessions/a.jsonl").unwrap(),
                CanonicalRevision::from_bytes(b"status\n"),
                10,
            )
            .unwrap();
        catalog
            .set_subscription_enabled(subscription.id, false)
            .unwrap();

        let status = catalog_status(&catalog, 50).unwrap();

        assert_eq!(status.latest_event_sequence, 1);
        assert_eq!(status.active_records, 1);
        assert_eq!(status.sources.len(), 1);
        assert_eq!(status.sources[0].next_generation, scan.generation());
        assert_eq!(status.subscriptions.len(), 1);
        assert!(!status.subscriptions[0].enabled);
        assert_eq!(status.subscriptions[0].deliveries.queued, 1);
        assert_eq!(status.subscriptions[0].oldest_unresolved_age_ms, Some(40));
    }
}
