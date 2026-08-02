//! Durable subscription delivery over the archive event ledger.

use std::fmt;

use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};
use thiserror::Error;
use uuid::Uuid;

use crate::catalog::{Catalog, CatalogError};
use crate::domain::{
    ArchiveEvent, CanonicalRevision, DeliveryOutcome, EventId, EventKind, LogicalLocation,
    RecordId, RevisionId,
};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SubscriptionId(Uuid);

impl SubscriptionId {
    fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub(crate) fn from_uuid(value: Uuid) -> Self {
        Self(value)
    }

    pub fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl fmt::Display for SubscriptionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct LeaseToken(Uuid);

impl LeaseToken {
    fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn as_uuid(self) -> Uuid {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubscriptionMode {
    ReplayEvents,
    RebuildCurrent,
}

impl SubscriptionMode {
    fn as_db(self) -> &'static str {
        match self {
            Self::ReplayEvents => "replay_events",
            Self::RebuildCurrent => "rebuild_current",
        }
    }

    pub(crate) fn from_db(value: &str) -> Result<Self, DeliveryError> {
        match value {
            "replay_events" => Ok(Self::ReplayEvents),
            "rebuild_current" => Ok(Self::RebuildCurrent),
            _ => Err(DeliveryError::Corrupt(format!(
                "unknown subscription mode {value:?}"
            ))),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubscriptionConfig {
    consumer_id: String,
    max_active_leases: u32,
    accepts_moves: bool,
    accepts_tombstones: bool,
    replay_after_sequence: u64,
    retry_policy: RetryPolicy,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetryPolicy {
    max_attempts: u32,
    initial_backoff_ms: u64,
    max_backoff_ms: u64,
}

impl RetryPolicy {
    pub fn new(
        max_attempts: u32,
        initial_backoff_ms: u64,
        max_backoff_ms: u64,
    ) -> Result<Self, DeliveryError> {
        if max_attempts == 0 || max_backoff_ms < initial_backoff_ms {
            return Err(DeliveryError::InvalidSubscription(
                "retry policy requires positive attempts and max_backoff_ms >= initial_backoff_ms"
                    .to_owned(),
            ));
        }
        Ok(Self {
            max_attempts,
            initial_backoff_ms,
            max_backoff_ms,
        })
    }

    pub fn max_attempts(self) -> u32 {
        self.max_attempts
    }
    pub fn initial_backoff_ms(self) -> u64 {
        self.initial_backoff_ms
    }
    pub fn max_backoff_ms(self) -> u64 {
        self.max_backoff_ms
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 8,
            initial_backoff_ms: 1_000,
            max_backoff_ms: 60_000,
        }
    }
}

impl SubscriptionConfig {
    pub fn new(
        consumer_id: impl Into<String>,
        max_active_leases: u32,
        accepts_moves: bool,
        accepts_tombstones: bool,
    ) -> Result<Self, DeliveryError> {
        let consumer_id = consumer_id.into();
        if consumer_id.is_empty() || consumer_id.trim() != consumer_id || consumer_id.contains('\0')
        {
            return Err(DeliveryError::InvalidSubscription(
                "consumer_id must be non-empty, trimmed, and free of NUL bytes".to_owned(),
            ));
        }
        if max_active_leases == 0 {
            return Err(DeliveryError::InvalidSubscription(
                "max_active_leases must be at least one".to_owned(),
            ));
        }
        Ok(Self {
            consumer_id,
            max_active_leases,
            accepts_moves,
            accepts_tombstones,
            replay_after_sequence: 0,
            retry_policy: RetryPolicy::default(),
        })
    }

    /// Limits an initial `replay_events` epoch to events strictly after this
    /// durable archive sequence. Live events are still appended normally.
    pub fn with_replay_after_sequence(mut self, sequence: u64) -> Self {
        self.replay_after_sequence = sequence;
        self
    }

    pub fn replay_after_sequence(&self) -> u64 {
        self.replay_after_sequence
    }

    pub fn with_retry_policy(mut self, retry_policy: RetryPolicy) -> Self {
        self.retry_policy = retry_policy;
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Subscription {
    pub id: SubscriptionId,
    pub consumer_id: String,
    pub mode: SubscriptionMode,
    pub replay_after_sequence: u64,
    pub enabled: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeliveryState {
    Queued,
    Leased,
    Blocked,
    Acknowledged,
    Superseded,
    IgnoredByPolicy,
    DeadLettered,
}

impl DeliveryState {
    fn as_db(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Leased => "leased",
            Self::Blocked => "blocked",
            Self::Acknowledged => "acknowledged",
            Self::Superseded => "superseded",
            Self::IgnoredByPolicy => "ignored_by_policy",
            Self::DeadLettered => "dead_lettered",
        }
    }

    pub(crate) fn from_db(value: &str) -> Result<Self, DeliveryError> {
        match value {
            "queued" => Ok(Self::Queued),
            "leased" => Ok(Self::Leased),
            "blocked" => Ok(Self::Blocked),
            "acknowledged" => Ok(Self::Acknowledged),
            "superseded" => Ok(Self::Superseded),
            "ignored_by_policy" => Ok(Self::IgnoredByPolicy),
            "dead_lettered" => Ok(Self::DeadLettered),
            _ => Err(DeliveryError::Corrupt(format!(
                "unknown delivery state {value:?}"
            ))),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeliveryLease {
    pub delivery_id: u64,
    pub subscription_id: SubscriptionId,
    pub event_id: EventId,
    pub event_sequence: Option<u64>,
    pub record_id: RecordId,
    pub record_version: u64,
    pub kind: EventKind,
    pub revision_id: Option<RevisionId>,
    pub revision: Option<CanonicalRevision>,
    pub location: Option<LogicalLocation>,
    pub is_snapshot: bool,
    pub attempt: u32,
    pub token: LeaseToken,
    pub expires_at_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeliveryCounts {
    pub queued: u64,
    pub leased: u64,
    pub blocked: u64,
    pub acknowledged: u64,
    pub superseded: u64,
    pub ignored_by_policy: u64,
    pub dead_lettered: u64,
}

impl Catalog {
    pub fn create_subscription(
        &mut self,
        config: SubscriptionConfig,
        mode: SubscriptionMode,
        created_at_ms: i64,
    ) -> Result<Subscription, DeliveryError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let subscription = Subscription {
            id: SubscriptionId::new(),
            consumer_id: config.consumer_id.clone(),
            mode,
            replay_after_sequence: config.replay_after_sequence,
            enabled: true,
        };
        transaction.execute(
            "INSERT INTO subscriptions (
                id, consumer_id, mode, max_active_leases, accepts_moves,
                accepts_tombstones, created_at_ms, enabled, max_attempts,
                initial_backoff_ms, max_backoff_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 1, ?8, ?9, ?10)",
            params![
                subscription.id.as_uuid().as_bytes(),
                subscription.consumer_id,
                mode.as_db(),
                i64::from(config.max_active_leases),
                i64::from(config.accepts_moves),
                i64::from(config.accepts_tombstones),
                created_at_ms,
                i64::from(config.retry_policy.max_attempts),
                as_i64(
                    config.retry_policy.initial_backoff_ms,
                    "initial retry backoff"
                )?,
                as_i64(config.retry_policy.max_backoff_ms, "maximum retry backoff")?,
            ],
        )?;

        match mode {
            SubscriptionMode::ReplayEvents => {
                let mut statement = transaction.prepare(
                    "SELECT id, sequence, record_id, record_version, kind, revision_id,
                            root_role, source_relative_path, committed_at_ms
                     FROM events WHERE sequence > ?1 ORDER BY sequence ASC",
                )?;
                let events = statement
                    .query_map(
                        params![as_i64(
                            config.replay_after_sequence,
                            "replay event sequence"
                        )?],
                        event_from_row,
                    )?
                    .collect::<Result<Vec<_>, _>>()?;
                drop(statement);
                for event in events {
                    enqueue_delivery(&transaction, &subscription.id, &config, &event, false)?;
                }
            }
            SubscriptionMode::RebuildCurrent => {
                let mut statement = transaction.prepare(
                    "SELECT r.id, r.record_version, r.current_revision_id,
                            r.root_role, r.source_relative_path
                     FROM records AS r
                     WHERE r.state = 'active'
                     ORDER BY r.id ASC",
                )?;
                let rows = statement
                    .query_map([], |row| {
                        let record_id =
                            RecordId::from_uuid(uuid_from_blob(&row.get::<_, Vec<u8>>(0)?)?);
                        let record_version = as_u64(row.get::<_, i64>(1)?, "record version")?;
                        let revision_id = row
                            .get::<_, Option<Vec<u8>>>(2)?
                            .map(|value| uuid_from_blob(&value).map(RevisionId::from_uuid))
                            .transpose()?;
                        let location = LogicalLocation::new(
                            row.get::<_, String>(3)?,
                            row.get::<_, String>(4)?,
                        )
                        .map_err(domain_to_sql_error)?;
                        Ok(DeliverySnapshot {
                            record_id,
                            record_version,
                            revision_id,
                            location,
                        })
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                drop(statement);
                for snapshot in rows {
                    let event = ArchiveEvent {
                        id: EventId::new(),
                        sequence: 0,
                        record_id: snapshot.record_id,
                        record_version: snapshot.record_version,
                        kind: EventKind::RevisionCommitted,
                        revision_id: snapshot.revision_id,
                        location: Some(snapshot.location),
                        committed_at_ms: created_at_ms,
                    };
                    enqueue_delivery(&transaction, &subscription.id, &config, &event, true)?;
                }
            }
        }
        transaction.commit()?;
        Ok(subscription)
    }

    pub fn lease_next(
        &mut self,
        subscription_id: SubscriptionId,
        now_ms: i64,
        lease_duration_ms: u64,
    ) -> Result<Option<DeliveryLease>, DeliveryError> {
        if lease_duration_ms == 0 {
            return Err(DeliveryError::InvalidLeaseDuration);
        }
        let expires_at_ms = now_ms
            .checked_add(
                i64::try_from(lease_duration_ms)
                    .map_err(|_| DeliveryError::InvalidLeaseDuration)?,
            )
            .ok_or(DeliveryError::InvalidLeaseDuration)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let admission = subscription_admission(&transaction, subscription_id)?;
        if !admission.enabled {
            transaction.commit()?;
            return Ok(None);
        }
        let mut expired_statement = transaction.prepare(
            "SELECT id, attempts FROM deliveries
             WHERE subscription_id = ?1 AND state = 'leased' AND lease_expires_at_ms <= ?2",
        )?;
        let expired = expired_statement
            .query_map(
                params![subscription_id.as_uuid().as_bytes(), now_ms],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )?
            .collect::<Result<Vec<_>, _>>()?;
        drop(expired_statement);
        for (delivery_id, attempts) in expired {
            let attempts = u32::try_from(attempts)
                .map_err(|_| DeliveryError::Corrupt("invalid delivery attempts".to_owned()))?;
            let not_before_ms = retry_not_before_ms(&admission.retry_policy, attempts, now_ms)?;
            transaction.execute(
                "UPDATE deliveries
                 SET state = CASE WHEN attempts >= ?1 THEN 'dead_lettered' ELSE 'queued' END,
                     lease_token = NULL, lease_expires_at_ms = NULL,
                     not_before_ms = CASE WHEN attempts >= ?1 THEN not_before_ms ELSE ?2 END,
                     settled_at_ms = CASE WHEN attempts >= ?1 THEN ?3 ELSE NULL END,
                     settlement_reason = CASE WHEN attempts >= ?1
                         THEN 'retry policy exhausted after lease expiry' ELSE NULL END
                 WHERE id = ?4 AND state = 'leased' AND lease_expires_at_ms <= ?3",
                params![
                    i64::from(admission.retry_policy.max_attempts),
                    not_before_ms,
                    now_ms,
                    delivery_id,
                ],
            )?;
        }
        let active_leases: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM deliveries WHERE subscription_id = ?1 AND state = 'leased'",
            params![subscription_id.as_uuid().as_bytes()],
            |row| row.get(0),
        )?;
        if active_leases >= i64::from(admission.max_active_leases) {
            transaction.commit()?;
            return Ok(None);
        }
        let candidate = transaction
            .query_row(
                "SELECT d.id, d.event_id, d.event_sequence, d.record_id, d.record_version,
                        d.kind, d.revision_id, d.root_role, d.source_relative_path,
                        d.is_snapshot, d.attempts, rv.byte_length, rv.digest
                 FROM deliveries AS d
                 LEFT JOIN revisions AS rv ON rv.id = d.revision_id
                 WHERE d.subscription_id = ?1
                   AND d.state = 'queued' AND d.not_before_ms <= ?2
                   AND NOT EXISTS (
                       SELECT 1 FROM deliveries AS prior
                       WHERE prior.subscription_id = d.subscription_id
                         AND prior.record_id = d.record_id
                         AND prior.record_version < d.record_version
                         AND prior.state NOT IN ('acknowledged', 'superseded', 'ignored_by_policy')
                   )
                 ORDER BY CASE WHEN d.event_sequence IS NULL THEN 1 ELSE 0 END,
                          d.event_sequence ASC, d.id ASC
                 LIMIT 1",
                params![subscription_id.as_uuid().as_bytes(), now_ms],
                delivery_candidate_from_row,
            )
            .optional()?;
        let Some(candidate) = candidate else {
            transaction.commit()?;
            return Ok(None);
        };
        let token = LeaseToken::new();
        let attempt = candidate
            .attempts
            .checked_add(1)
            .ok_or_else(|| DeliveryError::Corrupt("delivery attempt overflow".to_owned()))?;
        transaction.execute(
            "UPDATE deliveries
             SET state = 'leased', attempts = ?1, lease_token = ?2, lease_expires_at_ms = ?3
             WHERE id = ?4 AND state = 'queued'",
            params![
                i64::from(attempt),
                token.as_uuid().as_bytes(),
                expires_at_ms,
                as_i64(candidate.delivery_id, "delivery ID")?,
            ],
        )?;
        transaction.commit()?;
        Ok(Some(candidate.into_lease(
            subscription_id,
            token,
            attempt,
            expires_at_ms,
        )))
    }

    pub fn settle_delivery(
        &mut self,
        lease: &DeliveryLease,
        outcome: DeliveryOutcome,
        settled_at_ms: i64,
        reason: Option<&str>,
    ) -> Result<(), DeliveryError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let state = outcome_state(outcome);
        let changed = transaction.execute(
            "UPDATE deliveries
             SET state = ?1, lease_token = NULL, lease_expires_at_ms = NULL,
                 settled_at_ms = ?2, settlement_reason = ?3
             WHERE id = ?4 AND subscription_id = ?5 AND state = 'leased'
               AND lease_token = ?6 AND lease_expires_at_ms > ?2",
            params![
                state.as_db(),
                settled_at_ms,
                reason,
                as_i64(lease.delivery_id, "delivery ID")?,
                lease.subscription_id.as_uuid().as_bytes(),
                lease.token.as_uuid().as_bytes(),
            ],
        )?;
        if changed != 1 {
            return Err(DeliveryError::StaleLease);
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn retry_delivery(
        &mut self,
        lease: &DeliveryLease,
        now_ms: i64,
    ) -> Result<(), DeliveryError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let admission = subscription_admission(&transaction, lease.subscription_id)?;
        let not_before_ms = retry_not_before_ms(&admission.retry_policy, lease.attempt, now_ms)?;
        let changed = transaction.execute(
            "UPDATE deliveries
             SET state = CASE WHEN attempts >= ?5 THEN 'dead_lettered' ELSE 'queued' END,
                 lease_token = NULL, lease_expires_at_ms = NULL,
                 not_before_ms = CASE WHEN attempts >= ?5 THEN not_before_ms ELSE ?6 END,
                 settled_at_ms = CASE WHEN attempts >= ?5 THEN ?4 ELSE NULL END,
                 settlement_reason = CASE WHEN attempts >= ?5 THEN 'retry policy exhausted' ELSE NULL END
             WHERE id = ?1 AND subscription_id = ?2 AND state = 'leased'
               AND lease_token = ?3 AND lease_expires_at_ms > ?4",
            params![
                as_i64(lease.delivery_id, "delivery ID")?,
                lease.subscription_id.as_uuid().as_bytes(),
                lease.token.as_uuid().as_bytes(),
                now_ms,
                i64::from(admission.retry_policy.max_attempts),
                not_before_ms,
            ],
        )?;
        if changed != 1 {
            return Err(DeliveryError::StaleLease);
        }
        transaction.commit()?;
        Ok(())
    }

    /// Conservatively pauses one leased delivery when its required payload is
    /// unavailable. Unlike a dead letter, this remains an unresolved ordering
    /// barrier until an operator deliberately requeues it or changes policy.
    pub fn block_delivery(
        &mut self,
        lease: &DeliveryLease,
        now_ms: i64,
        reason: &str,
    ) -> Result<(), DeliveryError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = transaction.execute(
            "UPDATE deliveries
             SET state = 'blocked', lease_token = NULL, lease_expires_at_ms = NULL,
                 settled_at_ms = NULL, settlement_reason = ?1
             WHERE id = ?2 AND subscription_id = ?3 AND state = 'leased'
               AND lease_token = ?4 AND lease_expires_at_ms > ?5",
            params![
                reason,
                as_i64(lease.delivery_id, "delivery ID")?,
                lease.subscription_id.as_uuid().as_bytes(),
                lease.token.as_uuid().as_bytes(),
                now_ms,
            ],
        )?;
        if changed != 1 {
            return Err(DeliveryError::StaleLease);
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn delivery_counts(
        &self,
        subscription_id: SubscriptionId,
    ) -> Result<DeliveryCounts, DeliveryError> {
        subscription_admission(&self.connection, subscription_id)?;
        let mut counts = DeliveryCounts {
            queued: 0,
            leased: 0,
            blocked: 0,
            acknowledged: 0,
            superseded: 0,
            ignored_by_policy: 0,
            dead_lettered: 0,
        };
        let mut statement = self.connection.prepare(
            "SELECT state, COUNT(*) FROM deliveries WHERE subscription_id = ?1 GROUP BY state",
        )?;
        let rows = statement.query_map(params![subscription_id.as_uuid().as_bytes()], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?;
        for row in rows {
            let (state, count) = row?;
            let count = as_u64(count, "delivery count")?;
            match DeliveryState::from_db(&state)? {
                DeliveryState::Queued => counts.queued = count,
                DeliveryState::Leased => counts.leased = count,
                DeliveryState::Blocked => counts.blocked = count,
                DeliveryState::Acknowledged => counts.acknowledged = count,
                DeliveryState::Superseded => counts.superseded = count,
                DeliveryState::IgnoredByPolicy => counts.ignored_by_policy = count,
                DeliveryState::DeadLettered => counts.dead_lettered = count,
            }
        }
        Ok(counts)
    }

    /// Pausing is durable and affects only this subscription epoch. Its queued
    /// work remains in the ledger and becomes leaseable again on resume.
    pub fn set_subscription_enabled(
        &mut self,
        subscription_id: SubscriptionId,
        enabled: bool,
    ) -> Result<(), DeliveryError> {
        let changed = self.connection.execute(
            "UPDATE subscriptions SET enabled = ?1 WHERE id = ?2",
            params![i64::from(enabled), subscription_id.as_uuid().as_bytes()],
        )?;
        if changed != 1 {
            return Err(DeliveryError::UnknownSubscription);
        }
        Ok(())
    }
}

pub(crate) fn enqueue_event_for_active_subscriptions(
    transaction: &Transaction<'_>,
    event: &ArchiveEvent,
) -> Result<(), CatalogError> {
    let mut statement = transaction.prepare(
        "SELECT id, consumer_id, max_active_leases, accepts_moves, accepts_tombstones,
                max_attempts, initial_backoff_ms, max_backoff_ms
         FROM subscriptions",
    )?;
    let subscriptions = statement
        .query_map([], |row| {
            let id = SubscriptionId::from_uuid(uuid_from_blob(&row.get::<_, Vec<u8>>(0)?)?);
            let config = SubscriptionConfig {
                consumer_id: row.get(1)?,
                max_active_leases: u32::try_from(row.get::<_, i64>(2)?).map_err(|_| {
                    to_sql_error(DeliveryError::Corrupt(
                        "invalid max_active_leases".to_owned(),
                    ))
                })?,
                accepts_moves: row.get::<_, i64>(3)? != 0,
                accepts_tombstones: row.get::<_, i64>(4)? != 0,
                replay_after_sequence: 0,
                retry_policy: RetryPolicy::new(
                    u32::try_from(row.get::<_, i64>(5)?).map_err(|_| {
                        to_sql_error(DeliveryError::Corrupt("invalid max attempts".to_owned()))
                    })?,
                    as_u64(row.get::<_, i64>(6)?, "initial retry backoff")?,
                    as_u64(row.get::<_, i64>(7)?, "maximum retry backoff")?,
                )
                .map_err(to_sql_error)?,
            };
            Ok((id, config))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    drop(statement);
    for (subscription_id, config) in subscriptions {
        enqueue_delivery(transaction, &subscription_id, &config, event, false)?;
    }
    Ok(())
}

fn enqueue_delivery(
    transaction: &Transaction<'_>,
    subscription_id: &SubscriptionId,
    config: &SubscriptionConfig,
    event: &ArchiveEvent,
    is_snapshot: bool,
) -> Result<(), CatalogError> {
    let state = initial_delivery_state(config, event.kind);
    let (root_role, source_relative_path) = event
        .location
        .as_ref()
        .map(|location| {
            (
                Some(location.root_role()),
                Some(location.source_relative_path()),
            )
        })
        .unwrap_or((None, None));
    transaction.execute(
        "INSERT INTO deliveries (
            subscription_id, event_id, event_sequence, record_id, record_version,
            kind, revision_id, root_role, source_relative_path, is_snapshot, state,
            attempts, lease_token, lease_expires_at_ms, settled_at_ms,
            settlement_reason, created_at_ms
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, 0, NULL, NULL, NULL, NULL, ?12)",
        params![
            subscription_id.as_uuid().as_bytes(),
            event.id.as_uuid().as_bytes(),
            if is_snapshot {
                None
            } else {
                Some(as_i64(event.sequence, "event sequence")?)
            },
            event.record_id.as_uuid().as_bytes(),
            as_i64(event.record_version, "record version")?,
            event.kind.as_db(),
            event.revision_id.map(|id| id.as_uuid().as_bytes().to_vec()),
            root_role,
            source_relative_path,
            i64::from(is_snapshot),
            state.as_db(),
            event.committed_at_ms,
        ],
    )?;
    Ok(())
}

fn initial_delivery_state(config: &SubscriptionConfig, kind: EventKind) -> DeliveryState {
    match kind {
        EventKind::LocationChanged if !config.accepts_moves => DeliveryState::Blocked,
        EventKind::RecordTombstoned if !config.accepts_tombstones => DeliveryState::Blocked,
        _ => DeliveryState::Queued,
    }
}

fn subscription_admission(
    connection: &rusqlite::Connection,
    subscription_id: SubscriptionId,
) -> Result<SubscriptionAdmission, DeliveryError> {
    let (max_active_leases, enabled, max_attempts, initial_backoff_ms, max_backoff_ms) = connection
        .query_row(
            "SELECT max_active_leases, enabled, max_attempts, initial_backoff_ms, max_backoff_ms FROM subscriptions WHERE id = ?1",
            params![subscription_id.as_uuid().as_bytes()],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?, row.get::<_, i64>(2)?, row.get::<_, i64>(3)?, row.get::<_, i64>(4)?)),
        )
        .optional()?
        .ok_or(DeliveryError::UnknownSubscription)?;
    let max_active_leases = u32::try_from(max_active_leases)
        .map_err(|_| DeliveryError::Corrupt("invalid max_active_leases".to_owned()))?;
    let retry_policy = RetryPolicy::new(
        u32::try_from(max_attempts)
            .map_err(|_| DeliveryError::Corrupt("invalid max attempts".to_owned()))?,
        as_u64(initial_backoff_ms, "initial retry backoff")?,
        as_u64(max_backoff_ms, "maximum retry backoff")?,
    )?;
    match enabled {
        0 => Ok(SubscriptionAdmission {
            max_active_leases,
            enabled: false,
            retry_policy,
        }),
        1 => Ok(SubscriptionAdmission {
            max_active_leases,
            enabled: true,
            retry_policy,
        }),
        _ => Err(DeliveryError::Corrupt(
            "invalid subscription enabled value".to_owned(),
        )),
    }
}

#[derive(Clone, Copy, Debug)]
struct SubscriptionAdmission {
    max_active_leases: u32,
    enabled: bool,
    retry_policy: RetryPolicy,
}

fn outcome_state(outcome: DeliveryOutcome) -> DeliveryState {
    match outcome {
        DeliveryOutcome::Acknowledged => DeliveryState::Acknowledged,
        DeliveryOutcome::Superseded => DeliveryState::Superseded,
        DeliveryOutcome::IgnoredByPolicy => DeliveryState::IgnoredByPolicy,
        DeliveryOutcome::DeadLettered => DeliveryState::DeadLettered,
    }
}

fn retry_not_before_ms(
    policy: &RetryPolicy,
    attempt: u32,
    now_ms: i64,
) -> Result<i64, DeliveryError> {
    let shift = attempt.saturating_sub(1).min(63);
    let multiplier = 1_u64.checked_shl(shift).unwrap_or(u64::MAX);
    let delay = policy
        .initial_backoff_ms
        .saturating_mul(multiplier)
        .min(policy.max_backoff_ms);
    now_ms
        .checked_add(i64::try_from(delay).map_err(|_| DeliveryError::InvalidLeaseDuration)?)
        .ok_or(DeliveryError::InvalidLeaseDuration)
}

#[derive(Clone, Debug)]
struct DeliverySnapshot {
    record_id: RecordId,
    record_version: u64,
    revision_id: Option<RevisionId>,
    location: LogicalLocation,
}

#[derive(Clone, Debug)]
struct DeliveryCandidate {
    delivery_id: u64,
    event_id: EventId,
    event_sequence: Option<u64>,
    record_id: RecordId,
    record_version: u64,
    kind: EventKind,
    revision_id: Option<RevisionId>,
    revision: Option<CanonicalRevision>,
    location: Option<LogicalLocation>,
    is_snapshot: bool,
    attempts: u32,
}

impl DeliveryCandidate {
    fn into_lease(
        self,
        subscription_id: SubscriptionId,
        token: LeaseToken,
        attempt: u32,
        expires_at_ms: i64,
    ) -> DeliveryLease {
        DeliveryLease {
            delivery_id: self.delivery_id,
            subscription_id,
            event_id: self.event_id,
            event_sequence: self.event_sequence,
            record_id: self.record_id,
            record_version: self.record_version,
            kind: self.kind,
            revision_id: self.revision_id,
            revision: self.revision,
            location: self.location,
            is_snapshot: self.is_snapshot,
            attempt,
            token,
            expires_at_ms,
        }
    }
}

fn delivery_candidate_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<DeliveryCandidate> {
    let delivery_id = as_u64(row.get::<_, i64>(0)?, "delivery ID")?;
    let event_id = EventId::from_uuid(uuid_from_blob(&row.get::<_, Vec<u8>>(1)?)?);
    let event_sequence = row
        .get::<_, Option<i64>>(2)?
        .map(|value| as_u64(value, "event sequence"))
        .transpose()?;
    let record_id = RecordId::from_uuid(uuid_from_blob(&row.get::<_, Vec<u8>>(3)?)?);
    let record_version = as_u64(row.get::<_, i64>(4)?, "record version")?;
    let kind = EventKind::from_db(&row.get::<_, String>(5)?).map_err(domain_to_sql_error)?;
    let revision_id = row
        .get::<_, Option<Vec<u8>>>(6)?
        .map(|value| uuid_from_blob(&value).map(RevisionId::from_uuid))
        .transpose()?;
    let root_role = row.get::<_, Option<String>>(7)?;
    let source_relative_path = row.get::<_, Option<String>>(8)?;
    let location = match (root_role, source_relative_path) {
        (Some(root_role), Some(source_relative_path)) => Some(
            LogicalLocation::new(root_role, source_relative_path).map_err(domain_to_sql_error)?,
        ),
        (None, None) => None,
        _ => {
            return Err(to_sql_error(DeliveryError::Corrupt(
                "incomplete delivery location".to_owned(),
            )));
        }
    };
    let byte_length = row.get::<_, Option<i64>>(11)?;
    let digest = row.get::<_, Option<Vec<u8>>>(12)?;
    let revision = match (byte_length, digest) {
        (Some(byte_length), Some(digest)) => Some(CanonicalRevision::from_parts(
            as_u64(byte_length, "revision byte length")?,
            digest_from_blob(digest)?,
        )),
        (None, None) => None,
        _ => {
            return Err(to_sql_error(DeliveryError::Corrupt(
                "incomplete delivery revision".to_owned(),
            )));
        }
    };
    Ok(DeliveryCandidate {
        delivery_id,
        event_id,
        event_sequence,
        record_id,
        record_version,
        kind,
        revision_id,
        revision,
        location,
        is_snapshot: row.get::<_, i64>(9)? != 0,
        attempts: u32::try_from(row.get::<_, i64>(10)?).map_err(|_| {
            to_sql_error(DeliveryError::Corrupt(
                "invalid delivery attempts".to_owned(),
            ))
        })?,
    })
}

fn event_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ArchiveEvent> {
    let root_role = row.get::<_, Option<String>>(6)?;
    let source_relative_path = row.get::<_, Option<String>>(7)?;
    let location = match (root_role, source_relative_path) {
        (Some(root_role), Some(source_relative_path)) => Some(
            LogicalLocation::new(root_role, source_relative_path).map_err(domain_to_sql_error)?,
        ),
        (None, None) => None,
        _ => {
            return Err(to_sql_error(DeliveryError::Corrupt(
                "incomplete event location".to_owned(),
            )));
        }
    };
    Ok(ArchiveEvent {
        id: EventId::from_uuid(uuid_from_blob(&row.get::<_, Vec<u8>>(0)?)?),
        sequence: as_u64(row.get::<_, i64>(1)?, "event sequence")?,
        record_id: RecordId::from_uuid(uuid_from_blob(&row.get::<_, Vec<u8>>(2)?)?),
        record_version: as_u64(row.get::<_, i64>(3)?, "record version")?,
        kind: EventKind::from_db(&row.get::<_, String>(4)?).map_err(domain_to_sql_error)?,
        revision_id: row
            .get::<_, Option<Vec<u8>>>(5)?
            .map(|value| uuid_from_blob(&value).map(RevisionId::from_uuid))
            .transpose()?,
        location,
        committed_at_ms: row.get::<_, i64>(8)?,
    })
}

fn as_i64(value: u64, field: &str) -> rusqlite::Result<i64> {
    i64::try_from(value).map_err(|_| {
        to_sql_error(DeliveryError::Corrupt(format!(
            "{field} exceeds SQLite range"
        )))
    })
}

fn as_u64(value: i64, field: &str) -> rusqlite::Result<u64> {
    u64::try_from(value)
        .map_err(|_| to_sql_error(DeliveryError::Corrupt(format!("negative {field}"))))
}

fn uuid_from_blob(value: &[u8]) -> rusqlite::Result<Uuid> {
    Uuid::from_slice(value)
        .map_err(|_| to_sql_error(DeliveryError::Corrupt("invalid UUID blob".to_owned())))
}

fn digest_from_blob(value: Vec<u8>) -> rusqlite::Result<[u8; blake3::OUT_LEN]> {
    value.try_into().map_err(|_| {
        to_sql_error(DeliveryError::Corrupt(
            "invalid BLAKE3 digest length".to_owned(),
        ))
    })
}

fn domain_to_sql_error(error: crate::domain::DomainError) -> rusqlite::Error {
    to_sql_error(DeliveryError::Corrupt(error.to_string()))
}

fn to_sql_error(error: DeliveryError) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Blob, Box::new(error))
}

#[derive(Debug, Error)]
pub enum DeliveryError {
    #[error(transparent)]
    Catalog(#[from] CatalogError),
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("invalid subscription: {0}")]
    InvalidSubscription(String),
    #[error("unknown subscription")]
    UnknownSubscription,
    #[error("lease duration must fit in a positive i64 millisecond timestamp")]
    InvalidLeaseDuration,
    #[error("lease is stale, expired, or already settled")]
    StaleLease,
    #[error("delivery state is corrupt: {0}")]
    Corrupt(String),
}

#[cfg(test)]
mod tests {
    use super::{RetryPolicy, SubscriptionConfig, SubscriptionMode};
    use crate::catalog::Catalog;
    use crate::domain::{
        CanonicalRevision, DeliveryOutcome, EventKind, LogicalLocation, ProducerId, RecordIdentity,
    };

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

    fn location() -> LogicalLocation {
        LogicalLocation::new("active", "sessions/a.jsonl").unwrap()
    }

    fn config() -> SubscriptionConfig {
        SubscriptionConfig::new("fake-consumer", 1, true, true).unwrap()
    }

    #[test]
    fn later_record_versions_wait_for_the_prior_lease_to_settle() {
        let mut catalog = Catalog::open_in_memory().unwrap();
        let subscription = catalog
            .create_subscription(
                config().with_retry_policy(RetryPolicy::new(8, 0, 0).unwrap()),
                SubscriptionMode::ReplayEvents,
                0,
            )
            .unwrap();
        let identity = identity();
        catalog
            .observe_present_at(
                &identity,
                &location(),
                CanonicalRevision::from_bytes(b"first\n"),
                10,
            )
            .unwrap();
        catalog
            .observe_present_at(
                &identity,
                &location(),
                CanonicalRevision::from_bytes(b"first\nsecond\n"),
                20,
            )
            .unwrap();

        let first = catalog
            .lease_next(subscription.id, 30, 100)
            .unwrap()
            .unwrap();
        assert_eq!(first.record_version, 1);
        assert!(
            catalog
                .lease_next(subscription.id, 31, 100)
                .unwrap()
                .is_none()
        );
        catalog
            .settle_delivery(&first, DeliveryOutcome::Acknowledged, 40, None)
            .unwrap();
        let second = catalog
            .lease_next(subscription.id, 41, 100)
            .unwrap()
            .unwrap();

        assert_eq!(second.record_version, 2);
        assert_eq!(second.kind, EventKind::RevisionCommitted);
        assert_eq!(
            second.location.unwrap().source_relative_path(),
            "sessions/a.jsonl"
        );
    }

    #[test]
    fn unrelated_records_can_use_the_configured_parallel_lease_budget() {
        let mut catalog = Catalog::open_in_memory().unwrap();
        let subscription = catalog
            .create_subscription(
                SubscriptionConfig::new("fake-consumer", 2, true, true).unwrap(),
                SubscriptionMode::ReplayEvents,
                0,
            )
            .unwrap();
        let first_identity = identity();
        let second_identity = identity();
        catalog
            .observe_present_at(
                &first_identity,
                &location(),
                CanonicalRevision::from_bytes(b"first\n"),
                10,
            )
            .unwrap();
        catalog
            .observe_present_at(
                &second_identity,
                &location(),
                CanonicalRevision::from_bytes(b"second\n"),
                20,
            )
            .unwrap();

        let first = catalog
            .lease_next(subscription.id, 30, 100)
            .unwrap()
            .unwrap();
        let second = catalog
            .lease_next(subscription.id, 31, 100)
            .unwrap()
            .unwrap();

        assert_ne!(first.record_id, second.record_id);
        assert!(
            catalog
                .lease_next(subscription.id, 32, 100)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn expired_lease_redelivers_with_the_same_event_id() {
        let mut catalog = Catalog::open_in_memory().unwrap();
        let subscription = catalog
            .create_subscription(
                config().with_retry_policy(RetryPolicy::new(8, 0, 0).unwrap()),
                SubscriptionMode::ReplayEvents,
                0,
            )
            .unwrap();
        let identity = identity();
        catalog
            .observe_present_at(
                &identity,
                &location(),
                CanonicalRevision::from_bytes(b"first\n"),
                10,
            )
            .unwrap();

        let first = catalog
            .lease_next(subscription.id, 20, 10)
            .unwrap()
            .unwrap();
        let redelivery = catalog
            .lease_next(subscription.id, 30, 10)
            .unwrap()
            .unwrap();

        assert_eq!(redelivery.event_id, first.event_id);
        assert_ne!(redelivery.token, first.token);
        assert_eq!(redelivery.attempt, 2);
    }

    #[test]
    fn an_expired_lease_cannot_acknowledge_or_retry_before_scheduler_cleanup() {
        let mut catalog = Catalog::open_in_memory().unwrap();
        let subscription = catalog
            .create_subscription(
                config().with_retry_policy(RetryPolicy::new(8, 0, 0).unwrap()),
                SubscriptionMode::ReplayEvents,
                0,
            )
            .unwrap();
        let identity = identity();
        catalog
            .observe_present_at(
                &identity,
                &location(),
                CanonicalRevision::from_bytes(b"first\n"),
                10,
            )
            .unwrap();

        let lease = catalog
            .lease_next(subscription.id, 20, 10)
            .unwrap()
            .unwrap();
        assert!(
            catalog
                .settle_delivery(&lease, DeliveryOutcome::Acknowledged, 30, None)
                .is_err()
        );
        assert!(catalog.retry_delivery(&lease, 30).is_err());

        let replacement = catalog
            .lease_next(subscription.id, 30, 10)
            .unwrap()
            .unwrap();
        assert_eq!(replacement.event_id, lease.event_id);
        assert_eq!(replacement.attempt, 2);
    }

    #[test]
    fn zero_duration_lease_is_rejected() {
        let mut catalog = Catalog::open_in_memory().unwrap();
        let subscription = catalog
            .create_subscription(config(), SubscriptionMode::ReplayEvents, 0)
            .unwrap();

        assert!(catalog.lease_next(subscription.id, 20, 0).is_err());
    }

    #[test]
    fn retry_policy_applies_exponential_backoff_then_dead_letters() {
        let mut catalog = Catalog::open_in_memory().unwrap();
        let subscription = catalog
            .create_subscription(
                config().with_retry_policy(RetryPolicy::new(3, 10, 40).unwrap()),
                SubscriptionMode::ReplayEvents,
                0,
            )
            .unwrap();
        let identity = identity();
        catalog
            .observe_present_at(
                &identity,
                &location(),
                CanonicalRevision::from_bytes(b"first\n"),
                10,
            )
            .unwrap();

        let first = catalog
            .lease_next(subscription.id, 20, 100)
            .unwrap()
            .unwrap();
        catalog.retry_delivery(&first, 20).unwrap();
        assert!(
            catalog
                .lease_next(subscription.id, 29, 100)
                .unwrap()
                .is_none()
        );
        let second = catalog
            .lease_next(subscription.id, 30, 100)
            .unwrap()
            .unwrap();
        assert_eq!(second.attempt, 2);
        catalog.retry_delivery(&second, 30).unwrap();
        assert!(
            catalog
                .lease_next(subscription.id, 49, 100)
                .unwrap()
                .is_none()
        );
        let third = catalog
            .lease_next(subscription.id, 50, 100)
            .unwrap()
            .unwrap();
        assert_eq!(third.attempt, 3);
        catalog.retry_delivery(&third, 50).unwrap();

        assert!(
            catalog
                .lease_next(subscription.id, 100, 100)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            catalog
                .delivery_counts(subscription.id)
                .unwrap()
                .dead_lettered,
            1
        );
    }

    #[test]
    fn dead_letter_is_settled_but_keeps_later_record_versions_blocked() {
        let mut catalog = Catalog::open_in_memory().unwrap();
        let subscription = catalog
            .create_subscription(config(), SubscriptionMode::ReplayEvents, 0)
            .unwrap();
        let identity = identity();
        catalog
            .observe_present_at(
                &identity,
                &location(),
                CanonicalRevision::from_bytes(b"first\n"),
                10,
            )
            .unwrap();
        catalog
            .observe_present_at(
                &identity,
                &location(),
                CanonicalRevision::from_bytes(b"second\n"),
                20,
            )
            .unwrap();

        let first = catalog
            .lease_next(subscription.id, 30, 100)
            .unwrap()
            .unwrap();
        catalog
            .settle_delivery(
                &first,
                DeliveryOutcome::DeadLettered,
                40,
                Some("fixture failure"),
            )
            .unwrap();

        assert!(
            catalog
                .lease_next(subscription.id, 41, 100)
                .unwrap()
                .is_none()
        );
        let counts = catalog.delivery_counts(subscription.id).unwrap();
        assert_eq!(counts.dead_lettered, 1);
        assert_eq!(counts.queued, 1);
    }

    #[test]
    fn subscription_epoch_rebuild_has_fresh_delivery_identity() {
        let mut catalog = Catalog::open_in_memory().unwrap();
        let identity = identity();
        catalog
            .observe_present_at(
                &identity,
                &location(),
                CanonicalRevision::from_bytes(b"first\n"),
                10,
            )
            .unwrap();
        let first_subscription = catalog
            .create_subscription(config(), SubscriptionMode::ReplayEvents, 20)
            .unwrap();
        let first = catalog
            .lease_next(first_subscription.id, 30, 100)
            .unwrap()
            .unwrap();
        catalog
            .settle_delivery(&first, DeliveryOutcome::Acknowledged, 40, None)
            .unwrap();

        let rebuilt = catalog
            .create_subscription(config(), SubscriptionMode::RebuildCurrent, 50)
            .unwrap();
        let snapshot = catalog.lease_next(rebuilt.id, 60, 100).unwrap().unwrap();

        assert_ne!(rebuilt.id, first_subscription.id);
        assert!(snapshot.is_snapshot);
        assert_ne!(snapshot.event_id, first.event_id);
        assert_eq!(snapshot.record_id, first.record_id);
    }

    #[test]
    fn paused_subscription_keeps_queued_work_without_leasing_it() {
        let mut catalog = Catalog::open_in_memory().unwrap();
        let subscription = catalog
            .create_subscription(config(), SubscriptionMode::ReplayEvents, 0)
            .unwrap();
        let identity = identity();
        catalog
            .observe_present_at(
                &identity,
                &location(),
                CanonicalRevision::from_bytes(b"first\n"),
                10,
            )
            .unwrap();

        catalog
            .set_subscription_enabled(subscription.id, false)
            .unwrap();
        assert!(
            catalog
                .lease_next(subscription.id, 20, 100)
                .unwrap()
                .is_none()
        );
        assert_eq!(catalog.delivery_counts(subscription.id).unwrap().queued, 1);

        catalog
            .set_subscription_enabled(subscription.id, true)
            .unwrap();
        assert!(
            catalog
                .lease_next(subscription.id, 21, 100)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn replay_after_sequence_skips_old_events_but_subscribes_to_new_ones() {
        let mut catalog = Catalog::open_in_memory().unwrap();
        let identity = identity();
        catalog
            .observe_present_at(
                &identity,
                &location(),
                CanonicalRevision::from_bytes(b"first\n"),
                10,
            )
            .unwrap();
        let subscription = catalog
            .create_subscription(
                config().with_replay_after_sequence(1),
                SubscriptionMode::ReplayEvents,
                20,
            )
            .unwrap();
        assert_eq!(subscription.replay_after_sequence, 1);
        assert!(
            catalog
                .lease_next(subscription.id, 30, 100)
                .unwrap()
                .is_none()
        );

        catalog
            .observe_present_at(
                &identity,
                &location(),
                CanonicalRevision::from_bytes(b"second\n"),
                40,
            )
            .unwrap();
        let lease = catalog
            .lease_next(subscription.id, 50, 100)
            .unwrap()
            .unwrap();

        assert_eq!(lease.record_version, 2);
        assert_eq!(lease.event_sequence, Some(2));
    }

    #[test]
    fn unsupported_tombstone_is_blocked_instead_of_silently_settled() {
        let mut catalog = Catalog::open_in_memory().unwrap();
        let no_tombstones = SubscriptionConfig::new("fake-consumer", 1, true, false).unwrap();
        let subscription = catalog
            .create_subscription(no_tombstones, SubscriptionMode::ReplayEvents, 0)
            .unwrap();
        let identity = identity();
        catalog
            .observe_present_at(
                &identity,
                &location(),
                CanonicalRevision::from_bytes(b"first\n"),
                10,
            )
            .unwrap();
        let revision = catalog
            .lease_next(subscription.id, 20, 100)
            .unwrap()
            .unwrap();
        catalog
            .settle_delivery(&revision, DeliveryOutcome::Acknowledged, 30, None)
            .unwrap();
        catalog.tombstone_at(&identity, 40).unwrap();

        assert!(
            catalog
                .lease_next(subscription.id, 50, 100)
                .unwrap()
                .is_none()
        );
        let counts = catalog.delivery_counts(subscription.id).unwrap();
        assert_eq!(counts.blocked, 1);
    }
}
