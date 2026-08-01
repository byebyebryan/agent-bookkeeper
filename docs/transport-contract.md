# Transport contract

This document specifies the behavior Bookkeeper needs from a transport, independent of implementation.

## Record fields

Every observed raw record has at least:

| Field | Meaning |
| --- | --- |
| `producer_id` | Stable identifier for the device or local profile that produced the record. |
| `agent_namespace` | Agent/tool family namespace. |
| `session_id` | Producer-scoped session identifier. |
| `record_kind` | Payload role within the session, initially `transcript`. |
| `record_key` | Stable key within that role, initially `primary`. |
| `location` | Current source-relative location; mutable metadata, not identity. |
| `record_version` | Monotonic accepted state transition for this record. |
| `revision` | Byte-exact source version using length and canonical BLAKE3-256. |
| `source_observed_at` | Informational producer/client observation time. |
| `committed_at` | Server commit time; event sequence, not time, controls ordering. |
| `state` | Active, deleted, or retention-expired. |

The identity tuple is `(producer_id, agent_namespace, session_id, record_kind,
record_key)`. The provider uses a versioned identity schema. The initial
implementation rejects a source without stable, path-independent session
identity. See [domain-contract.md](domain-contract.md).

## Source delivery states

```text
pending -> scanned -> prepared -> uploaded -> committed
                         |             |
                         +---- retry --+
```

- **pending:** a lifecycle trigger requested eventual work.
- **scanned:** the client has observed candidate records and their cheap metadata.
- **prepared:** stable revisions have been selected for delivery.
- **uploaded:** transport staging has enough data for the selected revisions.
- **committed:** the service or destination has atomically published a generation.
Only a committed generation is visible to consumers. Retrying a request with the same record identity and revision must be idempotent.

V1.5 begins at `scanned` because its source adapter observes an external mirror;
it emits `completeness=observed` generations. V2 owns all source-delivery states
and emits `completeness=declared` generations committed by a client.

## Consumer delivery states

```text
queued -> leased -> acknowledged | superseded | ignored_by_policy | dead_lettered
   ^         |
   +---------+---- retryable failure / lease expiry
```

Consumer delivery is at least once until an explicit settled outcome. An
external adapter is idempotent on `(subscription_id, event_id)`. Different
records may run concurrently; later versions of one record remain blocked behind
its prior unresolved ordering barrier. A cursor is a compact high-water mark and
advances only across contiguous advancing rows; `dead_lettered` is settled but
continues to block until operator resolution. The complete contract is in
[payload-delivery-contract.md](payload-delivery-contract.md).

## Offline and failure behavior

If the destination is unreachable, the client retains durable pending state and exits without reporting success. The next trigger reschedules outstanding work before scanning for new work. Failures use bounded retry and backoff; a hook never blocks on retry. A manual status surface may report the oldest pending revision, last successful commit, and last error without exposing transcript content.

A V1.5 source-root outage blocks reconciliation. It must not be interpreted as
bulk deletion. A consumer outage affects only that consumer's leased and pending
deliveries.

## Revisions and atomicity

An append, replacement, truncation, or move is an observation of a new revision
or location for the same record identity. V1.5 records a guarded observation and
delivers through a held-descriptor `RevisionReader`. A source without a valid
root identity guard has deletion detection disabled. V2 stages bounded objects,
verifies canonical complete-record BLAKE3-256 from their ordered manifest, and
publishes a commit receipt with the durability protocol in
[durability-contract.md](durability-contract.md). Partial data must not become an
eligible revision.

Tombstones are records too. A receiver retains enough tombstone metadata to prevent a delayed retry from resurrecting an intentionally removed revision.

## Compatibility requirements

The V1 filesystem mirror and V2 API may differ in mechanics but must present the same conceptual output: committed raw records, stable identities, revisions, and explicit deletion state. The catalog/controller is the adapter boundary that lets existing consumers continue while transport evolves.

The raw-byte locator is an opaque `RevisionReader`. V1.5 resolves it through a
held, verified external descriptor; V2 resolves it to a committed chunk
manifest. Path-only consumers receive bounded lease-scoped materialization.
Consumers must not treat a V1 filesystem path, V2 object layout, cache, or
current projection as permanent archive identity.
