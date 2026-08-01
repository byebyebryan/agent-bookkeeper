# Transport contract

This document specifies the behavior Bookkeeper needs from a transport, independent of implementation.

## Record fields

Every observed raw record has at least:

| Field | Meaning |
| --- | --- |
| `producer_id` | Stable identifier for the device or local profile that produced the record. |
| `agent_namespace` | Agent/tool family namespace. |
| `session_id` | Producer-scoped session identifier. |
| `location` | Current source-relative location; mutable metadata, not identity. |
| `revision` | Byte-exact source version, including length and content digest. |
| `observed_at` | Time the client observed that revision. |
| `state` | Active, deleted, or retention-expired. |

The identity tuple is `(producer_id, agent_namespace, session_id)`. If a source format cannot provide a session ID, Bookkeeper must define a deterministic identity rule before accepting it.

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
queued -> leased -> acknowledged
   ^         |
   |         +-> retryable failure / lease expiry
   |
   +------------ replay
```

Consumer delivery is at least once until acknowledged. An external consumer or
adapter must be idempotent on the consumer, event sequence, record, and revision
identifiers. A cursor is a compact high-water mark; durable delivery rows remain
the authority for gaps and retries.

## Offline and failure behavior

If the destination is unreachable, the client retains durable pending state and exits without reporting success. The next trigger reschedules outstanding work before scanning for new work. Failures use bounded retry and backoff; a hook never blocks on retry. A manual status surface may report the oldest pending revision, last successful commit, and last error without exposing transcript content.

A V1.5 source-root outage blocks reconciliation. It must not be interpreted as
bulk deletion. A consumer outage affects only that consumer's leased and pending
deliveries.

## Revisions and atomicity

An append, replacement, truncation, or move is an observation of a new revision
or location for the same record identity. V1.5 records a guarded observation and
revalidates its borrowed current-file locator during consumption. V2 stages data
under a generation identifier, validates revision metadata, and publishes a
commit receipt atomically. Partial data must not become an eligible revision.

Tombstones are records too. A receiver retains enough tombstone metadata to prevent a delayed retry from resurrecting an intentionally removed revision.

## Compatibility requirements

The V1 filesystem mirror and V2 API may differ in mechanics but must present the same conceptual output: committed raw records, stable identities, revisions, and explicit deletion state. The catalog/controller is the adapter boundary that lets existing consumers continue while transport evolves.

The raw-byte locator is opaque to consumers. V1.5 resolves it to a guarded
external file; V2 resolves it to a committed object manifest or projection.
Consumers must not treat a V1 filesystem path as permanent archive identity.
