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

## Delivery states

```text
pending -> scanned -> prepared -> uploaded -> committed -> observed
                         |             |
                         +---- retry --+
```

- **pending:** a lifecycle trigger requested eventual work.
- **scanned:** the client has observed candidate records and their cheap metadata.
- **prepared:** stable revisions have been selected for delivery.
- **uploaded:** transport staging has enough data for the selected revisions.
- **committed:** the service or destination has atomically published a generation.
- **observed:** a consumer has advanced its own cursor; this state is per consumer.

Only a committed generation is visible to consumers. Retrying a request with the same record identity and revision must be idempotent.

## Offline and failure behavior

If the destination is unreachable, the client retains durable pending state and exits without reporting success. The next trigger reschedules outstanding work before scanning for new work. Failures use bounded retry and backoff; a hook never blocks on retry. A manual status surface may report the oldest pending revision, last successful commit, and last error without exposing transcript content.

## Revisions and atomicity

An append, replacement, truncation, or move is an observation of a new revision or location for the same record identity. The receiver stages data under a generation or upload identifier, validates revision metadata, and publishes the generation atomically. Partial data must not replace the last committed raw projection.

Tombstones are records too. A receiver retains enough tombstone metadata to prevent a delayed retry from resurrecting an intentionally removed revision.

## Compatibility requirements

The V1 filesystem mirror and V2 API may differ in mechanics but must present the same conceptual output: committed raw records, stable identities, revisions, and explicit deletion state. The catalog/controller is the adapter boundary that lets existing consumers continue while transport evolves.
