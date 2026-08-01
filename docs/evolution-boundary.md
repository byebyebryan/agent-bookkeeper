# V1.5 to V2 evolution boundary

This document makes the overlap and dependency explicit. V1.5 is not throwaway
scaffolding, and V2 is not a rewrite of the archive controller.

## Dependency graph

```text
                         core domain and invariants
                                    |
                         durable archive ledger
                                    |
                    ordered events + consumer delivery
                         /                          \
                        /                            \
       V1.5 filesystem observer             V2 commit ingress
             |                              /        |         \
      external raw mirror          local client  object store  projection
             |                              \        |         /
             +---------- server-side seed ---\-------+--------+
```

V2 depends on the V1.5 core, ledger, and delivery semantics. It does not depend
on a particular V1 filesystem layout or consumer product.

## Reuse matrix

| Capability | V1.5 | V2 | Evolution |
| --- | --- | --- | --- |
| Producer, record, location, and revision identity | Introduced | Reused unchanged | Must stabilize before V2 protocol compatibility. |
| Algorithm-tagged whole-revision digest | Computed server-side | Computed client-side and verified server-side | Same field and validation rule. |
| Generation model | Observed scan generations | Native client-declared generations | Same type with different completeness. |
| Durable archive event ledger | Introduced | Reused unchanged | V2 commits append through the existing core transaction. |
| Consumer delivery, leases, replay, and backpressure | Introduced | Reused unchanged | Consumers do not know which ingress produced an event. |
| Raw-byte locator interface | Borrowed read-only filesystem path | Committed object manifest/projection | Consumers resolve an opaque locator through Bookkeeper. |
| Filesystem source adapter | Primary ingress observer | Import, seed, recovery, and audit tool | Retained, not deleted. |
| Reconciliation scheduler | Periodic correctness mechanism | Audits V2 storage/projection; no longer client ingress | Narrowed but still useful. |
| Client spool and lifecycle-hook executable | Existing external V1 helper | Owned by Bookkeeper | New V2 component. |
| Authenticated upload API | Absent | Added | New V2 component. |
| Chunk/object store | Absent | Added | New V2 component. |
| Canonical commit receipts | Ledger events over observed files | Added as durable raw-storage receipts | Indexed into the same catalog. |
| Raw projection writer | Borrowed external mirror | Added | Implements the existing raw-byte locator/projection interface. |
| MemPalace/Hindsight-specific logic | Outside Bookkeeper | Outside Bookkeeper | Integration belongs to Agent Historian or deployment adapters. |

## V1.5 decisions that constrain V2

V1.5 must get these contracts right before V2 freezes an API:

1. Stable session identity and the distinction between identity, location, and
   byte revision.
2. Algorithm-tagged digest representation.
3. Move, duplicate, conflict, tombstone, and restore semantics.
4. Ordered event sequence and generation completeness.
5. At-least-once consumer delivery and idempotency key.
6. Opaque raw-byte locator interface; consumers must not store a V1 path as the
   permanent archive identity.
7. Per-producer ingress authority, enabling an explicit mirror-to-API handoff.
8. Archive metadata backup and migration behavior.

Changing one of these after V2 clients exist can require protocol migration.
Storage layout, scan interval, SQLite table shape, and consumer worker topology
are implementation details and may evolve without changing the protocol.

## Work deliberately excluded from V1.5

V1.5 should not pre-build speculative V2 machinery:

- no client authentication service;
- no upload or missing-chunk API;
- no chunk/object storage;
- no canonical projection copy when consumers can safely borrow the mirror;
- no content-defined chunking dependency;
- no dual-authority mirror/API mode;
- no automatic object garbage collection.

The seam is an opaque raw-byte locator plus the archive commit function. Tests
should use both an external-file locator and an in-memory fake locator so V2 can
add object manifests without changing consumer behavior.

## V2 work that can proceed in parallel

Once the V1.5 domain types and fixtures are stable, bounded V2 spikes may run
without delaying controlled consumer tests:

- benchmark streaming fixed-size BLAKE3 chunking on representative large files;
- prove idempotent object `PUT` and missing-object queries;
- design canonical manifest serialization and golden fixtures;
- inject crashes around receipt publication and catalog recovery;
- prototype a local client spool and hook latency test.

These are proof artifacts until the V1.5 identity/event model passes its gates.
Do not publish a stable V2 protocol version before then.

## Cross-project ownership

| Project/layer | Owns |
| --- | --- |
| Agent Bookkeeper | Raw capture, transport, archive identities/revisions, durable commit ledger, projection, and consumer delivery. |
| Agent Historian | Composition, consumer selection, ingestion policy, retrieval/learned-memory integration, and operator-facing memory workflows. |
| Evidence/search consumer | Parsing, semantic chunks, embeddings, retrieval index, and provenance-bearing search. |
| Learned-memory consumer | Retain/recall semantics, synthesis, confidence, and deletion behavior in learned context. |
| Deployment repository | Concrete hosts, networks, storage paths, credentials, resource limits, and backup policy. |

Bookkeeper may ship provider and generic delivery adapters, but it must not
encode an outer memory product's semantic policy.

## Recommended implementation sequence

```text
V1.5 core/schema
  -> filesystem catalog dry-run
  -> event ledger + fake consumer
  -> controlled real consumer cohort
  -> V1.5 recovery/backpressure closure
  -> V2 chunk/object/receipt proof
  -> server-side seed from V1.5
  -> shadow V2 producer proof
  -> bounded V1-to-V2 cutover
```

The real-consumer cohort and V2 transport proof may overlap after the fake
consumer gate. The cutover may not.

## Decision gates

### Gate A: domain freeze

- Identity, revision, location, event, and delivery fixtures cover moves,
  rewrites, conflicts, tombstones, and replay.
- V2 manifest fixtures can represent every accepted V1.5 state transition.

### Gate B: V1.5 operational proof

- A steady archive scans cheaply.
- One large record does not cause unbounded memory or consumer work.
- A consumer reset and replay produces equivalent provenance-bearing output.

### Gate C: V2 storage proof

- Fixed chunks meet measured append-update and failure-recovery needs.
- Objects plus receipts reconstruct every selected record exactly.
- Receipt publication and catalog recovery survive injected crashes.

### Gate D: cutover readiness

- Existing current revisions are seeded without workstation retransmission.
- The same consumers process V1.5-observed and V2-declared revisions through
  one event contract.
- The producer authority switch and rollback are rehearsed with no dual writes.
