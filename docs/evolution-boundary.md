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
| Producer, session/record, location, and revision identity | Introduced | Reused unchanged | Includes record kind/key and versioned provider identity schema. |
| Canonical BLAKE3-256 whole-revision digest | Computed server-side | Computed client-side and verified server-side | Same revision key; secondary digests do not affect identity. |
| Generation model | Observed scan generations | Native client-declared generations | Same type with different completeness. |
| Durable archive event ledger | Introduced | Reused unchanged | V2 commits append through the existing core transaction. |
| Consumer delivery, leases, replay, and backpressure | Introduced | Reused unchanged | Per-record order, settled outcomes, and subscription epochs are ingress-neutral. |
| `RevisionReader` payload interface | Held/verified external file descriptor | Committed chunk manifest | Consumers never own mutable V1 paths or V2 object layout. |
| Filesystem source adapter | Primary ingress observer | Import, seed, recovery, and audit tool | Retained, not deleted. |
| Reconciliation scheduler | Periodic correctness mechanism | Audits V2 storage/projection; no longer client ingress | Narrowed but still useful. |
| Client spool and lifecycle-hook executable | Existing external V1 helper | Owned by Bookkeeper | New V2 component. |
| Authenticated upload API | Absent | Added | New V2 component. |
| Chunk/object store | Absent | Added | New V2 component. |
| Canonical commit receipts | Ledger events over observed files | Added as durable raw-storage receipts | Indexed into the same catalog. |
| Current projection writer | Borrowed external mirror | Added as asynchronous rebuildable compatibility/export view | Consumer eligibility remains ledger plus `RevisionReader`. |
| MemPalace/Hindsight-specific logic | Outside Bookkeeper | Outside Bookkeeper | Integration belongs to Agent Historian or deployment adapters. |

## V1.5 decisions that constrain V2

V1.5 must get these contracts right before V2 freezes an API:

1. Producer identity, versioned provider identity schema, and session/record
   identity including record kind/key.
2. Canonical BLAKE3-256 revision identity and secondary digest representation.
3. Move, duplicate, conflict, tombstone, restore, and monotonic record-version
   semantics.
4. Ordered event sequence and generation completeness.
5. Per-record ordered at-least-once delivery, settled outcomes, subscription
   epochs, and replay modes.
6. `RevisionReader`; consumers must not store a V1 path or V2 object layout as
   permanent archive identity.
7. Per-producer ingress authority and client epochs, enabling an explicit
   mirror-to-API handoff and local-state recovery.
8. Root-guarded deletion, integrity scrub, archive metadata backup, and
   migration-epoch recovery behavior.

Changing one of these after V2 clients exist can require protocol migration.
Storage layout, scan interval, SQLite table shape, and consumer worker topology
are implementation details and may evolve without changing the protocol.

## Work deliberately excluded from V1.5

V1.5 should not pre-build speculative V2 machinery:

- no client authentication service;
- no upload or missing-chunk API;
- no chunk/object storage;
- no canonical projection copy when consumers can safely use a verified
  external `RevisionReader`;
- no content-defined chunking dependency;
- no dual-authority mirror/API mode;
- no automatic object garbage collection.

The seam is `RevisionReader` plus the archive commit function. Tests use a held
external-file reader, an in-memory fake reader, and later a retained-chunk reader
so V2 adds object manifests without changing consumer behavior.

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
| Agent Bookkeeper | Raw capture, transport, archive identities/revisions, durable commit ledger, verified payload access, rebuildable current projection, and consumer delivery. |
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
- Held-descriptor and path-materialization fixtures prove exact-byte delivery
  when the mutable source path is replaced during a lease.
- V2 manifest fixtures can represent every accepted V1.5 state transition.

### Gate B: V1.5 operational proof

- A steady archive scans cheaply.
- One large record does not cause unbounded memory or consumer work.
- Per-record ordering survives lease loss, dead letters, tombstones, and stale
  delayed retries.
- `rebuild_current` and retained-payload `replay_events` produce their documented
  provenance-bearing output.
- Root outage never emits tombstones; ledger backup/restore preserves ordering.

### Gate C: V2 storage proof

- Fixed chunks meet measured append-update and failure-recovery needs.
- Objects plus receipts reconstruct every selected record exactly.
- Receipt publication and catalog recovery survive injected crashes.
- Committing append revisions does not retain a permanent reconstructed full
  file per revision; path materialization remains bounded cache state.

### Gate D: cutover readiness

- Existing current revisions are seeded without workstation retransmission.
- A migration-epoch export plus receipts rebuilds the archive catalog.
- The same consumers process V1.5-observed and V2-declared revisions through
  one event contract.
- The producer authority switch and rollback are rehearsed with no dual writes.
