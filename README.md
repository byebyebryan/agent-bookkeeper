# Agent Bookkeeper

Agent Bookkeeper is a reusable data plane for preserving raw agent-session evidence: local discovery, reliable delivery, durable storage, and revision-aware cataloging. It gives memory and analysis systems one trustworthy, replayable source of records without coupling them to an individual workstation or storage topology.

It complements, rather than replaces, an overall memory system such as [Agent Historian](https://github.com/byebyebryan/agent-historian):

- **Bookkeeper** records what happened and makes the raw evidence durably available.
- An archive or retrieval consumer turns those records into searchable evidence.
- A learned-memory consumer may derive concise, fallible working context from that evidence.

## Implementation status

V1.5 implementation is underway. The current checkpoint implements the shared
Rust domain and durable SQLite event catalog, with fixtures for canonical
revisions, path-independent identity, moves, rewrites, tombstones/restores, and
multiple records per session. It also has a guarded read-only filesystem scanner
with a versioned Codex rollout schema, durable scan state, guarded tombstone
grace, held-descriptor validation for borrowed source bytes, and a durable
subscription/lease ledger that preserves per-record version ordering. A bounded
path-consumer proof harness creates verified lease-scoped copies instead of
exposing mutable source paths, and demonstrates idempotent redelivery with a
fake adapter. Integrity scrub, cache operational closure, and real consumers
remain pending. The current library also exposes content-free catalog status and
SQLite-aware backup/validation/restore helpers for the durable control ledger.
See [implementation status](docs/implementation-status.md).

V2's protocol is detailed but intentionally not frozen until the shared V1.5
domain model passes its gates. The plan is staged:

- **V1 — external mirror:** a minimal, reliable raw-file mirror using an operator-provided destination.
- **V1.5 — catalog and controller:** revision-aware discovery and independent consumer cursors over the V1 mirror.
- **V2 — service-owned transport:** a self-contained archive service with a local asynchronous client and incremental uploads.

The raw archive remains canonical throughout. V1.5 combines current external
bytes with a durable metadata ledger; V2 combines committed objects and receipts.
Derived projections, materialization caches, indexes, and consumer output must
be safe to discard and rebuild.

The central evolution rule is: V1.5 establishes stable identity, revisions,
the event ledger, and consumer delivery; V2 reuses those contracts and adds the
client, upload API, canonical object store and receipts, verified payload reader,
and rebuildable current projection.

## Documents

- [Architecture](docs/architecture.md) — scope, ownership, and data flow.
- [Versioned design](docs/versioned-design.md) — V1, V1.5, and V2 decisions.
- [Domain contract](docs/domain-contract.md) — producer/session/record identity, canonical revisions, ordering, and clocks.
- [Payload and delivery contract](docs/payload-delivery-contract.md) — verified bytes, per-record ordering, settled outcomes, and replay modes.
- [Durability contract](docs/durability-contract.md) — root guards, hashing/scrub, SQLite backup, receipts, cache, and crash recovery.
- [Design review closure](docs/design-review-closure.md) — resolution and proof mapping for the ten pre-implementation gaps.
- [V1.5 design](docs/v1.5-design.md) — catalog, reconciliation, durable events, and consumer delivery.
- [V2 design](docs/v2-design.md) — asynchronous client, chunk transport, archive storage, and commit protocol.
- [Evolution boundary](docs/evolution-boundary.md) — overlap, dependencies, reuse, and cutover gates.
- [Transport contract](docs/transport-contract.md) — identities, revisions, delivery, and durability semantics.
- [Proof plan](docs/proof-plan.md) — functional and operational acceptance gates.
- [Implementation status](docs/implementation-status.md) — implemented slices and remaining proof work.

## Non-goals

- A proprietary transcript format.
- Semantic retrieval, embedding, or learned-memory policy.
- A hard-coded server, network, cloud account, or storage backend.
- Running heavyweight work inside a lifecycle hook.

## Local checks

```sh
rtk cargo fmt --check
rtk cargo clippy --all-targets -- -D warnings
rtk cargo test
rtk git diff --check
```

## License

License selection is intentionally deferred until an implementation is ready to publish.
