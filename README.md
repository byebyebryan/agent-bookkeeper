# Agent Bookkeeper

Agent Bookkeeper is a reusable data plane for preserving raw agent-session evidence: local discovery, reliable delivery, durable storage, and revision-aware cataloging. It gives memory and analysis systems one trustworthy, replayable source of records without coupling them to an individual workstation or storage topology.

It complements, rather than replaces, an overall memory system such as [Agent Historian](https://github.com/byebyebryan/agent-historian):

- **Bookkeeper** records what happened and makes the raw evidence durably available.
- An archive or retrieval consumer turns those records into searchable evidence.
- A learned-memory consumer may derive concise, fallible working context from that evidence.

## Design status

The design is ready to begin a V1.5 implementation proof; V2's protocol is
detailed but intentionally not frozen until the shared V1.5 domain model passes
its gates. The plan is staged:

- **V1 — external mirror:** a minimal, reliable raw-file mirror using an operator-provided destination.
- **V1.5 — catalog and controller:** revision-aware discovery and independent consumer cursors over the V1 mirror.
- **V2 — service-owned transport:** a self-contained archive service with a local asynchronous client and incremental uploads.

The raw archive remains canonical throughout. Derived indexes and consumer output must be safe to discard and rebuild from it.

The central evolution rule is: V1.5 establishes stable identity, revisions,
the event ledger, and consumer delivery; V2 reuses those contracts and adds the
client, upload API, canonical object store, and raw projection.

## Documents

- [Architecture](docs/architecture.md) — scope, ownership, and data flow.
- [Versioned design](docs/versioned-design.md) — V1, V1.5, and V2 decisions.
- [V1.5 design](docs/v1.5-design.md) — catalog, reconciliation, durable events, and consumer delivery.
- [V2 design](docs/v2-design.md) — asynchronous client, chunk transport, archive storage, and commit protocol.
- [Evolution boundary](docs/evolution-boundary.md) — overlap, dependencies, reuse, and cutover gates.
- [Transport contract](docs/transport-contract.md) — identities, revisions, delivery, and durability semantics.
- [Proof plan](docs/proof-plan.md) — functional and operational acceptance gates.

## Non-goals

- A proprietary transcript format.
- Semantic retrieval, embedding, or learned-memory policy.
- A hard-coded server, network, cloud account, or storage backend.
- Running heavyweight work inside a lifecycle hook.

## Local checks

```sh
rtk git diff --check
```

## License

License selection is intentionally deferred until an implementation is ready to publish.
