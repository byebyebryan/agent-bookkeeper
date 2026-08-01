# Agent Bookkeeper

Agent Bookkeeper is a reusable data plane for preserving raw agent-session evidence: local discovery, reliable delivery, durable storage, and revision-aware cataloging. It gives memory and analysis systems one trustworthy, replayable source of records without coupling them to an individual workstation or storage topology.

It complements, rather than replaces, an overall memory system such as [Agent Historian](https://github.com/byebyebryan/agent-historian):

- **Bookkeeper** records what happened and makes the raw evidence durably available.
- An archive or retrieval consumer turns those records into searchable evidence.
- A learned-memory consumer may derive concise, fallible working context from that evidence.

## Design status

This is a design bootstrap. The versioned plan is intentionally staged:

- **V1 — external mirror:** a minimal, reliable raw-file mirror using an operator-provided destination.
- **V1.5 — catalog and controller:** revision-aware discovery and independent consumer cursors over the V1 mirror.
- **V2 — service-owned transport:** a self-contained archive service with a local asynchronous client and incremental uploads.

The raw archive remains canonical throughout. Derived indexes and consumer output must be safe to discard and rebuild from it.

## Documents

- [Architecture](docs/architecture.md) — scope, ownership, and data flow.
- [Versioned design](docs/versioned-design.md) — V1, V1.5, and V2 decisions.
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
