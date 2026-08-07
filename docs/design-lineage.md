# Design lineage

This document maps the pre-repository "Agent Session Archive V2" proposal to
the current Agent Bookkeeper design. It preserves why the project evolved
without competing with the normative contracts in this repository.

For current behavior, start with [architecture](architecture.md),
[versioned design](versioned-design.md), and
[implementation status](implementation-status.md).

## Origin

Transcript transport began as a small operational feature for one search
backend: a lifecycle hook started an asynchronous rsync reconciliation into an
operator-provided raw directory. The design became a separate product when the
same raw records needed stable identity, revisions, controlled backfill,
independent consumers, and rebuildable projections.

The early proposal used "Agent Session Archive" as a working name. Agent
Bookkeeper became the project name because it describes the broader data-plane
responsibility: record what happened, preserve the evidence, account for
revisions and delivery, and make the archive available to retrieval consumers.
It does not claim to author learned memory.

## Decisions carried forward

### Hooks wake work; they do not perform it

A lifecycle hook must return promptly. It may create durable pending state and
wake a short-lived worker, but network transfer, hashing, parsing, embedding,
and retry do not run inside the hook. A later lifecycle event may reconcile
old pending work; no permanent workstation daemon is required by the design.

This principle is retained in the [versioned design](versioned-design.md) and
[transport contract](transport-contract.md).

### Raw evidence is canonical

Source bytes are preserved verbatim. Catalogs, materialization caches,
ordinary-file projections, search indexes, embeddings, and consumer receipts
are derived or reconstructable according to their documented durability
boundary. Search success never authorizes deletion of raw evidence.

The current rules are in the
[durability contract](durability-contract.md) and
[payload and delivery contract](payload-delivery-contract.md).

### Identity is not a path

A session moving between active and archived roots is a location change, not a
new logical record. Rewrites, truncation, replacement, tombstones, and restore
must produce explicit revision events. Stable producer, agent, session, record,
and revision identities are defined in the
[domain contract](domain-contract.md).

This corrected an early filesystem-oriented assumption that a consumer could
use the source path as its durable identity.

### Consumers advance independently

Capture and archive durability must not depend on one embedding backend.
Consumers receive bounded, verified payloads and keep independent cursors,
leases, outcomes, and replay state. A slow, unavailable, or rebuilt consumer
does not block another consumer or source transport.

The current V1.5 proof implements this as durable catalog events and
lease-scoped verified copies. V2 is expected to reuse the same record and
delivery semantics.

### Transport evolves without replacing the domain

The early proposal separated three stages that remain the current roadmap:

```text
V1    external exact-byte mirror
V1.5 revision-aware catalog and consumer controller over that mirror
V2    service-owned asynchronous client, archive API, objects, and receipts
```

V1.5 deliberately proves identity, reconciliation, delivery ordering, and
recovery before V2 adds an upload protocol. The mapping and cutover conditions
are maintained in the [evolution boundary](evolution-boundary.md).

### V2 commits complete revisions

The proposed V2 client keeps a durable local queue, discovers exact source
bytes, uploads bounded missing data asynchronously, and commits a revision only
after the server can verify and reconstruct it. Content-addressed chunks,
resumable upload, atomic receipts, and a consumer-facing projection remain the
preferred direction, subject to the proof gates in
[V2 design](v2-design.md).

The service must handle append, truncate, rewrite, retry, duplicate requests,
archive moves, and deletion explicitly. It must never expose partially
committed content to consumers.

## Decisions deliberately changed

### Bookkeeper is the archive-search product

The earliest design described MemPalace and Hindsight as sibling consumers.
The current boundary is narrower and clearer:

- MemPalace is an archive-search backend integrated by Bookkeeper.
- Learned-context authoring is outside Bookkeeper and belongs to an optional
  Agent Historian workflow.
- Hindsight is not a planned automatic transcript consumer.

This does not prevent a future consumer adapter. It prevents a trial product
from shaping the canonical archive contract prematurely.

### Deployment topology is operator supplied

The pre-repository notes included one deployment's hostnames, accounts,
filesystem paths, snapshot tiers, SSH receiver, and container layout. Those
facts belong in that deployment's private operations repository.

The reusable contract says only that V1 receives an operator-provided durable
destination and V2 receives operator-provided storage, identity, and network
configuration. Bookkeeper does not hard-code a server, LAN name, mount path,
or storage appliance.

### Rsync completion is not consumer ingestion

V1 transport ends when the exact-byte mirror succeeds. It does not imply
catalog reconciliation, MemPalace ingestion, or a completed consumer cursor.
Those are separate observable stages. The current V1.5 controller is an
explicit one-shot bounded cohort, not an automatic post-rsync worker.

### Source identity remains an open integration gate

The Bookkeeper MemPalace adapter currently provides a catalog-local stable
record source ID. Direct backend batch import may derive a different identity
from a raw path. A clean wholesale rebuild therefore still needs one shared,
deterministic source-identity contract and a proven new-consumer/replay flow.

Preserving an old derived index is not a reason to freeze an awkward identity.
Raw evidence is canonical, so a clean candidate can be rebuilt after that
contract is accepted.

## Alternatives retained as design evidence

The early transport research compared:

- rsync over restricted SSH for a simple exact-byte V1 mirror;
- Syncthing for general block-level replication and offline convergence;
- rdiff-backup or a separate snapshot system for historical recovery;
- tus-style resumable uploads;
- librsync signatures and deltas; and
- content-addressed chunking for a service-owned V2 protocol.

No library alone supplies Bookkeeper's complete contract. Resumable upload
does not define logical revisions; delta transfer does not define atomic
commit; file synchronization does not define independent consumer cursors.
Those tools remain implementation candidates beneath the domain and durability
contracts.

## Document map

| Original concern | Current authority |
| --- | --- |
| Product scope and ownership | [Architecture](architecture.md) |
| V1, V1.5, and V2 sequence | [Versioned design](versioned-design.md) |
| Session, record, revision, and move identity | [Domain contract](domain-contract.md) |
| Verified bytes, leases, outcomes, and replay | [Payload and delivery contract](payload-delivery-contract.md) |
| Hashing, SQLite, cache, receipt, and recovery rules | [Durability contract](durability-contract.md) |
| V1.5 scanner, catalog, and controller | [V1.5 design](v1.5-design.md) |
| V2 client, upload, commit, and projection | [V2 design](v2-design.md) |
| Cross-version reuse and cutover | [Evolution boundary](evolution-boundary.md) |
| Acceptance and failure proofs | [Proof plan](proof-plan.md) |
| Implemented versus proposed behavior | [Implementation status](implementation-status.md) |

Future design changes should update the normative document first. Update this
lineage only when a major premise or ownership boundary changes.
