# Design review closure

Status: architecture closure record. These gaps are resolved in the normative
design; implementation still has to pass the linked proof gates.

| # | Gap | Normative resolution | Proof obligation |
| --- | --- | --- | --- |
| 1 | V2 full-file-per-revision storage amplification | Canonical history is immutable bounded objects plus receipts. Full-file materializations are bounded leased cache entries, and the ordinary-file projection contains only current state. | Repeated append revisions must not retain a reconstructed full file per revision; cache byte/lease limits and exact reconstruction are tested. |
| 2 | V1.5 mutable-path TOCTOU | `RevisionReader` opens one confined, no-follow descriptor, retains it through delivery, and validates length/digest from that same descriptor. Path-only adapters receive a verified lease-scoped materialization. | Replace or mutate the source pathname during a lease and prove wrong bytes are never acknowledged. |
| 3 | Consumer ordering and incomplete outcomes | Deliveries are strictly ordered per subscription and record, parallel across records, and idempotent by `(subscription_id, event_id)`. Acknowledged/superseded/ignored outcomes advance; a dead letter is settled but remains an ordering barrier pending operator action. | Inject retries, lease loss, dead letters, tombstones, restores, and stale attempts without reordering or resurrection. |
| 4 | Unpinned V1.5/V2 revision identity | Both phases use complete-record BLAKE3-256 plus byte length as the canonical revision key. Secondary digests never change identity. | Golden fixtures must produce the same revision key through filesystem and chunk-manifest readers. |
| 5 | Receipt/catalog crash authority | One writer serializes commit. Durable `preparing` state precedes a self-contained immutable receipt published by file and parent-directory `fsync`; receipt rename is the commit point. The receipt carries the accepted manifest and assigned record/event order. Startup reconciles receipts before readiness and refuses committed-catalog/missing-receipt corruption. | Inject crashes at every publication boundary and rebuild the catalog from receipts plus the migration epoch. |
| 6 | Unsafe optional deletion guard | Each source selects `disabled` or `guarded` deletion. Guarded mode requires root identity checks at scan start and end plus elapsed-time and complete-scan grace; unguarded absence never creates tombstones. | Wrong, missing, replaced, or transient roots must not emit deletion events. |
| 7 | One record assumed per session | Stable identity is `(producer_id, agent_namespace, session_id, record_kind, record_key)`, interpreted by a versioned provider identity schema. | Fixtures cover a primary transcript, multiple session records, moves, duplicates, and schema migration/rejection. |
| 8 | Ambiguous replay promise | `rebuild_current` snapshots latest available state. `replay_events` replays ordered history but can supply historical bytes only when payloads are retained; unavailable V1.5 bytes follow explicit block, supersede, or metadata-only policy. A rebuild uses a new subscription ID. | Reset consumers under both modes and verify provenance, availability handling, and fresh idempotency scope. |
| 9 | Unbounded large-file hashing and scrub behavior | Correctness requires a full streaming hash of each admitted changed revision. Quiescence, minimum revision intervals, concurrency/byte budgets, and maintenance windows coalesce work; a resumable byte-budgeted scrub detects metadata-preserving drift. | Measure full read/hash cost on representative large records and prove bounded coalescing plus eventual scrub coverage. |
| 10 | SQLite, producer lifecycle, and clock authority left implicit | Live SQLite requires proven local locking/rename/`fsync`; backups use a SQLite-aware snapshot and restore test. Producer identity is stable across credential rotation; V2 client-state recovery creates an authorized new epoch chained to the latest receipt. Server sequences, not client clocks, define order. | Prove backup/restore, credential rotation, client-state loss, epoch replay rejection, and skewed-clock ordering. |

## Normative documents

- [Domain contract](domain-contract.md): identity, revisions, generations, and
  clock authority.
- [Payload and delivery contract](payload-delivery-contract.md): verified byte
  access, ordering, outcomes, subscriptions, and replay modes.
- [Durability contract](durability-contract.md): source safety, resource policy,
  SQLite recovery, canonical V2 storage, and commit crash behavior.
- [Proof plan](proof-plan.md): phase acceptance gates.

Closing a row here means the design no longer leaves the choice implicit. It
does not mean the implementation has passed that row's proof obligation.
