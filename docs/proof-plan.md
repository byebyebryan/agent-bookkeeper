# Proof plan

Bookkeeper should be implemented behind measurable acceptance gates, not only a happy-path demo.

## V1 gates

- A hook returns promptly while a detached worker performs delivery.
- A client that is offline at its normal trigger leaves durable pending state and later succeeds without manual repair.
- A later trigger retries existing pending work even when no new session exists.
- A large existing session seeds successfully, and a small append transfers only the incremental change where the selected transport supports it.
- The destination preserves byte-exact source files and does not expose partially copied files as completed records.
- Source root restrictions prevent delivery outside the declared archive scope.

Record elapsed time, bytes sent, CPU time, and peak memory for the seed and append cases. Measure concurrent clients as well as a single client; the useful result is an operational envelope, not a benchmark headline.

## V1.5 gates

- A metadata-only scan has bounded cost over a steady archive.
- Only changed, stable files are rehashed and queued.
- A source that changes during hashing is retried and never committed partially.
- A missing or wrong source mount blocks reconciliation without emitting bulk tombstones.
- A rename or path move preserves record identity and avoids duplicate downstream ingest.
- A rewrite or truncation creates exactly one new revision.
- A borrowed V1.5 file that advances before consumer delivery fails revision
  validation and follows the configured supersede/block policy without being
  acknowledged as the older bytes.
- Duplicate identities and conflicting live revisions stop in an explicit conflict state.
- Lease expiry and injected consumer failure redeliver at least once without duplicate derived effects from an idempotent adapter.
- Each consumer can be paused independently and replay from a selected event sequence.
- Backpressure limits consumer work so a discovery sweep does not create an uncontrolled embedding or indexing burst.
- Backup and restore of the durable ledger preserve event order, identities, tombstones, and outstanding deliveries.

## V2 gates

- Interrupted bounded-object uploads retry without re-sending already accepted objects.
- A new client can seed a large archive without exhausting memory or blocking agent hooks.
- An append normally sends only a replacement tail chunk plus new fixed chunks; measured overhead is recorded.
- Append, rewrite, and truncation cases all materialize a byte-exact raw projection after commit.
- Repeated requests, out-of-order delivery, and service restarts are idempotent.
- Crash injection around object publication, projection rename, receipt publication, and catalog commit recovers to one committed or one uncommitted result.
- Producer-scoped authorization rejects paths, identities, and generation commits outside the assigned scope.
- Clients require only the service API; they have no direct access to the service's raw-storage backend.
- Export and recovery can rebuild catalogs and downstream consumer state from the canonical raw archive.
- A server-side V1.5 seed avoids workstation retransmission and produces equivalent record revisions and consumer events.
- The mirror-to-API authority handoff has no dual-write interval and has a rehearsed rollback.

## Promotion rule

Do not call a phase ready because it transferred files once. Promote only after its failure, recovery, resource, and provenance gates pass against representative long-running sessions.
