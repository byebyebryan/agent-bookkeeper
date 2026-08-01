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

- Provider fixtures preserve stable session/record identity across moves and
  distinguish multiple record kinds/keys within one session.
- V1.5 and V2 fixtures calculate the same canonical BLAKE3-256 revision key for
  identical bytes; secondary digests do not affect identity.
- A metadata-only scan has bounded cost over a steady archive.
- Only changed, stable files are rehashed and queued.
- A source that changes during hashing is retried and never committed partially.
- A missing or wrong source mount blocks reconciliation without emitting bulk tombstones.
- A source without a root identity guard has deletion detection disabled.
- Time-and-scan tombstone grace survives transient absence and active/archive moves.
- A rename or path move preserves record identity and avoids duplicate downstream ingest.
- A rewrite or truncation creates exactly one new revision.
- A borrowed V1.5 file that advances before consumer delivery fails revision
  validation and follows the configured supersede/block policy without being
  acknowledged as the older bytes.
- Replacing the mutable source pathname during delivery does not change the
  bytes read from the held descriptor; a path-only consumer receives a verified,
  lease-scoped materialization rather than the mutable source path.
- Duplicate identities and conflicting live revisions stop in an explicit conflict state.
- Lease expiry and injected consumer failure redeliver at least once without duplicate derived effects from an idempotent adapter.
- Later record versions never pass an earlier unresolved ordering barrier; tombstones
  and restores cannot be undone by a delayed retry.
- `acknowledged`, `superseded`, `ignored_by_policy`, and `dead_lettered` outcomes
  advance or block cursors exactly as documented.
- Each consumer can be paused independently and replay from a selected event sequence.
- A new subscription epoch rebuilds a reset consumer without reusing prior
  idempotency acknowledgements.
- `rebuild_current` succeeds from current V1.5 bytes; historical byte replay
  explicitly blocks or supersedes unavailable revisions.
- Backpressure limits consumer work so a discovery sweep does not create an uncontrolled embedding or indexing burst.
- Quiescence/revision coalescing bounds repeated hashing of one active large
  record, while a byte-budgeted scrub eventually detects same-size drift.
- SQLite-aware backup and restore on the supported filesystem preserve event
  order, identities, tombstones, subscriptions, and outstanding deliveries.

## V2 gates

- Interrupted bounded-object uploads retry without re-sending already accepted objects.
- A new client can seed a large archive without exhausting memory or blocking agent hooks.
- Client-state loss recovers through a server-authorized new epoch chained to
  the latest receipt; prior generation IDs are never guessed or reused.
- An append normally sends only a replacement tail chunk plus new fixed chunks;
  full client/server read and hashing cost is measured and bounded separately.
- Append, rewrite, and truncation cases all stream byte-exact payloads through
  `RevisionReader`; the asynchronous current projection eventually converges to
  the latest committed state.
- Repeated requests, out-of-order delivery, and service restarts are idempotent.
- Crash injection around object publication, preparing transaction, receipt
  file/parent `fsync`, receipt rename, and catalog finalization recovers to one
  committed or one uncommitted result.
- Receipt golden fixtures alone retain the accepted manifest, record versions,
  event IDs/sequences, predecessor chain, and commit metadata needed to rebuild
  the online catalog.
- A committed-catalog/missing-receipt state fails readiness rather than being
  guessed healthy.
- Producer-scoped authorization rejects paths, identities, and generation commits outside the assigned scope.
- Clients require only the service API; they have no direct access to the service's raw-storage backend.
- Export and recovery can rebuild catalogs and downstream consumer state from the canonical raw archive.
- Repeated append revisions retain chunks/receipts without permanently storing
  one reconstructed full file per revision; the materialization cache remains
  within its configured byte and lease limits.
- A server-side V1.5 seed avoids workstation retransmission and produces equivalent canonical record revisions and consumer events.
- A migration-epoch bundle plus later receipts rebuilds V1.5 metadata history,
  current record state, V2 catalog order, and payload-availability classifications.
- The mirror-to-API authority handoff has no dual-write interval and has a rehearsed rollback.

## Promotion rule

Do not call a phase ready because it transferred files once. Promote only after its failure, recovery, resource, and provenance gates pass against representative long-running sessions.
