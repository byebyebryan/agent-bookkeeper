# Implementation status

Status: active V1.5 proof. This document distinguishes executable behavior from
the intended contracts; it is not a promotion claim.

## Implemented: Slice 1 core and catalog

The Rust crate currently provides:

- strong types for producer, record, revision, and event identities;
- the five-part record identity and path-confined logical location;
- streaming canonical BLAKE3-256 revision hashing;
- a SQLite WAL catalog with transactional schema migration and foreign keys;
- transactional record version allocation and globally ordered event rows;
- idempotent unchanged observation, move, rewrite, tombstone, and restore
  transitions;
- fixtures for path-independent identity and multiple records in one session.

The implementation does **not** yet claim a completed V1.5 deployment. In
particular, no source adapter has been allowed to infer a deletion, no raw path
has been given to a consumer, and no external consumer has been started.

## Implemented in progress: guarded filesystem discovery

The read-only source foundation provides:

- absolute configured roots with no-follow component traversal on Unix;
- optional marker guards checked before enumeration and immediately before
  catalog commit;
- provider-level filename filtering and a versioned Codex rollout schema;
- cross-checking of the rollout filename UUID with `session_meta.payload.id`;
- two unchanged observations before a candidate is hashed;
- pre/post descriptor fingerprints, full streaming BLAKE3 hashing, duplicate
  identity rejection before commit, and change-during-hash rejection.
- durable source registration, scan generations, and fingerprint observations;
- source-to-record presence tracking across roots and scanner restarts;
- deletion disabled by default, with guarded deletion requiring root checks at
  both scan boundaries plus complete-scan and elapsed-time grace;
- stale-scan rejection, so a superseded scan cannot later overwrite presence
  state or emit a delayed tombstone.

Codex's local rollout layout is treated as a locally observed, versioned source
schema—not a public Codex storage protocol. An unexpected layout or metadata
mismatch is rejected rather than guessed.

## Implemented in progress: verified external payload reader

`CurrentExternalRevision` resolves a configured root-relative file through a
held no-follow descriptor. It streams the same descriptor it opened, checks its
complete canonical revision before delivery success, and rejects length,
metadata, or digest changes. An atomic pathname replacement after open therefore
does not substitute a new payload; an in-place mutation fails validation.

## Implemented in progress: durable subscription delivery

The catalog now materializes one delivery ledger per subscription epoch:

- `replay_events` copies the ordered archive-event ledger, while
  `rebuild_current` creates fresh snapshot deliveries for active records;
- each `(subscription_id, event_id)` appears once, so a retry preserves the
  external adapter idempotency key;
- bounded lease admission permits concurrency across records but never lets a
  later record version pass an unresolved earlier version;
- acknowledgements, explicit advancing outcomes, retries, expiry, blocked
  capabilities, and dead letters are durable states; a dead letter remains an
  ordering barrier;
- a lease cannot be acknowledged or retried after its expiry, even if the next
  scheduler pass has not yet run.

This is the durable queue and lease core, not an assertion that an external
consumer is ready. Materialization, consumer policy, pause/resume control, and
an adapter run loop remain deliberately outside this slice.

## Remaining V1.5 slices

1. Finish source operational proof: a resumable byte-budgeted integrity scrub,
   scan/source health status, source-level resource limits, and representative
   large-record measurements.
2. Finish payload and delivery: bounded materialization for path-only consumers,
   consumer policy and pause/resume, an idempotent fake consumer, and a
   controlled adapter run loop.
3. Operational proof: status, SQLite-aware backup/restore, resource controls,
   representative large-record measurements, and a controlled evidence-consumer
   cohort.

## Validation

Run from the repository root:

```sh
rtk cargo fmt --check
rtk cargo clippy --all-targets -- -D warnings
rtk cargo test
rtk git diff --check
```

The acceptance criteria remain [proof-plan.md](proof-plan.md); a slice becomes
promotion evidence only after its corresponding failure, recovery, resource,
and provenance gates pass.
