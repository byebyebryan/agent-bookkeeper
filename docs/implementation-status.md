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

Codex's local rollout layout is treated as a locally observed, versioned source
schema—not a public Codex storage protocol. An unexpected layout or metadata
mismatch is rejected rather than guessed.

## Remaining V1.5 slices

1. Finish source durability: persist scan fingerprints and source generations in
   the catalog, root-guarded tombstone grace, active/archive move handling, and
   a byte-budgeted integrity scrub. The current stability observation is
   intentionally process-local and is not yet promotion-ready.
2. Payload and delivery: held-descriptor `RevisionReader`, bounded
   materialization for path-only consumers, subscriptions, leases, per-record
   ordering, outcomes, replay, and an idempotent fake consumer.
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
