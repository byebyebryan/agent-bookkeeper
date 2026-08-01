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

## Remaining V1.5 slices

1. Guarded filesystem source adapter: provider identity schemas, root checks,
   path-safe enumeration, stability observation, streaming hash, scan grace,
   and integrity scrub.
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
