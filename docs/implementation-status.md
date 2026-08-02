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
- one admitted hash per stable fingerprint rather than rehashing every later
  unchanged scan; a changed fingerprint becomes hash-pending again;
- pre/post descriptor fingerprints, full streaming BLAKE3 hashing, duplicate
  identity rejection before commit, and change-during-hash rejection.
- durable source registration, scan generations, and fingerprint observations;
- source-to-record presence tracking across roots and scanner restarts;
- deletion disabled by default, with guarded deletion requiring root checks at
  both scan boundaries plus complete-scan and elapsed-time grace;
- stale-scan rejection, so a superseded scan cannot later overwrite presence
  state or emit a delayed tombstone.
- an optional per-scan full-hash byte budget, with deferred stable candidates
  remaining hash-pending for a later scan instead of being forgotten.

Codex's local rollout layout is treated as a locally observed, versioned source
schema—not a public Codex storage protocol. An unexpected layout or metadata
mismatch is rejected rather than guessed.

## Implemented in progress: resumable integrity scrub

Each registered source now has a durable logical-location cursor, completed
cycle counter, and last-completion time. An explicit scrub re-enumerates
provider-approved current candidates and rehashes them under a byte budget. A
single oversized first record is admitted so a large session cannot starve
forever; later candidates resume next time from the persisted cursor.

The scrub opens the same guarded descriptor pattern as reconciliation and only
updates a record already owned by that source. It does not discover new records,
infer missing records, or turn a partial scrub into deletion evidence. A scrub
rewrite therefore follows the normal revision/event path, while a changing file
is skipped until a later cycle. Fixtures cover same-size drift and a zero-budget
resume boundary.

## Implemented in progress: controlled-run measurements

Reusable measurement helpers wrap ordinary reconciliation and integrity scrubs.
They report elapsed time, bytes hashed, derived hash throughput, process user/
system CPU deltas, and process high-water resident memory where the host
provides it (Linux `ru_maxrss` is normalized to bytes). The memory value is a
process-lifetime high-water, not a per-run allocation claim.

The helper deliberately does not publish a benchmark headline. A deployment
still needs to record its own representative seed, append/rewrite, scrub, and
consumer-cohort envelopes under the selected limits and hardware.

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
consumer is ready. Consumer policy beyond capability declarations and a real
deployment adapter remain deliberately outside this slice.

## Implemented in progress: bounded path-consumer proof

The reference controller now provides a deliberately synchronous, explicit
controlled-run helper rather than a background mining worker:

- a subscription can be paused/resumed durably and a replay epoch can begin
  strictly after a selected archive event sequence;
- each run has a delivery count, payload-byte, and lease-duration bound;
- byte-bearing deliveries resolve only a configured logical root, then stream
  into a unique, read-only, lease-scoped materialization cache entry;
- the source pathname is never exposed to a path-only adapter;
- unavailable or changed external bytes, adapter failures, and byte-budget
  deferral return the delivery to `queued` and are represented in the run
  report; one run stops after a retry to avoid a tight failure loop;
- an idempotent fake adapter proves a durable external effect is not duplicated
  when the acknowledgement is lost and the same event is redelivered.

The cache is derived controller state. It has per-entry and active entry/byte
admission limits, holds an exclusive advisory owner lock, and reclaims only its
recognized lease/partial filenames at next controlled startup. It does not yet
have a reusable shared-entry index. The controller is a proof harness, not a
daemon, schedule, or real archive adapter.

## Implemented in progress: durable retry admission

Subscriptions now persist a retry limit plus initial and capped maximum backoff.
An explicit retry or lease expiry keeps its `(subscription_id, event_id)`
identity, becomes leaseable only after the capped exponential delay, and
transitions to a durable dead letter once the configured attempt limit is
reached. Dead letters retain their existing per-record ordering-barrier behavior.

For mutable V1.5 source bytes, a controlled path run now selects either retry
or a conservative durable `blocked` outcome when verification fails. Blocking
is an explicit non-success and retains the ordering barrier; automatic
supersession and metadata-only adapters remain later consumer-policy work.

## Implemented in progress: status and recovery-set proof

The library exposes a content-free catalog status snapshot with archive event
position, active/tombstoned record counts, revisions, per-source scan progress,
tombstone candidates, and per-subscription delivery/age/acknowledgement state.

It also creates SQLite-online-backup recovery sets rather than copying a live
database and WAL independently. A set consists of a new SQLite artifact plus a
durably written adjacent manifest containing the schema version, latest event
sequence, creation time, and a caller-supplied digest of the reviewed deployment
source configuration. Validation compares manifest and read-only SQLite state;
restore validates first and writes only to a new destination. The automated
restore fixture preserves an outstanding leased delivery as well as identity,
revision, and event state.

## Remaining V1.5 slices

1. Finish source operational proof: recorded representative large-record
   measurements and source health/readiness integration for deployment.
2. Finish payload and delivery: consumer filters, explicit unavailable-revision
   policy, and a real adapter boundary.
3. Operational proof: a controlled evidence-consumer cohort.

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
