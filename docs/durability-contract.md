# Durability, integrity, and recovery contract

Status: shared implementation contract with phase-specific storage rules.

## Recovery classes

| State | Class |
| --- | --- |
| V1 external mirror current bytes | Operator-owned canonical current evidence. |
| V1.5 identity/revision/event ledger | Durable archive metadata; consistently backed up. |
| V2 committed objects and receipts | Canonical retained archive; backed up as one recovery set. |
| Consumer delivery state | Durable operational state; backed up for exact restore, or explicitly rebuilt with a new subscription ID. |
| Scan fingerprints, materialization cache, current projection, consumer indexes | Derived and rebuildable. |
| Upload staging without a committed receipt | Incomplete and conservatively expirable. |

## V1.5 catalog persistence

The reference catalog is SQLite in WAL mode on a local filesystem with reliable
POSIX locking, atomic rename, and `fsync`. Do not place the live SQLite database
on NFS, an object-store mount, or another filesystem whose locking and durability
semantics have not passed an explicit proof.

The operator provides a durable control directory separate from consumer index
state. Schema migrations are transactional and versioned. Readiness fails if an
unknown, partially applied, or newer schema is found.

Consistent backup uses the SQLite online-backup API or an equivalent
SQLite-aware snapshot operation. Copying live database and WAL files separately
is not a supported backup method. A backup artifact records schema version,
latest event sequence, source configuration digest, and creation time.

A restore proof must demonstrate preserved identities, record versions, event
order, tombstones, subscription state, and outstanding delivery outcomes.

## Filesystem source safety

### Root identity guard

Every V1.5 source declares a deletion mode:

- `disabled`: absence never creates a tombstone;
- `guarded`: absence may create a tombstone only after the configured root
  identity and grace policy pass.

`guarded` mode requires an expected root marker or equivalent stable storage
identity. The reconciler verifies it before enumeration and again before
committing missing-record observations. A missing, empty, replaced, or changed
root is unhealthy, not an empty archive.

Without a valid guard, Bookkeeper may observe additions and changed present
files but cannot convert absence into deletion.

### Path confinement

The filesystem adapter:

- resolves only provider-approved relative paths beneath the configured root;
- rejects absolute paths and `..` traversal;
- opens without following symlinks;
- accepts only regular files with configured size limits;
- ignores transport temporary names and unapproved file types;
- rechecks root identity during long scans.

### Tombstone grace

A guarded missing record becomes a tombstone candidate only after a complete
scan. Commit requires both a configurable elapsed grace period and a minimum
number of complete guarded scans. A later matching record cancels the candidate.

Active-to-archived movement is resolved by stable record identity before
tombstone evaluation.

## Stability, hashing, and scrub policy

Cheap reconciliation uses path, size, modification time, and available file
identity only to find candidates. It is not a permanent integrity guarantee.

For every accepted V1.5 revision:

1. require the configured quiescence window or unchanged observations;
2. open one guarded descriptor;
3. stream the complete file through canonical BLAKE3-256;
4. verify length and pre/post file metadata;
5. abandon the observation if the file changed during the read.

The default correctness path is O(record size) for every admitted changed
revision. Bookkeeper does not claim that fixed chunks or cheap metadata eliminate
this read cost.

To prevent excessive work on active large sessions, admission policy may
coalesce observations using:

- minimum quiescence time;
- minimum interval between committed revisions of one active record;
- a latest-only consumer policy;
- bounded hashing bytes/concurrency and maintenance windows.

These controls delay admission; they never fabricate an append-only guarantee.

Every source also has a bounded integrity-scrub schedule. A scrub rehashes
otherwise unchanged records over time, with a byte budget and resumable cursor,
so restored timestamps or same-size changes cannot remain invisible forever.
Scrub mismatch opens an integrity alert and normal revision reconciliation; it
does not silently rewrite history.

V2 clients follow the same correctness baseline: a changed record is streamed
completely to calculate its canonical digest and fixed chunk manifest. The first
protocol does not use an append fast path. A future provider-specific fast path
must be versioned, proven, and periodically checked by a full scrub.

## V2 canonical storage

The V2 filesystem backend contains:

```text
archive-root/
  objects/<producer>/<algorithm>/...
  receipts/
  staging/
  state/
  cache/materialized/
  projection/current/
```

- Objects are immutable, bounded, producer-scoped content-addressed chunks.
- Receipts are immutable canonical commit records.
- The online catalog indexes receipts and holds operational delivery state.
- `cache/materialized` contains leased full-file paths created on demand by a
  `RevisionReader`; it is bounded and disposable.
- `projection/current` is an asynchronous latest-state convenience/export view;
  it is rebuildable and never an eligibility or commit signal.

V2 does not persist a complete immutable reconstructed file for every revision.
Retained exact bytes are represented by ordered immutable chunks plus receipts.
This avoids full-file storage amplification across append revisions.

## V2 commit serialization

One archive writer owns an exclusive commit mutex. Uploading objects may be
concurrent, but generation publication is serialized.

A generation manifest has deterministic, versioned canonical serialization and
golden fixtures. Its digest, predecessor receipt, client epoch/generation, and
record transitions are validated before commit.

The canonical receipt embeds the accepted bounded generation manifest plus all
server-assigned archive/event sequences, event IDs, record versions,
`committed_at`, and predecessor linkage required for catalog reconstruction. Its
digest covers the versioned canonical receipt body excluding the digest field
itself. `committed_at` is sampled once under the commit mutex before publication;
receipt existence, not time, establishes commitment.

The archive writer performs:

1. Validate producer scope, predecessor receipt, limits, identity transitions,
   and every referenced object digest/length.
2. Stream the ordered objects for each new revision through canonical
   BLAKE3-256 and verify the declared whole-record digest and byte length. This
   is O(total committed revision bytes) and is admitted through a bounded server
   byte/concurrency budget.
3. Under the commit mutex, allocate the next global archive/event sequences and
   record versions, construct the receipt, and write a durable catalog
   `preparing` transaction containing its canonical body and digest.
4. Write the receipt to a same-filesystem temporary file, `fsync` the file,
   atomically rename it to its final immutable path, and `fsync` the parent
   directory. Receipt rename is the canonical commit point.
5. Finalize the catalog generation/events and create eligible delivery rows in
   one transaction.
6. Return the immutable receipt. Current projection refresh is asynchronous and
   outside the commit transaction.

No full-file projection is required to commit. Byte-exact export or path-based
consumer delivery uses the bounded materialization cache.

## Crash recovery matrix

| Durable state after restart | Recovery action |
| --- | --- |
| Objects only, no preparing row or receipt | Leave as unreferenced objects; never serve them, and do not delete them before a future canonical-GC proof. |
| Preparing row, no receipt | Return that exact request to retryable state; no consumer event exists and any reserved sequence is never reused. |
| Receipt exists, catalog missing or preparing | Verify receipt chain and objects, then idempotently finalize catalog/events. |
| Catalog says committed, receipt exists | Normal committed state. |
| Catalog says committed, receipt missing or invalid | Fail readiness as corruption; never serve the generation. |
| Receipt predecessor or global sequence conflicts | Fail readiness and require operator recovery; never guess ordering. |

Startup completes receipt/catalog reconciliation before the service becomes
ready for commits or consumer leases.

Committed archive/event sequences strictly increase but need not be gap-free;
an abandoned pre-commit reservation may leave a gap. Cursor contiguity means no
earlier delivery row remains unresolved, not arithmetic adjacency of sequence
numbers.

## Receipt and catalog reconstruction

Objects plus committed receipts reconstruct V2 archive identities, revisions,
locations, tombstones, and event order. Consumer subscriptions and delivery
outcomes may be recreated by replay.

V1.5 history predates canonical V2 receipts. Before cutover, Bookkeeper writes a
versioned `migration_epoch` bundle into the receipt recovery set containing:

- a consistent V1.5 ledger snapshot or canonical event export;
- its schema and identity-schema versions;
- latest event sequence and current record state;
- availability metadata for historical revisions;
- digest of the imported current-revision manifests.

The bundle preserves V1.5 historical metadata even when old external bytes are
unavailable. Current available revisions are chunked server-side and receive
import receipts. The online V2 catalog must rebuild from the migration epoch
plus later receipts before cutover is accepted.

## Materialization cache recovery

Cache entries are keyed by canonical revision digest and contain a completed
marker written only after length/digest verification and atomic rename. Startup
deletes incomplete temporary files. Completed unleased entries may be evicted at
any time under the byte/age policy.

Consumers never infer archive eligibility by scanning the cache or current
projection. They receive an exact leased locator from `RevisionReader` after the
archive event is committed.

## Garbage collection and retention

The initial V2 proof may expire incomplete staging and materialization cache
entries. It does not garbage-collect canonical objects or receipts.

Canonical garbage collection requires a selected retention/deletion policy and
a mark phase over every committed receipt, migration epoch, retained revision,
and active staging manifest. Cross-producer physical deduplication remains
disabled in the initial logical model.
