# V2 service-owned transport and archive storage

Status: detailed design proposal. Protocol details remain subject to a bounded
proof before compatibility is promised.

## Outcome

V2 replaces direct client writes to an operator-managed mirror with an
authenticated archive service and asynchronous local client. The service owns
accepted revisions, durable raw objects and receipts, commits, and verified
payload access. Its ordinary-file current projection is rebuildable derived
state. V1.5 identity, ledger, event, delivery, and consumer behavior remain
unchanged.

```text
agent lifecycle hook
        |
        v
local Bookkeeper client -> durable local queue -> scan/hash/chunk worker
                                                    |
                                                    v
                                    authenticated archive API
                                                    |
                                      missing chunks + staging
                                                    |
                                          generation commit
                                                    |
                         +--------------------------+------------------+
                         v                                             v
              canonical objects/receipts               rebuildable current projection
                         |                                             |
                         +------------------> V1.5 ledger/events ------+
                                                    |
                                             consumer delivery
```

## What V2 adds

- A device-scoped client identity and authorization boundary.
- A local durable client queue and acknowledged source state.
- Incremental, idempotent byte upload over an API.
- Service-owned canonical raw object storage and commit receipts.
- Verified streaming and bounded on-demand materialization for consumers.
- A rebuildable asynchronous current-file projection for compatibility/export.
- Native client-declared generations and source-to-archive lag status.

V2 does not replace record identity, revision semantics, archive events,
consumer queues, backpressure, replay, or consumer adapters established in
V1.5.

## Deployment assumptions

The reference V2 is a single-writer archive service backed by an
operator-provided durable filesystem. It can be packaged as one Docker service,
but its storage is not assumed to be a Docker volume. Future object-store
backends may be added behind the same archive-store interface.

The first release does not target horizontally concurrent archive writers.
Avoiding distributed transactions keeps the commit and recovery model small and
inspectable for a personal or small-team deployment.

## Local client

The client is a normal executable invoked by provider lifecycle hooks. The hook
only records pending work and asks a detached worker to run; it never scans large
files or waits for the network.

Client state uses a local durable database or equivalent journal containing:

- stable `producer_id` and client instance identity;
- current random `client_epoch` and its server-authorized predecessor receipt;
- configured provider roots and layout plugins;
- pending scan triggers and last completed scan;
- observed cheap fingerprints and accepted local revisions;
- chunk manifests and upload acknowledgements;
- monotonically increasing client generation IDs;
- commit receipts and last server acknowledgement;
- retry/backoff and last error state.

Credential rotation does not change the producer or client epoch. If local state
is lost, an administrator-authorized recovery enrolls a new client epoch chained
to the server's latest producer receipt; it never guesses a generation number in
the lost epoch. The lifecycle is normative in
[domain-contract.md](domain-contract.md).

Every later trigger first resumes pending work. A permanent daemon or periodic
timer is optional; eventual retry on a later lifecycle event remains supported.
An explicit status command may inspect local state without creating an archive
event.

## Chunk and delta strategy

The initial protocol uses bounded, content-addressed fixed-size chunks rather
than introducing tus, librsync, or content-defined chunking as required
dependencies.

The initial proof profile is a concrete versioned protocol input:

```text
chunking: fixed-v1
target size: 4 MiB
chunk digest: BLAKE3-256
whole-revision digest: algorithm-tagged BLAKE3-256
```

The final chunk may be shorter. Appending to a record changes at most its prior
partial tail chunk plus newly added chunks. Mid-file insertion can shift and
re-upload the following fixed chunks; that is acceptable for the first proof
because agent transcripts are expected to be append-dominant and rewrites are
handled correctly even when less efficiently.

Fixed chunks reduce network transfer, not necessarily client read cost. The
initial correctness path streams every admitted changed record completely to
compute canonical BLAKE3-256 and its chunk manifest. Quiescence, minimum revision
intervals, byte budgets, and controlled maintenance windows coalesce active
large files. No append-only shortcut is assumed in the first protocol.

The digest algorithm and chunking profile are explicit in every manifest. They
must never be changed in place under an existing profile name.

### Why not require larger transfer systems initially?

| Primitive | What it solves | Initial decision |
| --- | --- | --- |
| [tus](https://tus.io/protocols/resumable-upload) | Resuming an offset-based HTTP upload after interruption. | Defer. A failed 4 MiB object can be retried idempotently without a second upload state machine. Reconsider for much larger objects or a whole-file backend. |
| [librsync](https://librsync.github.io/page_formats.html) | Producing a delta against an exact basis signature. | Defer. It adds basis selection, signature exchange, patch application, and basis validation while content-addressed chunks already handle append deltas. |
| [FastCDC](https://www.usenix.org/conference/atc16/technical-sessions/presentation/xia) | Preserving deduplication boundaries when content shifts. | Keep as a future versioned chunking profile if measured rewrites make fixed chunks too expensive. |
| [BLAKE3](https://github.com/BLAKE3-team/BLAKE3) | Fast streaming cryptographic content addressing. | Use for the initial object and whole-revision digest, stored with an algorithm tag. |

This is not a claim that those systems are unsuitable. It keeps the proof
focused on Bookkeeper's archive semantics instead of integrating several
overlapping transfer state machines at once.

## Archive storage layout

A filesystem backend has six logical areas:

```text
archive-root/
  objects/       # immutable producer-scoped content-addressed raw-byte chunks
  receipts/      # immutable accepted revision and generation receipts
  staging/       # incomplete uploads and receipt-publication work
  cache/         # bounded leased materializations for path-only consumers
  projection/    # rebuildable current convenience/export view
  state/         # online catalog, delivery queues, migrations, and locks
```

Recovery classes are:

| State | Recovery class |
| --- | --- |
| Objects and committed receipts | Canonical archive; durable and backed up together. |
| Online catalog/event index | Durable online state, rebuildable from committed receipts plus migrations. |
| Projection | Rebuildable from objects and receipts. |
| Materialization cache | Bounded derived state; removable when unleased. |
| Incomplete staging | Disposable after a conservative expiry and reconciliation. |
| Consumer delivery state | Durable operational state; backed up for exact restore, or explicitly rebuilt as a new subscription. |

Consumers never infer eligibility by scanning `objects`, `staging`, cache, or
projection. They receive a committed `RevisionReader`; stream consumers read
ordered chunks directly, while path-only consumers receive a leased verified
cache entry. The read-only current projection is compatibility/export state.

Initial object presence is logically producer-scoped even when a backend can
deduplicate physical bytes. A client must not learn whether another producer
has uploaded a particular digest. Cross-producer deduplication and its content-
existence privacy tradeoff remain deferred.

## Manifest model

A revision manifest contains:

- protocol and chunking profile versions;
- producer, agent namespace, and stable session identity;
- record kind and record key;
- logical root role and source-relative location;
- whole-revision digest and byte length;
- ordered chunk digest/length pairs;
- source observation time and provider-neutral metadata;
- optional predecessor revision for diagnostics, never for correctness.

A generation manifest contains:

- producer, client epoch, and client generation ID;
- previous accepted generation receipt, if any;
- the complete set of revision/location changes and tombstones in the client
  transaction;
- a digest of the canonical manifest representation.

The tuple `(producer_id, client_epoch, client_generation_id)` is unique.
Repeating it with the same manifest is an idempotent retry; repeating it with
different content is a conflict and never overwrites the original request.

The immutable commit receipt contains enough accepted server state to rebuild
the catalog without trusting a surviving online database:

- receipt format version and the complete accepted generation manifest;
- request key, predecessor receipt digest, and generation-manifest digest;
- server-assigned archive sequence and `committed_at`;
- emitted event IDs/sequences, assigned record versions, and transitions;
- receipt digest over a canonical representation that excludes the digest field
  itself.

Manifest and receipt sizes are bounded. Golden fixtures pin their deterministic
serialization before the protocol is declared stable.

## Conceptual API

The exact paths and encoding are not yet a stable public API, but the protocol
has four operations:

1. **Propose generation:** submit identities, revision manifests, locations,
   and tombstones. The service validates scope and reports already accepted or
   missing chunks.
2. **Query/upload objects:** batch-query object digests and idempotently upload
   each missing bounded object. The server streams, hashes, length-checks, and
   atomically publishes only a matching object.
3. **Commit generation:** request commit after all referenced objects exist.
   The service validates manifests, conflicts, producer sequence, and policy.
4. **Read receipt/status:** retrieve the immutable commit receipt and source-to-
   archive lag state. A lost response is recovered by querying the same client
   generation ID.

Object upload is safe to retry because the URL/key is content-derived. An
existing object with the correct length and digest succeeds without rewriting;
any mismatch is corruption and blocks commit.

## Commit state machine

```text
proposed -> uploading -> ready -> verifying -> publishing -> committed
    |           |          |          |             |
    +-----------+----------+----------+-------------+--> failed/retryable
```

The commit procedure is:

1. Confirm every referenced object exists with the declared digest and length.
2. Stream ordered objects for each new revision through BLAKE3-256 and verify
   the declared whole-record digest and byte length. No full-file copy is kept.
3. Under the exclusive archive commit mutex, validate predecessor state,
   allocate archive/event sequences, and durably record `preparing` state.
4. Write, `fsync`, atomically rename, and parent-directory `fsync` the immutable
   canonical receipt. Receipt rename is the archive commit point.
5. Finalize catalog events and eligible delivery rows in one transaction.
6. Return the receipt. Refresh of the non-authoritative current projection is
   asynchronous.

Startup reconciles receipts before becoming ready. A receipt with missing or
preparing catalog state is idempotently imported; catalog-committed state with a
missing or invalid receipt fails readiness. The complete serialization, `fsync`,
sequence, and crash matrix is normative in
[durability-contract.md](durability-contract.md).

One committed generation may contain many records, but implementation limits
its manifest bytes, record count, and total newly referenced bytes so commits
remain bounded.

## Payload access and current projection

A V2 revision resolves to `availability=retained_chunks`. `RevisionReader`
streams its committed chunk manifest and validates both object and whole-record
digests. A path-only consumer receives a lease-scoped full-file materialization
from a bounded derived cache; Bookkeeper does not permanently reconstruct every
historical revision.

The current projection contains only the latest ordinary-file view for
inspection, export, and compatibility. It is refreshed after commit, may lag the
ledger, and is rebuilt from objects and receipts. A location move updates this
view without changing identity; a tombstone removes the current view according
to policy. Recursive scanning of the projection is never the preferred or
authoritative consumer protocol.

Projection paths are derived from validated producer, agent, logical root, and
provider-safe location fields. Client strings are never concatenated directly
into filesystem paths. See
[payload-delivery-contract.md](payload-delivery-contract.md).

## Authentication and authorization

Authentication is pluggable, but authorization semantics are fixed:

- a credential maps to exactly one producer scope unless explicitly elevated;
- a producer may propose records only in its assigned namespace;
- clients may query their own generation receipts and object presence, but do
  not receive transcript bytes or other producers' metadata by default;
- administrative and consumer credentials are separate from producer upload
  credentials;
- request size, object size, manifest count, and rate limits are enforced before
  expensive work.

LAN placement may reduce exposure but does not replace the producer identity or
scope checks. TLS termination and credential mechanism remain deployment
choices until the API proof selects a reference setup.

Producer identity is random and stable; credential rotation and client-epoch
recovery follow [domain-contract.md](domain-contract.md). Client-supplied clocks
are provenance only and never determine generation order.

## Retry, offline, and conflict behavior

- Offline clients retain scan/upload/commit state locally and retry on a later
  trigger.
- Client-state loss requires an administrator-authorized new epoch chained to
  the latest receipt; it cannot reuse an unknown prior sequence.
- A server restart preserves uploaded objects and proposed generations, then
  reconciles their state.
- Duplicate object uploads and generation commits are idempotent.
- Out-of-order client generation IDs are rejected or held according to an
  explicit predecessor receipt; they are never silently reordered.
- A source file that changes while being read is abandoned locally and scanned
  again.
- Truncation or rewrite produces a complete new revision manifest; append-only
  behavior is never assumed for correctness.
- Conflicting live locations or identities use the same V1.5 conflict states
  and do not bypass them through transport.

## Garbage collection

The initial V2 proof does not automatically delete canonical objects. It may
expire incomplete staging after a conservative age. Canonical object garbage
collection is introduced only after retention semantics are selected and a
mark phase can prove that no committed revision, retained history, or active
staging manifest references an object.

## Existing-mirror seed

Migration does not send an existing archive back through a workstation:

1. V1.5 reconciles the mirror and freezes a selected stable catalog cohort.
2. Bookkeeper exports a versioned migration-epoch bundle containing the V1.5
   event ledger/current state and historical payload-availability metadata.
3. A server-side seed tool streams current available revisions through the selected
   chunk profile into the V2 object store.
4. It verifies canonical whole-revision digests and writes imported commit
   receipts linked to existing V1.5 records.
5. A catalog rebuild from the migration epoch plus receipts, current projection,
   and consumer event equivalence are checked before any transport
   cutover.

The filesystem source adapter remains a supported import, recovery, and audit
tool after V2 becomes primary.

## V1-to-V2 cutover

Do not allow the filesystem mirror and API to race as co-authoritative writers
for one producer. Cutover is a bounded handoff:

1. Prove V2 in a test producer namespace while V1 remains authoritative.
2. Seed and verify all selected current V1.5 revisions server-side.
3. Drain the V1 client's pending queue and perform one final guarded V1.5
   reconciliation.
4. Pause that producer's V1 transport briefly.
5. Change its Bookkeeper ingress mode from `mirror` to `api` and install the V2
   client hook.
6. Require one successful V2 generation commit and consumer acknowledgement.
7. Keep the old mirror read-only as rollback evidence through an acceptance
   window, then retire it through a separate retention decision.

Producer ingress mode is a catalog invariant. An operator cannot accidentally
accept live commits from both authorities without an explicit migration mode.

## Proof order

1. Bounded chunk upload and idempotent object storage.
2. Multi-gigabyte seed and byte-exact reconstruction.
3. Full-scan resource measurement plus tail-network append, truncation, rewrite,
   interrupted upload, and lost response.
4. Commit crash recovery at every state transition.
5. Location move and tombstone behavior.
6. Server-side seed from the V1.5 filesystem adapter.
7. Catalog reconstruction from the migration epoch and receipts.
8. Streaming and path-only consumer equivalence against the same event stream.
9. One-producer cutover and rollback drill.

The full gates are in [proof-plan.md](proof-plan.md).

## Deferred extensions

- FastCDC or another content-defined chunking profile.
- A tus whole-object/large-object upload adapter.
- Object-store backends such as S3-compatible storage.
- Multi-writer archive service clustering.
- Cross-producer deduplication policy and privacy analysis.
- Canonical-object retention and privacy-deletion garbage collection.
