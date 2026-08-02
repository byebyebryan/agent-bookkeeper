# Payload and consumer-delivery contract

Status: shared V1.5/V2 implementation contract.

## RevisionReader

Consumers receive a revision through a Bookkeeper-owned `RevisionReader`, not a
permanent source pathname. Conceptually it provides:

```text
record identity and record_version
expected byte length and canonical digest
payload availability class
open verified byte stream
optional leased materialized path
```

The reader owns validation and lifetime. A consumer adapter must not reopen a
mutable V1.5 source path independently.

## Payload availability classes

### `current_external`

V1.5 borrows bytes from an operator-managed mutable mirror.

The reader:

1. resolves the provider-approved relative path beneath a configured root;
2. opens the file without following symlinks and retains that descriptor;
3. checks regular-file type, identity, length, and pre-read metadata;
4. streams from the same descriptor while calculating the canonical digest;
5. checks post-read metadata and accepts only the expected length and digest.

If a transport atomically replaces the pathname after open, the retained
descriptor still refers to one inode. If bytes are modified in place during the
read, digest or metadata validation fails. A mismatching locator becomes stale;
it is never reported as the expected historical revision.

This class guarantees verified delivery of bytes that remain available when
the reader opens them. It does not retain every old revision after the mirror
advances.

### `retained_chunks`

V2 resolves the revision to an immutable committed chunk manifest. The reader
streams chunks in manifest order, verifies each object digest/length, and
verifies the complete canonical revision digest/length. The payload remains
available according to archive retention policy.

### `unavailable_historical`

The ledger may retain V1.5 metadata for a historical revision whose external
bytes have advanced or disappeared. Its events remain auditable, but a consumer
requiring raw bytes cannot ingest it. Delivery follows explicit supersede,
metadata-only, or block policy.

## Path-requiring consumers

Some consumers accept only a filesystem pathname. Bookkeeper materializes a
lease-scoped verified file from a `RevisionReader` into a bounded cache:

1. stream to a temporary file while validating the complete revision;
2. fsync and atomically rename it under a cache key containing the canonical
   revision digest;
3. grant the adapter a lease on that immutable cache entry;
4. delete or evict it only after no delivery lease references it.

The cache is derived and may use copy, reflink, or ordinary streaming according
to the filesystem. It is not canonical archive storage and is bounded by bytes,
entries, and age. A multi-gigabyte path consumer therefore incurs temporary
materialization cost, but not permanent full-file storage for every revision.

The initial V1.5 reference implementation uses a distinct read-only file per
lease, keyed with the canonical digest plus a lease nonce, and releases it with
the delivery attempt. It enforces in-process active-entry and active-byte
limits. Reuse across controller processes and crash reclamation are operational
closure work; an incomplete or unowned cache entry is never treated as payload
evidence.

Adapters that accept a stream or inherited file descriptor avoid this cache.

## Delivery ordering

Bookkeeper allows parallelism across different records while preserving strict
order within one record:

- at most one unresolved delivery is leaseable for a given
  `(subscription_id, record_id)`;
- record version `N+1` is not leaseable until version `N` reaches an explicit
  advancing outcome;
- leases for unrelated records may run concurrently within consumer limits;
- consumers persist the greatest applied `record_version` per record and reject
  stale attempts.

This prevents a delayed revision retry from resurrecting content after a move,
tombstone, restore, or newer revision.

## Delivery states

```text
queued -> leased -> acknowledged
   ^         |  \-> superseded
   |         |  \-> ignored_by_policy
   |         |  \-> dead_lettered
   |         |
   +---------+---- retryable failure / lease expiry
```

- `acknowledged`: the consumer durably applied the event and provenance.
- `superseded`: an unavailable older payload was intentionally replaced by a
  later available version under a `latest_only` policy.
- `ignored_by_policy`: the subscription explicitly excludes or declines this
  event type; the reason is durable.
- `dead_lettered`: retry policy was exhausted. It is settled with respect to
  automatic retry but remains a per-record ordering barrier until an operator
  retries it or explicitly changes it to an advancing policy outcome.

`acknowledged`, `superseded`, and `ignored_by_policy` are settled, advancing
outcomes. `dead_lettered` is settled but non-advancing. `blocked`, `queued`, and
`leased` are unsettled. A consumer lacking required tombstone or move capability
becomes blocked rather than silently acknowledging the event.

A global high-water cursor may advance only across contiguous advancing
deliveries. Delivery rows remain authoritative for gaps, blocked or dead-lettered
records, and retries.

## Idempotency

The external adapter idempotency key is:

```text
(subscription_id, event_id)
```

The payload also includes `record_id`, `record_version`, canonical revision
digest, and event type so the consumer can reject stale state independently.

The adapter acknowledges only after the external consumer has durably stored
the event's source identity and revision provenance. A lost acknowledgement
causes safe redelivery with the same idempotency key.

## Subscription epochs

A consumer configuration has a stable logical `consumer_id`, while each
delivery history uses a random `subscription_id`.

- Restarting a worker retains the same subscription.
- Replaying failed deliveries retains the same subscription and idempotency
  keys.
- Rebuilding a consumer index creates a new subscription ID and new delivery
  rows, so prior acknowledgements do not suppress the rebuild.
- The archived event and record identities remain unchanged across subscription
  epochs.

## Replay modes

### `rebuild_current`

Create a new subscription snapshot containing the latest available,
non-tombstoned record version for every matching record. It works with a V1.5
mirror even when old revisions are unavailable. Events are synthetic snapshot
deliveries linked to the authoritative record versions.

### `replay_events`

Create deliveries from the ordered archive event history. Metadata events can
always be replayed, but byte-requiring revision events are fully replayable only
when their payload availability is retained. An unavailable V1.5 revision must
be superseded, ignored under explicit metadata-only policy, or left blocked.

`replay_events` is lossless for retained V2 revisions. V1.5 promises a durable
metadata history and a rebuildable current state, not retained bytes for every
historical event.

## Tombstones and restores

Tombstones and restores carry `record_version` and participate in the same
per-record ordering. A consumer must never apply a lower version after a higher
one. A delayed transport or delivery retry therefore cannot resurrect a record.

Deletion propagation remains policy-controlled. Until a consumer and deployment
select a deletion policy, tombstone deliveries remain paused for that consumer
instead of being assumed successful.

## Controlled consumer runs

A controlled run freezes:

- subscription ID and configuration digest;
- source event or `rebuild_current` snapshot boundary;
- record allowlist/filter;
- concurrency and byte limits;
- retry/dead-letter policy;
- consumer adapter version;
- provenance and retrieval acceptance checks.

Results include every settled delivery outcome. A successful run cannot hide
superseded, ignored, or dead-lettered work inside one aggregate count.
