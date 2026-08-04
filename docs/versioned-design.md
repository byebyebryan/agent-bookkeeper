# Versioned design

## Guiding constraints

- Preserve ordinary raw session files verbatim and make them replayable.
- Never make an agent hook wait on a network round trip or a large scan.
- Do inexpensive discovery frequently; reserve hashing and consumer work for changed, stable records.
- Keep delivery idempotent and safe across restarts, offline intervals, moves, truncation, and retries.
- Treat raw evidence as canonical and all downstream products as rebuildable.

## V1: external raw mirror

V1 provides a pragmatic baseline for one or more clients:

1. A lifecycle hook asks a local client to schedule a scan, then exits.
2. The client writes a durable pending marker and launches, or wakes, a detached worker.
3. The worker mirrors only configured session roots to an operator-provided, access-restricted destination.
4. A later lifecycle event also retries any pending delivery; there is no need for an idle periodic timer.

The mirror is a byte-preserving plaintext copy of the configured raw roots. An incremental filesystem transport such as `rsync` is sufficient when clients can reach the destination through a restricted account or endpoint. Initial transfer necessarily sends initial content; append-oriented updates are then inexpensive. Files deleted at the source are reflected only after a deliberate retention decision, using delayed deletion or an equivalent safe policy.

V1 deliberately keeps transport and storage external. It is simple, inspectable, and an effective proof that archive/search consumers add value, but it requires a reachable destination and a separately managed access boundary.

## V1.5: catalog and consumer controller

V1.5 adds a service-packaged controller that reads the V1 raw mirror without taking over delivery:

- scans configured source roots for cheap metadata changes;
- hashes only new or changed stable files;
- records identity, current location, revision, deletion state, and observed time;
- appends ordered archive events to a durable ledger;
- delivers each eligible event at least once until each consumer acknowledges it;
- coalesces work so an archive of many files does not cause a simultaneous full re-ingestion burst.

This layer separates ingestion policy from transport. An evidence archive can
ingest a completed session while an archive-search backend advances at a
different rate or is disabled entirely. A path move changes location metadata instead of
being reclassified as a wholly new record. If derived consumer state becomes
unsuitable, V1.5 can `rebuild_current` from the latest verified mirror bytes.
Lossless historical byte replay requires retained V2 payloads; V1.5 preserves
historical metadata but may no longer have old external bytes.

V1.5 is useful even if V2 is later adopted: it establishes the catalog schema, revision protocol, provenance, backpressure controls, and consumer boundary that V2 will use.

The detailed design is in [v1.5-design.md](v1.5-design.md). Its filesystem
source adapter remains a V2 import, recovery, and audit tool.

## V2: service-owned transport

V2 turns Bookkeeper into a self-contained archive product. The client remains local and asynchronous, but the service owns logical storage and ingestion rather than asking clients to write a remote filesystem.

```text
hook -> local scanner/spool -> incremental transport API -> service staging
                                                    -> committed generation
                                                    -> canonical objects/receipts + catalog
                                                    -> rebuildable current projection
```

The client discovers records, calculates the delta from its acknowledged state,
and uploads only missing data. The initial proof uses versioned, fixed 4 MiB
BLAKE3-addressed chunks. Each bounded chunk is independently idempotent, so tus
is unnecessary for the first proof; librsync and content-defined chunking remain
measured future options rather than required protocol dependencies.

Network updates normally reuse prior chunks, but the first correctness profile
still reads every admitted changed record completely to detect rewrites and
calculate its canonical digest. Quiescence, revision-interval, and byte-budget
policies bound this work.

The protocol must support a first full seed, append-oriented updates,
rewrite/truncation fallback, and atomic generation commit. Objects plus receipts
are canonical. Ordinary byte-exact files are streamed or materialized into a
bounded derived cache on demand; only a rebuildable latest-state projection is
kept for inspection/export compatibility.

V2 reduces remote filesystem exposure and makes the deployment portable, but it should not be started merely to avoid a small V1 inconvenience. The V1/V1.5 acceptance gates provide the evidence needed to choose it.

See [v2-design.md](v2-design.md) for the client, archive-store, commit, and
cutover design. [evolution-boundary.md](evolution-boundary.md) lists every
component reused from V1.5 and every component V2 adds.

## Explicitly deferred decisions

- Retention schedule and deletion propagation policy.
- Multi-user tenancy and authorization model.
- Additional canonical storage backends beyond the initial durable filesystem.
- Canonical event envelope and supported agent formats.
- Whether measured rewrite behavior justifies a content-defined chunk profile.
