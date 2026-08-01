# Architecture

## Purpose

Agent Bookkeeper preserves agent-session records as durable, replayable evidence and exposes them to multiple independent consumers. A deployment must provide storage that is both durable and reachable from the archive service or transport destination. The project deliberately does not prescribe a particular filesystem, object store, host, or network.

## Ownership boundary

Bookkeeper owns:

- local discovery of supported session roots;
- stable identity and revision tracking for raw records;
- queued, resumable delivery;
- a canonical raw archive and its catalog;
- consumer cursors and delivery state.

Bookkeeper does not own:

- embedding, chunking for semantic retrieval, or search ranking;
- inferred facts, learned preferences, or memory-promotion policy;
- agent execution or lifecycle policy beyond a minimal trigger integration.

## Data flow

```text
agent lifecycle event
        |
        v
local archive client --scan --background
        |
        +--> durable local spool and state
        |
        v
transport adapter ---------------> archive service or compatible destination
                                                |
                                                v
                                    committed raw archive + catalog
                                                |
                          +---------------------+---------------------+
                          v                                           v
                 evidence/search consumer                     learned-memory consumer
```

The hook only requests a scan and returns. It must not synchronously upload transcripts, wait for a remote endpoint, or run embedding. The client records pending work locally; a detached worker coalesces scans, retries failed delivery, and checks outstanding work on every later trigger.

## Canonical and derived state

The committed raw archive is the recovery boundary. It consists of verbatim
source revisions plus the durable identity/revision/commit metadata needed to
interpret those bytes. In V1.5 the operator-provided mirror is the canonical
current-byte source and Bookkeeper adds a durable observation ledger. In V2,
Bookkeeper's committed objects and receipts are canonical together.

Raw projections, scan fingerprints, catalog indexes, queued jobs, retrieval
indexes, and learned memories are rebuildable. Consumer cursors and delivery
state are durable operational state: they may be replayed from the event ledger,
but should survive ordinary restarts to prevent waste.

Consumers read only committed archive events and maintain independent delivery
state. A consumer failure must never block receipt of raw records or corrupt
another consumer's progress.

## Stable record model

A record is identified by an immutable producer identity plus an agent namespace and session identifier. A current path is an attribute, not an identity: a file move must not create a new session. A source revision identifies a specific byte representation, normally using a content hash and length. Deletion or retention expiry is represented as an explicit tombstone rather than silently erasing catalog history.

See [the V1.5 design](v1.5-design.md) for the detailed domain model and
[the evolution boundary](evolution-boundary.md) for the parts V2 reuses.

## Security and privacy posture

Raw transcripts may contain sensitive material. Bookkeeper does not assume a public network; deployments should choose the authentication, authorization, encryption, retention, and access controls appropriate for their risk. The reference architecture requires that the operator provide accessible durable storage, but does not expose it directly to every client in the V2 design.
