# Domain contract

Status: shared V1.5/V2 implementation contract. Changes to identity or canonical
revision semantics require an explicit schema and protocol migration.

## Producer identity and lifecycle

A producer is an authorized origin of agent-session records. The archive
service provisions a random stable `producer_id`; it is not derived from a
hostname, path, credential, or network address.

Credentials and producer identity are separate:

- rotating or replacing a credential does not change `producer_id`;
- reinstalling a client recovers the existing producer through an
  administrator-authorized enrollment flow;
- creating a replacement producer is an explicit fork of archive identity, not
  an automatic recovery behavior.

V2 generations also carry a random `client_epoch`. The unique client request
key is:

```text
(producer_id, client_epoch, client_generation_id)
```

`client_generation_id` increases within one epoch. If local client state is
lost, the server reports the producer's latest committed receipt and an
administrator authorizes a new epoch chained to that receipt. A client never
guesses or resets a sequence inside an existing epoch.

## Session and record identity

A session may contain more than one archived record. The stable record key is:

```text
(
  producer_id,
  agent_namespace,
  session_id,
  record_kind,
  record_key
)
```

- `agent_namespace` identifies the provider family and identity contract.
- `session_id` is the provider's stable session identity.
- `record_kind` identifies the payload role, initially `transcript`.
- `record_key` distinguishes multiple records of one kind, initially `primary`.

The single-JSONL case uses `record_kind=transcript` and
`record_key=primary`. Providers with rotated segments or sidecars may define
additional stable values without pretending each file is a separate session.

Current root role and source-relative path are location metadata. They are not
part of record identity.

## Provider identity schema

Every source adapter declares an `identity_schema` name and version. It defines:

- accepted source layouts and finalized file types;
- bounded parsing rules for `session_id`, `record_kind`, and `record_key`;
- canonical root roles and location normalization;
- whether a record may be treated as append-dominant for scheduling only;
- any deterministic fallback identity rule.

The initial implementation rejects a source that lacks a stable session
identity. A fallback may be enabled only when it is independent of current path
and documented with golden move/rename fixtures. Changing identity extraction
under an existing schema version is forbidden; a new version requires a catalog
migration or explicit reimport.

## Record and location versions

Each accepted consumer-relevant transition increments a monotonic
`record_version` scoped to the stable record:

- a new byte revision;
- a location change;
- a tombstone;
- a restore.

Conflicts are archive/operator events but do not advance consumer-visible record
state until resolved. A consumer can reject any delivery whose
`record_version` is older than its durable state for that record.

## Canonical byte revision

Bookkeeper V1.5 and V2 use the same canonical raw-byte digest:

```text
algorithm: blake3-256
revision_key: (record_id, byte_length, blake3-256 digest)
```

The canonical digest is over the complete raw record bytes. Chunk digests use
the same algorithm but are typed separately and cannot be confused with a
whole-record digest.

The catalog may store an algorithm-tagged `secondary_digests` set, such as
SHA-256 for interoperability with an existing consumer. Secondary digests never
change revision identity. A migration to another canonical digest algorithm
requires a new domain/protocol version and an equivalence mapping; it is not an
in-place configuration change.

V1.5 computes the canonical digest server-side from the guarded external file.
V2 computes it client-side and independently verifies it server-side by
streaming the ordered committed chunks before the generation receipt is
published.

## Generation identity

A generation groups record transitions:

- V1.5 scan generations have `completeness=observed`; they are a reconciliation
  grouping, not proof of an atomic client tree.
- V2 client generations have `completeness=declared`; the client declares one
  bounded transaction chained to its prior receipt.

Each committed generation receives a global monotonic `archive_sequence` from
the single archive writer. Each emitted event receives a unique `event_id` and
ordered `event_sequence` within the same durable commit protocol.

Consumer correctness relies on `record_version` and event type. Wall-clock
timestamps never determine ordering.

## Time fields

Bookkeeper distinguishes:

| Field | Authority and purpose |
| --- | --- |
| `source_observed_at` | Producer/client clock; informational provenance only. |
| `server_observed_at` | Server clock when a V1.5 candidate was observed or a V2 request arrived. |
| `committed_at` | Server clock assigned by the archive writer to the accepted commit; embedded in a V2 receipt and informational. |
| `archive_sequence` / `event_sequence` | Authoritative ordering independent of clock skew. |

Provider timestamps found inside a transcript remain raw provider metadata.
They are not substituted for archive commit time. In V2, `committed_at` is
sampled once under the commit mutex while constructing the receipt immediately
before publication; receipt publication, not timestamp comparison, decides
whether the commit exists.

## Initial event types

Consumer-relevant events are:

- `revision_committed`;
- `location_changed`;
- `record_tombstoned`;
- `record_restored`.

Operator-only events are:

- `conflict_opened`;
- `conflict_resolved`;
- source health and integrity alerts.

Operator-only events enter the audit ledger and status surface but do not create
consumer deliveries unless a consumer explicitly subscribes to the audit
stream.

## Domain invariants

1. One stable record has at most one current non-tombstoned location.
2. One `record_version` represents one accepted state transition.
3. A canonical revision digest always names the same exact raw bytes.
4. Location changes never create a second record or byte revision by
   themselves.
5. Later record versions never become eligible ahead of an earlier unresolved
   ordering barrier for the same consumer and record.
6. Producer credentials may change; producer identity does not change
   implicitly.
7. Client and provider clocks never define archive ordering.
