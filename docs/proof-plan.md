# Proof plan

Bookkeeper should be implemented behind measurable acceptance gates, not only a happy-path demo.

## V1 gates

- A hook returns promptly while a detached worker performs delivery.
- A client that is offline at its normal trigger leaves durable pending state and later succeeds without manual repair.
- A later trigger retries existing pending work even when no new session exists.
- A large existing session seeds successfully, and a small append transfers only the incremental change where the selected transport supports it.
- The destination preserves byte-exact source files and does not expose partially copied files as completed records.
- Source root restrictions prevent delivery outside the declared archive scope.

Record elapsed time, bytes sent, CPU time, and peak memory for the seed and append cases. Measure concurrent clients as well as a single client; the useful result is an operational envelope, not a benchmark headline.

## V1.5 gates

- A metadata-only scan has bounded cost over a steady archive.
- Only changed, stable files are rehashed and queued.
- A rename or path move preserves record identity and avoids duplicate downstream ingest.
- A rewrite or truncation creates exactly one new revision.
- Each consumer observes each committed revision at least once, can be paused independently, and can replay from a selected cursor.
- Backpressure limits consumer work so a discovery sweep does not create an uncontrolled embedding or indexing burst.

## V2 gates

- Interrupted uploads resume without re-sending acknowledged bytes beyond the protocol's documented overhead.
- A new client can seed a large archive without exhausting memory or blocking agent hooks.
- Append, rewrite, and truncation cases all materialize a byte-exact raw projection after commit.
- Repeated requests, out-of-order delivery, and service restarts are idempotent.
- Clients require only the service API; they have no direct access to the service's raw-storage backend.
- Export and recovery can rebuild catalogs and downstream consumer state from the canonical raw archive.

## Promotion rule

Do not call a phase ready because it transferred files once. Promote only after its failure, recovery, resource, and provenance gates pass against representative long-running sessions.
