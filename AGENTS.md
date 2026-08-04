# Agent Bookkeeper contributor guidance

## Working rules

- Prefix shell commands with `rtk`.
- Keep this repository reusable. Do not commit hostnames, IP addresses, local paths, credentials, real session content, or rendered deployment configuration.
- A deployment operator supplies durable storage and a reachable transport endpoint. This project defines contracts and reference implementations, not a particular home-lab topology.
- Preserve raw session evidence and the durable identity/revision/commit ledger. In V2, committed content-addressed objects are canonical raw evidence, not disposable chunks. Treat projections, scan caches, consumer indexes, and learned output as rebuildable. Keep delivery outcomes durable for exact restore; a deliberate rebuild uses a new subscription ID.
- Keep phase documents consistent with the normative domain, payload/delivery, and durability contracts. Canonical revision identity is complete-record BLAKE3-256 in both V1.5 and V2.
- Keep lifecycle hooks short and non-blocking. Network transfer and expensive scans belong in a detached, durable worker.
- Before committing, run the relevant checks and `rtk git diff --check`.

## Scope boundary

Agent Bookkeeper owns capture, delivery, durable raw archival, revision-aware
cataloging, and the archive-search integration that makes session evidence
retrievable with provenance. It does not decide semantic memory, author learned
context, or replace current repository state with derived history.
