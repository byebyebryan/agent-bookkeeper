# Agent Bookkeeper contributor guidance

## Working rules

- Prefix shell commands with `rtk`.
- Keep this repository reusable. Do not commit hostnames, IP addresses, local paths, credentials, real session content, or rendered deployment configuration.
- A deployment operator supplies durable storage and a reachable transport endpoint. This project defines contracts and reference implementations, not a particular home-lab topology.
- Preserve raw session evidence and the durable identity/revision/commit ledger. In V2, committed content-addressed objects are canonical raw evidence, not disposable chunks. Treat projections, scan caches, consumer indexes, and learned output as rebuildable; keep consumer delivery state durable but replayable.
- Keep lifecycle hooks short and non-blocking. Network transfer and expensive scans belong in a detached, durable worker.
- Before committing, run the relevant checks and `rtk git diff --check`.

## Scope boundary

Agent Bookkeeper owns capture, delivery, durable raw archival, and revision-aware cataloging of agent-session records. It does not decide semantic memory, retrieve evidence for users, or derive learned context.
