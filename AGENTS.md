# Agent Bookkeeper contributor guidance

## Working rules

- Prefix shell commands with `rtk`.
- Keep this repository reusable. Do not commit hostnames, IP addresses, local paths, credentials, real session content, or rendered deployment configuration.
- A deployment operator supplies durable storage and a reachable transport endpoint. This project defines contracts and reference implementations, not a particular home-lab topology.
- Preserve raw session evidence verbatim. Treat derived catalogs, chunks, indexes, and consumer state as rebuildable unless a deployment explicitly declares otherwise.
- Keep lifecycle hooks short and non-blocking. Network transfer and expensive scans belong in a detached, durable worker.
- Before committing, run the relevant checks and `rtk git diff --check`.

## Scope boundary

Agent Bookkeeper owns capture, delivery, durable raw archival, and revision-aware cataloging of agent-session records. It does not decide semantic memory, retrieve evidence for users, or derive learned context.
