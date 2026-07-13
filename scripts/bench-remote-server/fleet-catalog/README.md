# Signed fleet routing catalog

This prototype makes team and repository isolation a server-owned invariant.
Clients provide logical repository, team, project, and database-epoch identity;
they never provide a DSN, SQL host, database name, or cell address.

The control plane signs an immutable catalog containing one authoritative route
per repository with an Ed25519 private key. Cells hold only public verification
keys. A compromised cell therefore cannot forge routes. A cell rechecks the
15-minute maximum lease on every resolution, rejects non-increasing catalog
versions and key-epoch downgrades, and requires every request identity field to
match. A restore changes `database_epoch`, so a stale worker fails closed. Key
epochs support an explicit public verification ring during signing-key
rotation.

The 300-repository test inventory maps ten repositories per team cell across
thirty teams. `ae` is a mandatory exception: its route is valid only when both
its execution cell and storage endpoint are dedicated and exclusive. A cell ID
may belong to exactly one team; any route marked dedicated has an exclusive
cell and endpoint. Shared ordinary endpoints remain explicitly allowed, so
physical storage isolation for ordinary teams is not proven by this primitive.

This is a routing/fencing primitive, not a secret store or HA catalog service.
Production still needs an authenticated control-plane distribution channel,
audited two-person route changes, KMS-backed private signing keys, transactional
database-side epoch checks for in-flight work, and fault-tested expiry/recovery.
