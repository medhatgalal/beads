# Remote-server performance lab

This directory is an isolated, synthetic laboratory for Beads against Dolt SQL
server mode. It is intentionally not a production service or a new public Beads
surface. Retained mutating harnesses require numeric loopback, non-production
ports, synthetic database names, and exact lab identity. `cluster-lab` is
excluded from the retained/staged set because its global role-mutation boundary
does not yet meet that contract.

## Decision

`GATEWAY_ARCHITECTURE_REQUIRED` is the current evidence-backed outcome.

Direct WAN SQL remains dominated by connection and round-trip cost. A near-data
gateway met the command SLOs at 150 ms simulated RTT, and the terminal protocol,
bounded retention state machine, cluster acknowledgement guard, and lazy fleet
pool were independently fault-tested. They are still separate prototypes; this
branch does not claim production readiness.

The work does not supersede upstream list/query optimizations in PRs #3458 or
#4167, or connection work in #4581. Those reduce direct-path cost and remain
useful for embedded and co-located modes. The gateway prototype exercises a
different architectural boundary.

## Reproducible evidence summary

Most latency values below are historical synthetic evidence from Beads commit
`64a136d56e8ae2b89071e57f90f57255e56c9ad9`, Dolt 2.1.10, and either loopback
or the disposable private GCP VM. Safety, identity, projection-retention,
dead-letter, and shutdown changes in the current tree postdate those runs and
must be re-benchmarked from a committed source manifest. The real-Dolt
retention run `c26c8325` is the exception: it includes matching evidence,
source checksum manifests, and the exact pre-existing server-control identity
gate required before any mutating proof action.

| Experiment | Retained result | Boundary |
| --- | --- | --- |
| Direct remote SQL at 150 ms RTT | current-source ping p95 3.424 s; read-fast candidate ping 2.481 s, list 3.853 s, ready 3.255 s, show 2.938 s | misses original command SLOs |
| Near-data gateway at 150 ms RTT | ping 162 ms, list 187 ms, ready 203 ms, show 160 ms, create 228 ms, update 206 ms, close 298 ms, 100-node/200-edge graph 7.50 s | latency proof, not fleet correctness by itself |
| Realistic busy profile | 8.325 point commands/s vs 7.895 target; command p95s 178-251 ms; graph 2.38 s; exact independent effects | original SLOs pass; utilization/recovery promotion gates remain unknown |
| Coordinated burst | 35.233 point commands/s; create p95 2.302 s; graph p95 4.067 s; exact effects | original p95 SLOs pass; create p99 3.235 s and max 4.813 s expose tail/HOL risk |
| Terminal v2, two gateway peers | same key/body -> same terminal result and one canonical effect; conflicting body -> one winner, loser always 409; empty projection rebuild -> attempts 0 | single-primary component passes |
| Terminal v2 plus Dolt cluster | standby down -> 503; catch-up -> same key 200/idempotent; primary loss and promotion preserved one effect; empty queue after epoch-7 recovery replayed attempts 0 | same-host manual HA proof, not multi-zone/RTO proof |
| Bounded retention on real Dolt | shared ledger CAS at producer 10->11 and receipt 7->8 admitted exactly one contender; rollback/kill/compaction/epoch/tamper fences pass | source-bound single-node storage oracle passes |
| 100-database lazy pool | application budget 28, Dolt hard cap 32; two consecutive 10k runs, zero failures; p95 622 ms then 439 ms; peak 30; hibernated to one sampler connection | high eviction/wait churn; memory/history long soak open |

The realistic runner uses the legacy asynchronous operations lane for workload
capacity. It must not be cited as terminal-v2 durability evidence. Its report's
`capacity_promotion_eligible` remains false until utilization and recovery are
measured in the same integrated run.

## Packages

- `gateway`: near-data service, canonical terminal outcomes, cluster barrier.
- `gateway-client`: guarded RTT/load client.
- `gateway-inspect` and `fleet-inspect`: independent exact-effect inspectors.
- `realistic-fleet`: frozen AI-paced busy and coordinated-burst workloads.
- `fleet-lazy`: zero-idle, hard-capped pool experiment.
- `fleet-catalog`: signed team/repository routing model.
- `fleet-scheduler`: bounded class/team/repository scheduling model.
- `idempotency-retention`: million-operation bounded SQLite oracle.
- `idempotency-retention-dolt`: real-Dolt transaction and crash oracle.
- `cluster-beads-probe`: attested, fixed-input cluster probe.
- `cluster-lab`: rejected and excluded; it can change global Dolt role state
  without a sufficiently strong server-control identity/acknowledgement gate.
- `lab-bootstrap`: least-privilege synthetic database attestation.

## Intended fleet topology

Route by signed `team_id` to a cell, then by logical repository ID to one
database actor. Cold repositories own no connection. Each cell enforces a Dolt
server connection cap above a smaller application-pool budget, bounded queues,
catalog/database/key epochs, and per-team/class reservations. A hot `ae`
repository receives a dedicated execution cell and dedicated primary/standby
Dolt pair.

Do not shard the writable `ae` graph merely because it is hot. First isolate it
physically and batch/set-optimize graph work. Sharding is allowed only when a
stable ownership key makes claims, batches, dependencies, and restore local to
one shard; cross-shard views must then be explicit eventual projections.

## Remaining production gates

1. Integrate catalog, scheduler, lazy pools, terminal protocol, retention, and
   transaction-side epoch checks in one cell binary.
2. Run two or three independent failure domains: partitions, primary kill at
   every commit boundary, automatic routing, measured RTO, and stale-primary
   fencing.
3. Restore an old disk snapshot while a stronger signed catalog retains a newer
   database epoch; prove every stale queue/worker/request fails closed.
4. Run 24-72 hour history, auto-GC, memory, disk, backup, restore, and poison
   outbox soaks.
5. Complete the focused Beads lease/row-lock/fresh-UOW parity patch. Direct
   gateway use of generic domain mutations must not bypass canonical semantics.
6. Put the monotonic catalog/key/database epoch watermark in a control plane
   that cannot roll back with a restored cell; prove cold-start downgrade rejection.
7. Replace the single lab subject/token with OIDC/mTLS workload identities,
   subject-bound quotas/idempotency/audit, revocation, and per-team credentials.
8. Provide a narrowly attested cluster-status interface for each derived SQL
   principal. The historical cluster grant and current bootstrap postcondition
   are not yet a reproducible least-privilege pair.
9. Re-run terminal-v2 capacity and failure experiments from the committed tree;
   older latency artifacts do not prove the post-review safety changes.

## Validation

```sh
go test -tags gms_pure_go ./scripts/bench-remote-server/...
go test -tags gms_pure_go -race ./scripts/bench-remote-server/...
go vet -tags gms_pure_go ./scripts/bench-remote-server/...
```

The evidence controller and JSON ledgers live outside the database under test.
No experiment controller depends on the remote Beads service it is measuring.

## Current source hardening (not yet performance-promoted)

- Legacy `/operations` admission is disabled by default; only terminal-v2 is
  the accepted lane unless a lab-only compatibility flag is explicit.
- SQLite terminal projections are capped at 20,000 rows / 128 MiB and rebuild
  from canonical Dolt outcomes. Retry-exhausted projections are capped at 100.
- Persistent failures stop after three attempts and are not auto-requeued by
  the one-second reconciler.
- Cluster acknowledgement certificates avoid an allow-empty commit on every
  replay and use a bounded 4,096-entry cache; process restart may re-barrier once.
- Shutdown drains active handlers before zeroing secrets or closing queue/SQL
  dependencies. The direct provider accepts numeric loopback only.
- The 100-database pool uses 28 application leases under Dolt's 32-connection
  hard cap, drains active leases on close, and requires return to the observed
  server-process baseline.

These items passed focused unit/race/vet checks. They are not represented by
the historical RTT/fleet JSON and therefore remain Draft/HOLD.

## Isolation identity contract

`lab-bootstrap` derives the SQL principal from all 128 bits of the canonical
project UUID: remove only the hyphens to produce one exact 32-lowercase-hex
user. `gateway`, `gateway-inspect`, `fleet-inspect`, and gateway-facing cluster
grants derive that same principal themselves; no current configuration accepts
a shared or caller-selected SQL user. The selected database's `_project_id`
and synthetic attestation must still match before work or evidence is accepted.

Live mutating harnesses reject ports `3306` and `3307`. `fleet-scale` also
requires an explicit destructive acknowledgement and verifies a pre-existing
synthetic `beads_perf_lab_control.lab_identity` singleton with the exact
canonical lab UUID before creating any fleet database. It never creates or
repairs that control identity.
