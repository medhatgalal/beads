# Beads Fleet Realistic-Workload Goal Contract

Date: 2026-07-12
Experiment plan state: ready
Paced workload runner: not implemented
Auto-Research decision state: dry_run_ready
Production decision state: hold

## Target Summary

Prove whether bounded configuration and focused Beads/gateway code changes can
support hundreds of registered repository databases with team-level isolation,
while one hot repository (`ae`) remains a single logical consistency domain.

The goal is not maximum raw commit throughput. It is predictable interactive
latency, correctness, and recovery under a realistic AI-team arrival process,
plus enough burst headroom to survive planning waves and agent fan-out.

## Workload model

Use the closed-loop command-arrival model:

`commands/s = H * f * A * k / (Z + kR)`

`H` is registered people, `f` the active fraction, `A` agents per active
person, `k` commands per reasoning cycle, `Z` LLM/tool think time, and `R`
Beads response time. This makes agents wait for results; the old open-loop probe
did not. The scenario arithmetic uses `R = 1 s` as a placeholder inside the
existing SLO envelope; replay must use the measured response distribution.

The following are synthetic planning assumptions, not production facts:

| Profile | Active people | Agents/person | Think time | Commands/cycle | Fleet commands/s | Read/write | `ae` share | `ae` point writes/s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Normal | 10% of 300 | 1.5 | 90 s | 2 | 0.98 | 70/30 | 25% | 0.07 |
| Busy, realistic-high | 25% | 2 | 45 s | 2.5 | 7.89 | 60/40 | 40% | 1.26 |
| Coordinated five-minute burst | 50% | 2.5 | 30 s | 3 | 34.09 | 50/50 | 50% | 8.52 |

Model large graphs separately: four per hour fleet-wide normally, 30 per hour
when busy, and 30 within two minutes during a synchronized planning burst, half
targeting `ae`. These are intentionally conservative assumptions. Model 30
teams with ten repositories each until actual anonymized topology/rate
aggregates are approved.

Open-loop 50/100/200 command/s saturation remains a separate failure-envelope
test, not the sizing target.

## Goal

1. Preserve the original 150 ms RTT command SLOs.
2. Keep sustained busy utilization at or below 0.60.
3. Absorb the five-minute coordinated burst at or below 0.80 utilization, or
   prove its calculated backlog drains within twice the burst duration.
4. Prevent one team or `ae` from violating another team's absolute SLO or
   increasing its p95 beyond 1.25 times isolated performance.
5. Preserve every correctness and recovery invariant.
6. Keep connection, memory, file-descriptor, GC, and backup behavior bounded as
   registered databases rise from 100 to 300 and then 500.

## Non-goals

- A land-speed-record raw SQL benchmark.
- Proving hundreds of simultaneously authenticated humans are continuously
  writing; LLM and human pacing are part of the model.
- Splitting `ae` across databases before graph shardability is proven.
- Production deployment, production logging, or production data.
- Cross-repository atomic transactions.

## Editable surface

- Isolated gateway and fleet harnesses in disposable lab workspaces (not production).
- Synthetic repository databases and synthetic identities.
- Pool, cell, scheduler, GC, and dedicated-`ae` configuration.
- Canonical Beads UOW parity, actor registry, set-based graph path,
  microbatching, and conflict-certificate prototype.

## Protected surfaces

- Production VM, database, credentials, snapshots, refs, and remotes.
- EngOS and dotfiles.
- Existing project databases.
- Upstream or fork deployment without a later explicit approval.

## Baseline evidence

- Direct WAN SQL misses routine command gates; near-data gateway passes them.
- Fixed pool prewarm opened 200 connections for 100 databases.
- Shared hot-database load increased a cold database p95 by 2.979 times.
- Raw lost-update, graph-cycle, and duplicate-effect anomalies reproduced 3/3.
- Frozen gateway UOW path failed canonical lease, row-lock, and retry parity.
- The 100-writer loop is a useful failure-envelope discriminator, not a
  realistic AI-workload capacity proof.

## Scorecard

- `ping` p95 under 1.5 s at 150 ms RTT.
- `list`, `ready`, and `show` p95 under 2 s.
- `create`, `update`, `close`, and `claim` p95 under 3 s.
- 100-node/200-edge graph under 60 s with no ordinary operation waiting more
  than 3 s behind structural work.
- Normal/busy queue-wait p95 under 500 ms; bounded-burst p95 under 2 s.
- Busy utilization no greater than 0.60; burst utilization no greater than
  0.80, with bounded calculated drain.
- Unloaded teams retain absolute SLOs and p95 no worse than 1.25 times isolated.
- At least 20% connection and worker capacity remains available for
  reconciliation, failover, and administration.
- Zero lost updates, duplicate claims/effects, cycles, partial graphs, stale
  leaders, cross-team routing, or false success.
- Cell connections no greater than 256; cold-heavy idle no greater than 64.
- No monotonic RSS, disk, file-descriptor, queue, receipt, or outbox growth after
  steady-state GC/retention windows.

## Search surface and order

Measure one attributable delta per candidate:

1. A: repair canonical issueops/UOW parity and whole-UOW conflict replay.
2. B: A plus configuration-only lazy pools, zero cold idle, per-database
   caps 1/2/4/8, global caps 64/128/256, cell sizes 10/25/50, and staggered GC.
3. C: B plus per-repository actors and per-team weighted admission/quotas.
4. D: C plus a dedicated single-writer `ae` process/server.
5. E: D plus set-based graph insertion/recomputation.
6. F: E plus 5/10/20 ms microbatches only after a measured miss.
7. G: F plus conflict-certified 2/4/8 point lanes only after correctness proof.
8. Read/projection shards and direct standby are read/HA experiments.
9. Multi-database `ae` subgraphs remain behind the shardability gate.

## Trial policy and budget

- Five steady windows per normal/busy candidate; three coordinated-burst
  windows.
- Report retained candidates at 20, 50, 100, 150, and 200 ms RTT; the original
  thresholds remain enforced at 150 ms.
- One 30–60 minute churn/GC run before any 24-hour soak.
- Local synthetic execution first.
- No billable cloud restart without a renewed budget, TTL, and isolation
  approval.
- Stop a candidate after two correctness failures, two non-material tuning
  attempts, unbounded growth, or a clearly dominant alternative.

## Promotion threshold

A candidate is promotable only when it passes the complete scorecard under the
realistic closed-loop profiles, the bounded surge, and the recovery matrix.
Raw maximum throughput cannot compensate for a correctness or isolation miss.
Stopping after D passes means stop adding optimization complexity, not skip HA,
restore, migration, or soak proof.

## Rollback trigger

Any semantic-parity regression, cross-team impact, unclassified accepted
operation, stale-leader commit, restore-epoch error, or unbounded resource
growth returns the decision to hold and restores the last serialized candidate.

## Telemetry needed before final sizing

Only aggregate, secret-free counts are needed: active humans and agents by
15-minute window; command class; repository/team pseudonym; inter-command think
time; command-chain length; read/write ratio; graph size/frequency; same-issue
contention; active-repo set; burst duration. If an optional 2–4 week pilot is
approved, collect these at clients without issue text, credentials, or raw SQL.
Until then, retain the synthetic profiles above and do not call them production
truth.
