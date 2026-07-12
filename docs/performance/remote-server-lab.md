# Remote Server Performance Lab

**Status:** Research evidence (HOLD) — not a production readiness claim, public API, or accepted ADR  
**Lab date:** 2026-07-11 – 2026-07-12  
**Terminal architecture verdict:** `GATEWAY_ARCHITECTURE_REQUIRED`  
**Production readiness:** **No**

This document records the problem framing, workload assumptions, measured results, and decision boundary for Beads remote-team use over a high-latency path to Dolt. It is intentionally separate from implementation PRs.

Compact provenance and checksums live under
[`scripts/bench-remote-server/evidence/`](../../scripts/bench-remote-server/evidence/)
and are summarized in
[`scripts/bench-remote-server/EVIDENCE.md`](../../scripts/bench-remote-server/EVIDENCE.md).
The architecture contracts are in
[`docs/design/remote-server-gateway.md`](../design/remote-server-gateway.md).

## 1. Problem

Teams want a shared Beads issue graph with:

- one Dolt database per repository;
- many repositories (tens to hundreds);
- human and agent concurrency;
- clients that are not co-located with Dolt (WAN / multi-region RTT);
- routine CLI commands that remain interactive.

The natural integration path—Beads opening MySQL-protocol sessions to a remote Dolt and running the full command SQL chat over that path—was hypothesized to be too chatty for interactive SLOs once RTT reaches ~150 ms.

## 2. Workload assumptions

| Dimension | Lab assumption | Limit |
| --- | --- | --- |
| RTT | Controlled delay (20–200 ms matrices; focus 150 ms) | Synthetic `tc`/netem style path, not public internet variance |
| Data | Synthetic issues/deps only | Never production DBs, snapshots, or credentials |
| Repositories | One DB per repo model | Realistic fleet runs used **two** DBs only |
| Identities | Lab tokens / configured subjects | Not OIDC/mTLS production identity |
| Hot tenant | Dedicated pressure on repository `ae` | Full multi-principal `ae` not proven |
| Operations lane | Historical realistic runs used **legacy** gateway lane | Not terminal-v2 fleet promotion |

## 3. Service level objectives (150 ms RTT)

| Operation | Target |
| --- | --- |
| `ping` p95 | < 1.5 s |
| `list` / `ready` / `show` p95 | < 2.0 s |
| `create` / `update` / `close` p95 | < 3.0 s |
| Graph 100 nodes / 200 edges | < 60 s |
| Ordinary write p99 (fleet promotion) | < 3.0 s |
| Quiet-team p95 under noisy neighbor | ≤ 1.25× isolated |

Correctness gates (non-negotiable): no partial graphs, no duplicate claims, no lost edges, no foreign-team effects; deterministic recovery after interrupted operations; bounded connections/memory/FDs/queues/receipts/outbox/history/disk/GC under soak.

## 4. Solution space (hypothesis matrix)

| ID | Hypothesis | Outcome |
| --- | --- | --- |
| H1 | Direct Beads↔Dolt SQL over 150 ms WAN can meet routine SLOs with small client patches | **Rejected** (measured miss) |
| H2 | Tunnel / IAP / VPN / DSN / prepared-statement tweaks are the primary fix | **Rejected** (does not remove chatty round-trips) |
| H3 | Already-shipped Beads 1.1.0 batching/prevalidation alone closes the gap | **Rejected** as terminal answer |
| H4 | `max_allowed_packet`, ping-count-only, or dirty read-fast paths are terminal | **Rejected** |
| H5 | Near-data high-level operation service (gateway / repository cell) can meet latency SLOs | **Supported as architecture direction** by **historical** gateway matrices |
| H6 | Historical gateway prototype is a durable production fix | **Rejected** — components exist; integrated production cell does not |
| H7 | Real-Dolt CAS retention + same-key idempotency can be proven source-bound | **Supported** for the oracle scope only (`c26c8325`, 11/11) |
| H8 | Broad research branch can merge as one upstream PR | **Rejected** |

Full append-only experiment decisions: F00–F36 in
[`evidence/experiment-ledger-decisions.json`](../../scripts/bench-remote-server/evidence/experiment-ledger-decisions.json).

## 5. Topology under test

```text
Client / harness  --(controlled RTT)-->  Beads path under test  -->  Dolt (synthetic DBs)
```

Two families:

1. **Direct path** — Beads storage/driver talks to remote Dolt with the normal multi-statement command path.
2. **Near-data gateway path** — high-level operations execute near Dolt; the WAN carries fewer round-trips.

Lab compute (disposable): GCP project `peng-os`, VM `beads-perf-lab-20260711` (`us-east4-b`, e2-standard-2). Verified **TERMINATED** after the lab; scheduled instance **DELETE** `2026-07-14T07:36:31Z`. Local Dolt listeners on ports 13360/13361 were stopped.

## 6. Results summary

### 6.1 Direct path at 150 ms (retained candidate)

| Op | p95 | vs SLO |
| --- | --- | --- |
| ping | 2.481 s | miss (<1.5 s) |
| list | 3.853 s | miss (<2 s) |
| ready | 3.255 s | miss (<2 s) |
| show | 2.938 s | miss (<2 s) |

Graph floor (100 nodes / 200 edges) was far above the 60 s gate in the measured direct configuration.

**Conclusion:** direct Beads-over-WAN is not a viable primary architecture for these SLOs within a small reasonable patch surface.

### 6.2 Historical gateway path at 150 ms

| Op | p95 (approx.) | vs SLO |
| --- | --- | --- |
| ping | 162 ms | pass |
| list | 187 ms | pass |
| ready | 203 ms | pass |
| show | 160 ms | pass |
| create / update / close | 228 / 206 / 298 ms | pass |
| graph 100/200 | 7.50 s | pass |

**Provenance boundary (critical):** these latency matrices were produced against Beads base `64a136d56…`. They must **not** be attributed to the later hardened gateway head `de720d7c0…` without a full current-source rerun.

### 6.3 Current-source correctness (strongest source-bound proof)

Real-Dolt CAS retention run `c26c8325-f640-451a-bef6-09d30f86388d`:

- 11/11 scenarios pass;
- zero dirty tables;
- independent inspection matches canonical state;
- executed binary and clean-cache rebuild byte-identical  
  (`edcc8802eaea66516a95ffc031bdb50ef470c25b57477738aaf8d444f549a447`);
- retention evidence manifest SHA-256:  
  `c70257657add914c1798b55b255ee39416b4e8bb6edd922d66a74bb77cfe2fbb`.

This proves a **retention/idempotency oracle**, not an integrated multi-tenant gateway cell.

### 6.4 Fleet / pool / memory probes (limits)

- Two 10k lazy-pool runs stayed under 28 application / 32 Dolt connections with zero failures.
- 3 GiB / GOGC100 is a **HOLD** candidate; 1 GiB caused severe GC thrashing (**reject** as primary fix).
- Raw same-process hot-`ae` probe harmed cold repositories (cold p95 ≈ 2.979× isolated; failed frozen 2.0 noisy-neighbor gate).
- Realistic busy/burst windows: **two databases, two teams, one window each, legacy operations lane**; capacity promotion **false**.

## 7. What is proven vs not proven

| Proven | Not proven |
| --- | --- |
| Direct 150 ms path misses routine SLOs | Integrated current-source cell meets fleet SLOs |
| Near-data boundary is the right architecture direction | Historical gateway performance still holds on hardened source |
| Source-bound retention/CAS oracle on real Dolt | 100–300 DB multi-identity terminal-v2 fleet |
| Fixed prewarmed pools are unsafe at DB count | Multi-zone HA, automatic fencing, measured RTO |
| SELECT FOR UPDATE is insufficient for shared capacity CAS | Old-backup restore + stale-worker fencing in production topology |

## 8. Operator draft PRs (fork-only research)

| PR | Purpose | Merge? |
| --- | --- | --- |
| [medhatgalal/beads#1](https://github.com/medhatgalal/beads/pull/1) | Partial UOW correctness / fresh-snapshot retry | **No** (draft; not green required gate) |
| [medhatgalal/beads#2](https://github.com/medhatgalal/beads/pull/2) | Broad gateway research + evidence anchors | **No** (fork research only) |

These drafts must not be read as upstream merge candidates. Future contributions should be **narrow** slices (docs/evidence, UOW fixes, protocol, tooling) per project charter.

## 9. Verdict

```text
GATEWAY_ARCHITECTURE_REQUIRED
```

Meaning:

1. High-level Beads operations for remote teams must execute **near Dolt** (repository cell / gateway).
2. The lab does **not** deliver a leak-proof production implementation.
3. The next work is durable evidence (this package), focused correctness, then one integrated cell with current-source proof—not more tunnel tweaking.

## 10. Related documents

- Architecture contracts: [`../design/remote-server-gateway.md`](../design/remote-server-gateway.md)
- Evidence index and reproduction: [`../../scripts/bench-remote-server/EVIDENCE.md`](../../scripts/bench-remote-server/EVIDENCE.md)
- Compact provenance JSON: [`../../scripts/bench-remote-server/evidence/remote-server-lab-provenance.json`](../../scripts/bench-remote-server/evidence/remote-server-lab-provenance.json)
- Project scope: [`../PROJECT_CHARTER.md`](../PROJECT_CHARTER.md)
