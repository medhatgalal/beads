# Remote Server Gateway / Repository Cell Architecture

**Status:** Draft design — candidate direction from lab (not an accepted ADR; integrated production cell **not** implemented)  
**Paired evidence:** [`docs/performance/remote-server-lab.md`](../performance/remote-server-lab.md)  
**Charter note:** Orchestration policy and multi-tenant gateway control planes belong **outside** core Beads issue primitives when they exceed the storage/driver boundary. See [`docs/PROJECT_CHARTER.md`](../PROJECT_CHARTER.md).

## 1. Goal

Provide a near-data execution plane so remote clients can:

- invoke high-level Beads operations with few WAN round-trips;
- preserve canonical Dolt authority for mutations;
- isolate teams and repositories physically enough to bound noisy neighbors;
- recover deterministically from retries, partial failures, failover, and restore.

This document defines **contracts and promotion gates**. It does not claim the current prototype is production-ready.

## 2. Why a gateway (boundary result)

Measured lab work established:

- Direct Beads SQL over ~150 ms RTT misses interactive SLOs (multi-statement chatter).
- A near-data high-level operation path can meet those SLOs in **historical** matrices.
- Therefore the WAN boundary should carry **operations**, not full storage SQL chats.

See the performance lab doc for numbers and provenance limits.

## 3. Required production shape

```text
Remote clients / sidecars
        |
 Stateless regional ingress
 auth | quota | validate | route
        |
 Versioned repository catalog
 repo -> database + epoch + home cell
        |
 +------+--------+------------------+
 | cell A        | cell B           | dedicated ae cell
 | cold/warm DBs | cold/warm DBs    | ae Dolt pair only
 | lazy pools    | lazy pools       | bulkhead
 | 1 mutator/DB  | 1 mutator/DB     |
 +------+--------+---------+--------+
        |                  |
   Dolt per repository   Dolt ae
   receipt/outbox CAS    receipt/outbox CAS
```

### 3.1 Invariants

| Invariant | Rationale |
| --- | --- |
| One Dolt database per repository | Failure domain and connection cardinality |
| Physical team cells / bulkheads | Logical queues alone failed noisy-neighbor probes |
| Dedicated `ae` cell **and** Dolt pair before sharding `ae` | Hot tenant domination observed on shared engine |
| Zero connections for cold repositories | Prewarmed N×DBs multiplies memory/FDs |
| One mutation actor per repository + fair cross-repo admission | Lost-update / cycle probes without serialization |
| Full high-level operation replay in a **fresh** UOW/snapshot | Retrying only `DOLT_COMMIT` on a failed UOW is unsafe |
| Versioned shared `UPDATE`/CAS ledger for capacity | `SELECT FOR UPDATE` did not enforce needed bounds |
| Dolt is write authority; SQLite/projections never are | Projection loss must be recoverable |
| Subject/request/repository/epoch-bound idempotency | Same-key replay; body-conflict reject |
| External monotonic epoch/watermark authority | Survives restore rollback and stale primaries |
| OIDC or mTLS identities, revocation, per-team credentials/quotas | Lab tokens are not production identity |

## 4. Protocol contracts

### 4.1 Terminal operations (terminal-v2 direction)

- Client submits a high-level operation with a deterministic idempotency key bound to subject, repository, epoch, and body hash.
- Server returns a **terminal** outcome (success, conflict, validation failure) or a recoverable non-terminal that requires **identical** key/body retry.
- A `503` (or equivalent) after a write is **unknown**: client retries the identical key/body; server must not double-apply.

### 4.2 Mutation kernel

- Execute canonical Beads domain semantics (create/update/close/claim/ready/deps/graph) via fresh unit-of-work snapshots.
- On retryable storage conflicts, **replay the full operation** in a new UOW; never commit-only retry on a dirty failed UOW.
- Lease/heartbeat/claim paths must match canonical issueops semantics (lab found domain/UOW split risks).

### 4.3 Receipt, outbox, retention

- Canonical mutation receipt and outbox markers live in Dolt.
- Bounded retention via CAS-guarded cleanup (lab oracle `c26c8325` passed 11/11 scenarios; **not** yet integrated into a full gateway binary path).
- Independent inspector must reconstruct exact terminal state from canonical tables without trusting the queue projection.

### 4.4 Catalog and epochs

- Signed team/repository catalog maps `repo_id → database_id + home cell + capability epoch`.
- Monotonic watermarks are **external** to the restored Dolt failure domain.
- Cold start after restore must reject epoch rollback (stale worker fencing).

### 4.5 Scheduling and pools

- Lazy pools with hard caps (measured: 10k ops under 28 app / 32 Dolt connections feasible in lab shape).
- Bounded fair admission across repositories; per-DB mutation serialization initially.
- Memory guidance from lab: 3 GiB / GOGC100 **HOLD** candidate; 1 GiB **reject**.

## 5. Threat model (summary)

| Threat | Mitigation direction |
| --- | --- |
| Cross-team data or effect leakage | Physical bulkheads + authz on catalog route |
| Duplicate claims / lost edges under retry | Idempotency keys + fresh-UOW replay + CAS |
| Stale primary after partition | Epoch fencing; persistent ack certificates |
| Restore of old backup + live workers | External watermark; reject lower epochs |
| Hot `ae` starves quiet teams | Dedicated cell/Dolt pair; fair admission |
| Queue projection loss | Dolt receipts authority; rebuild projection |
| Credential theft of lab-style static tokens | OIDC/mTLS, short-lived creds, revocation |
| Accidental production mutation by lab harness | Synthetic-only guards; reject ports 3306/3307; no prod DSNs |

## 6. Rejected alternatives

Documented so they are not re-proposed without new evidence:

- IAP / SSH forwarding / VPN overlay as the **primary** latency fix.
- DSN tweaks, prepared statements, `max_allowed_packet`, ping-count-only, dirty read-fast as terminal answers.
- Workstation-local daemon as final multi-team architecture.
- Local-queue-authoritative `202` responses.
- Fixed prewarmed pool per database at fleet cardinality.
- 1 GiB memory limit or disabling auto-GC as the main fix.
- Premature graph sharding of `ae`.
- Shipping `cluster-lab` root/global mutation harnesses.
- Merging the broad research prototype as one upstream PR.

## 7. Migration and rollback

### 7.1 Migration sketch

1. **Co-located / embedded** users unchanged.
2. Introduce near-data cell for remote teams only; clients opt in via config.
3. Dual-run correctness probes (direct vs cell) on synthetic workloads.
4. Cut remote traffic to cell after promotion gates; keep direct path for break-glass co-located ops.
5. Expand multi-cell routing and `ae` bulkhead only after isolation soaks.

### 7.2 Rollback

- Feature-flag clients back to co-located or direct path for emergency.
- Cell retains Dolt authority; no SQLite-authoritative rewrite.
- Epoch/catalog remain external so rollback does not invent higher epochs.

## 8. Promotion gates

Do **not** call the architecture production-ready until **all** hold on **current** source and binary manifests:

1. Routine and graph SLOs at 150 ms (see performance lab).
2. Ordinary write p99 < 3 s under fleet promotion workload.
3. Quiet-team p95 ≤ 1.25× isolated under hot `ae`.
4. No duplicate claims, lost edges, partial graphs, or foreign-team effects.
5. Deterministic recovery after interrupted ops and projection loss.
6. Automatic stale-primary and restore fencing with measured RTO.
7. Resource plateaus (RSS, heap, FDs, connections, queues, receipts, outbox, history, disk, GC) over 24–72 h or 1e6 fixed-cardinality ops.
8. One source/binary manifest equals the exact integrated executable under test.

## 9. Relationship to core Beads

| In core Beads (likely) | Outside core / orchestration layer |
| --- | --- |
| Fresh-UOW retry correctness | Multi-tenant ingress and quotas |
| Storage driver safety | Signed fleet catalog service |
| Domain claim/lease semantics | Physical cell deployment topology |
| Optional harnesses under `scripts/` | Production gateway binary packaging |

Upstream contributions should stay **narrow**: correctness fixes, driver safety, documented harnesses, and RFCs. Broad laboratory prototypes remain operator-fork research until sliced.

## 10. Open implementation work

1. One request path integrating terminal-v2, mutation kernel, catalog epochs, bulkheads, lazy pools, CAS retention, cluster ack, inspector, real identities.
2. Current-source latency matrices (do not reuse historical numbers for promotion).
3. 100+ repository DBs, ≥30 identities/teams, dedicated `ae`.
4. Multi-peer failure at every commit/ack boundary; actual old-backup restore fencing.
5. Long soak and history/GC bounds.

Until those close, the durable statement remains:

> Near-data execution is **required**; a production gateway/cell is **not yet delivered**.
