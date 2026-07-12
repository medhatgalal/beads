# Bounded Canonical Idempotency Prototype

## Problem Framing

The current gateway keeps durable terminal receipts in Beads metadata but does
not compact them. Deleting an old receipt without retaining stronger canonical
state would permit the same operation to execute again. Keeping every receipt
forever makes metadata, indexes, Dolt history, reconciliation work, and local
projections grow without a limit.

There is an unavoidable theorem behind the design: indefinite retryability,
unbounded accepted throughput, and finite storage cannot all be guaranteed at
once. This prototype preserves correctness and the configured retry horizon;
when neither age-eligible compaction nor capacity remains, admission fails
closed rather than silently weakening either guarantee.

## Objective

Prove a bounded idempotency state machine in which:

- each producer has a fenced epoch and strictly contiguous sequence;
- each repository ledger has a monotonic epoch that can fence and reclaim the
  entire old producer namespace with one permanent watermark;
- a business mutation and its terminal receipt are one canonical transaction;
- compaction replaces receipt prefixes with one canonical watermark;
- a compacted retry returns `gone` (HTTP 410) and never invokes mutation code;
- detailed outcomes, producer tombstones, key epochs, payload bytes, and local
  memory/disk all have explicit upper bounds;
- crashes cannot publish a watermark without deleting the same receipt prefix,
  or delete a prefix without publishing the watermark.

## In Scope

- One team/cell/repository-bound canonical store. A team cell may host many
  stores, but each repository has an independent ledger epoch and transaction
  domain.
- Producer ID, producer epoch, monotonic sequence, and request digest identity.
- A bounded HMAC verification ring whose key epoch is not part of the
  idempotency identity, allowing the same request to be re-signed after
  rotation.
- SQLite as an isolated ACID stand-in for state-machine, crash, cardinality,
  disk, and heap evidence.
- Fail-closed capacity and project/team binding behavior.

## Out of Scope

- Production Beads or EngOS data, credentials, endpoints, or deployment.
- Claims about Dolt multi-process conflict behavior, Dolt commit-history GC,
  cross-zone failover, or KMS durability.
- Cross-repository atomic transactions. Production canonical state must live
  in the same repository database and unit of work as the Beads mutation.
- Online, selective deletion of one producer tombstone. Safe reclamation is a
  repository-wide ledger-epoch rotation; selective reuse still requires a
  separately proven credential-revocation protocol.

## Assumptions & Constraints

- A logical agent/workstream owns one producer ID. Concurrent agents do not
  share a sequence allocator.
- A producer head is immutably bound to the authenticated subject hash. The
  verification-ring secret remains gateway/KMS-side and is never distributed
  as a shared team client credential.
- New sequences are exactly contiguous. A future sequence is rejected until
  its predecessor is terminal; this prevents permanent gaps from pinning old
  receipts.
- Terminal outcomes are at most the configured size. Larger results must be
  represented by a bounded canonical summary plus references to business data.
- `MaxProducers` is a per-repository admission quota. Producer heads are not
  forgotten merely because detailed receipts compact. On quota pressure, an
  explicit ledger-epoch rotation fences every old request, deletes all old
  heads/receipts atomically, and forces producers to re-register.
- Authentication happens before revealing whether an operation is retained or
  compacted.
- Receipt age uses a trusted near-data server timestamp. Compaction advances
  only across a contiguous prefix in which every receipt is old enough, so a
  clock anomaly on a later receipt cannot be skipped.

## Architecture Recommendation

Use a **Fenced Segmented Producer Log** per repository database:

```text
authenticated request
  -> verify bounded key ring
  -> compare signed repository ledger epoch
       old: 410 Gone; future: fenced
  -> load producer head (epoch, next_sequence, compacted_through)
  -> retired epoch or sequence <= watermark: 410 Gone
  -> retained same digest: replay terminal outcome
  -> retained different digest: 409 Conflict
  -> sequence > next: 425/409 sequence gap
  -> sequence == next:
       one Dolt UOW { business mutation + receipt + head CAS }
  -> compact eligible prefix:
       one Dolt UOW { advance watermark + delete/replace prefix }
```

For the upstream-compatible first implementation, use namespaced Beads
metadata: one small producer-head value plus fixed-size receipt segments. A
segment key is computed directly from producer epoch and sequence, so retained
retry lookup is O(1), not a prefix scan. Segment size is chosen so its worst
encoded outcome stays below the existing metadata-value limit; only the active
segment is rewritten and sealed segments are immutable. Compaction advances
the producer watermark and deletes whole prefix segments in the same UOW.

Repository-ledger rotation first publishes one global epoch fence. Old segment
keys are then unreachable and may be deleted incrementally. Permit at most one
retired generation and reserve a two-generation physical quota; if cleanup is
incomplete, refuse another rotation and backpressure new admissions before the
physical quota. This keeps cleanup crash-recoverable and storage-bounded without
a giant deletion transaction. If measured segment rewrites or deletes remain
material, widen the storage-driver interface for a relationalized ledger; do
not add Dolt-specific recovery logic to Beads core.

The SQLite prototype is the relationalized oracle. Its persisted binding fixes
cell, team, and policy so a file cannot be adopted by another identity or
reopened with silently weaker limits.

## Component or Module Boundaries

| Component | Responsibility | Bounded state |
| --- | --- | --- |
| `VerificationRing` | Sign current epoch; verify current/previous epochs | exactly `maxKeys` secrets |
| `ledger_binding` | Bind team/cell/policy and fence retired producer namespaces | one current ledger epoch |
| `producer_heads` | Fence epoch, allocate next sequence, retain watermark | `MaxProducers` rows |
| receipt segments | Replay bounded terminal outcomes by computed segment key | global/per-producer caps and metadata value cap |
| `ledger.Execute` | Verify, decide, mutate, receipt, and head CAS | one transaction |
| `Compact` | Atomically watermark and delete eligible prefixes | no side state |
| `appendBatch` | Accelerate storage soak only | batch-size memory; not production execution |

## Interface Contracts

Identity is `(project_id, ledger_epoch, producer_id, subject_hash,
producer_epoch, sequence, request_hash)`. A producer head binds the subject for
the current ledger epoch. `key_epoch` authenticates the envelope but is
excluded from idempotency identity. Domain results map as follows:

| Disposition | API behavior | Mutation permitted? |
| --- | --- | --- |
| `executed` | 200 terminal result | exactly once in canonical UOW |
| `replay` | 200 stored terminal result | no |
| `gone` | 410 permanent expiration | no |
| `conflict` | 409 same sequence/different digest | no |
| `sequence_gap` | 425 or 409 with expected sequence | no |
| `producer_fenced` | 409/412; activate epoch through control plane | no |
| `unauthorized` | 401/403 | no |
| capacity error | 429/503 plus `Retry-After`; never 202 | no |

The production storage contract must expose a UOW operation that reads/CASes
the producer head and writes the bounded receipt atomically with the issue
mutation. A separate control database cannot provide this guarantee.

## Trade-offs

- Strict sequences simplify compaction and make holes impossible, but require
  one sequence allocator per producer. Multiple agents should use distinct
  producers.
- Keeping a producer watermark forever prevents ancient replay, but consumes a
  row until an explicit repository-ledger epoch rotation. Rotation reclaims all
  heads at once but deliberately ends the retry horizon for every old request.
- A minimum retry horizon improves client recovery. Under an abnormal burst it
  can force temporary backpressure before any receipt is old enough to delete.
- A metadata-embedded ring avoids a schema migration but rewrites a bounded
- Segmented metadata avoids a schema migration and keeps lookups direct, but
  rewrites the active segment. A relational table is more query-efficient but
  requires a deliberate driver/schema decision.
- Re-signing with the current key keeps retries alive across rotations. A
  request signed only by an evicted key is unauthorized, not `gone`.

## Rejected Alternatives

- **TTL deletion without a watermark:** rejected because an old retry becomes
  indistinguishable from a new request and can execute twice.
- **Random idempotency keys only:** rejected because finite storage cannot
  remember an unbounded random-key set.
- **Bloom filters:** rejected because false positives discard legitimate work
  and false-negative-free expiry still needs an ordered freshness boundary.
- **Gateway-local SQLite as canonical truth:** rejected for production because
  host loss separates the receipt from the Dolt mutation.
- **Central receipt database for all repos:** rejected because it cannot commit
  atomically with independent repository databases.
- **Deleting individual producer heads after inactivity:** rejected. The safe
  bounded alternative is atomic repository-ledger epoch rotation; selective
  deletion still needs independently proven identity revocation.
- **Unlimited storage plus periodic GC:** rejected because it has no admission
  bound and turns cleanup failure into an outage or disk leak.

## Migration / Rollback Plan

1. Expand: gateway accepts the new producer envelope but keeps the existing
   deterministic idempotency path authoritative.
2. Shadow: compute producer decisions and compare them with existing receipts;
   do not compact.
3. Dual-read: write bounded producer heads/receipts in the same synthetic Dolt
   UOW, and fail the operation if either canonical representation fails.
4. Prove: crash/partition/8-writer tests, key rotation, 24-hour soak, Dolt GC,
   restore, and one repository moved between team cells.
5. Activate: new clients use producer sequences; legacy keys remain read-only
   for the published retry horizon.
6. Compact only after the dual-read mismatch count is zero for the full retry
   horizon.
7. Contract: retire legacy receipt keys after backup/restore proof.

Rollback before step 6 is disabling the producer path. After compaction starts,
rollback must preserve producer watermarks; restoring a database snapshot older
than the latest watermark requires replaying the canonical operation log before
serving writes.

## Risks & Validation

### Scope

State-machine behavior, crash atomicity, storage cardinality, file high-water
behavior, heap behavior, concurrency within one process, key rotation, binding,
and fail-closed capacity.

### Assumptions

SQLite `WAL` plus `synchronous=FULL` is an ACID oracle only. Dolt must repeat
the same tests with separate processes and real UOW conflicts.

### Recommended Tests or Gaps

- Implemented: replay/conflict/gap/gone paths and zero mutation on every
  non-execute decision.
- Implemented: eight concurrent same-request writers mutate exactly once.
- Implemented: process exit after watermark updates, after receipt deletes,
  before commit, and after commit; reopen invariants are exact.
- Implemented: retry-horizon capacity backpressure and later recovery.
- Implemented: producer epoch fence, hard producer quota, team/policy binding,
  subject binding, repository-ledger epoch reclamation/fence, three-key
  rotation ring, SQLite quick check, and contiguous receipt invariant.
- Implemented: configurable million-operation storage soak with heap and DB/WAL
  samples plus compacted/replay/conflict probes. The soak uses a one-minute
  retry horizon to force many compaction cycles over 27.8 logical hours; it is
  a boundedness test, not the production retry-window sizing test.
- Required in GCP: two gateway processes racing the same and adjacent
  sequences against Dolt; kill -9 at UOW boundaries; leader/fencing loss;
  network partition; standby promotion; restore to an older snapshot; KMS/key
  unavailability; metadata-ring versus relational-ledger performance; Dolt
  commit-history and GC disk plateau over 24 hours; and a 24-hour logical retry
  window with the measured busy/burst workload.

For the measured hot-repository assumptions, a 24-hour sizing example is:

```text
busy:  1.263 writes/s * 86,400 s = 109,123 receipts
burst: (8.52 - 1.263) writes/s * 300 s = 2,177 extra receipts
reserve: (109,123 + 2,177) * 1.20 = 133,560 -> cap at 150,000
worst sustained burst: 8.52 * 86,400 * 1.20 = 883,354 -> emergency cap 1,000,000
```

At a 4 KiB outcome cap, the active payload ceiling is 600 MB for the realistic
150,000-receipt policy, before index/version overhead. The sustained-burst
million-receipt policy is an emergency envelope, not the normal target; reaching
it should page and shed bulk work.

### Priority or Risk

P0 is proving business mutation + receipt + head CAS in one real Dolt UOW.
P0 is also restore fencing: a stale restored watermark must never serve writes.
P1 is Dolt history/GC storage plateau; table cardinality alone does not bound
version history. P1 is cell-move fencing and key-rotation failure injection.

### Framework Notes

Use the repository's Go test stack with `-tags gms_pure_go`; race coverage uses
the same package with `-race`. The crash test spawns the test binary so SQLite
recovery occurs after an actual process exit, not merely a returned error.

### Follow-up Work

Translate the state machine into the gateway's canonical Dolt metadata UOW,
then run the listed multi-process tests in the disposable GCP lab. Do not merge
the SQLite oracle into Beads core.

## Decision Log

| Decision | Recommendation | Why | Risk | Mitigation |
| --- | --- | --- | --- | --- |
| Retry boundary | producer epoch + monotonic sequence | finite watermark rejects ancient retries | strict ordering | one producer per agent/workstream |
| Canonical location | same repo DB/UOW as mutation | atomic exactly-once effect | per-repo state | team cell catalog and quotas |
| Compaction | contiguous prefix to watermark | old retry is permanently 410 | no exact old result | published retry horizon |
| Capacity | hard caps + backpressure | no storage leak | temporary write rejection | size from measured rate/horizon + alert |
| Producer tombstones | keep until repository epoch rotates | no identity resurrection | quota consumption | size quota; rotate the whole epoch, never one row |
| Tombstone reset | rotate one repository-ledger epoch | bounded churn without per-producer revocation set | ends all old retries | rare signed control-plane migration |
| Keys | three-epoch verification ring | bounded rotation overlap | old signatures expire | re-sign retry with current key |
| Initial persistence | namespaced metadata head/segments | charter-compatible | rewrite amplification | measure, then widen driver if needed |
| Metadata layout | head plus fixed-size immutable segments | O(1) retry lookup and bounded values | active-segment rewrite | benchmark segment sizes 4/8/16 |

## Confidence

High for the state-machine invariants and SQLite crash/storage evidence. Medium
for the metadata-embedded production mapping. Low until the Dolt multi-process,
restore-fencing, and commit-GC experiments pass in the disposable lab.

## Architecture Quality Scorecard

- Output Completeness: 2
- Scope Discipline: 2
- Technical Specificity: 2
- Evidence Quality: 1
- Failure-Aware Decisions: 2
- Migration Clarity: 2
- Benchmark Fit: 2
- Overall Score: 13
- Pass: true
- Rationale: Dolt and multi-process evidence remains intentionally unclaimed.

## Running the Evidence

```bash
go test -tags gms_pure_go ./scripts/bench-remote-server/idempotency-retention
go test -tags gms_pure_go -race ./scripts/bench-remote-server/idempotency-retention
go run -tags gms_pure_go ./scripts/bench-remote-server/idempotency-retention \
  --path /private/tmp/new-retention-soak.db \
  --output /private/tmp/new-retention-soak.json \
  --operations 1000000 --producers 400 --batch 1000 \
  --sample-every 100000 --logical-rate 10 --outcome-bytes 256
```

Both output paths must be new. The command refuses to adopt or overwrite an
existing store or evidence file.
