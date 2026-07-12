# Real-Dolt Bounded Idempotency Proof

## Problem Framing

The SQLite oracle proved the state machine, but not that Dolt commits a
synthetic business mutation, receipt, producer-head update, and compaction
watermark as one durable unit under concurrent processes and process death.
This package supplies that missing single-node Dolt evidence without touching
Beads core, the gateway prototype, EngOS, production data, or a production
endpoint.

## Objective

Prove on real Dolt that:

- two processes using the same producer sequence create exactly one business
  row, one canonical receipt, and one Dolt commit;
- two processes that both read one remaining producer or receipt slot cannot
  overshoot the stored cap;
- stored producer, receipt, and outcome limits are enforced inside immutable
  binary ceilings;
- adjacent sequences cannot pass a missing predecessor;
- producer and repository epochs permanently fence old requests;
- transaction rollback removes business, receipt, sequence, and ledger-counter
  changes;
- compacted retries are `Gone` and never mutate;
- process death at practical operation and compaction boundaries recovers to
  one legal state;
- an independently launched inspector can reproduce the orchestrator's exact
  canonical state, capacity counters, and commit counts.

## In Scope

- Numeric loopback `127.0.0.1:13360` only.
- A pre-existing exact `beads_perf_lab_control.lab_identity` singleton must
  declare `environment=synthetic` and the fixed lab UUID before any action.
- Proof setup additionally requires explicit `--ack-synthetic-lab`.
- Database `beads_perf_lab_retention_ae` only.
- Fixed synthetic table names, producers, payload prefix, lab UUID, caps, and
  operation-derived IDs.
- UUID-namespaced proof runs so failed evidence is retained rather than reset.
- Real Dolt 2.1.10 transactions, conflicts, commits, branches, and time-local
  process termination.

## Out of Scope

- EngOS, Beads production schemas, existing project databases, snapshots,
  passwords, production users, and public/network endpoints.
- Production deployment or an upstream patch.
- Dolt server-process death, disk loss, multi-zone replication, partitions,
  leader promotion, actual snapshot restore, KMS, 24-hour retention load, and
  Dolt history/GC plateau.

## Assumptions & Constraints

- The lab server is already running and loopback-only. The command uses
  passwordless root solely on this disposable endpoint and stores no secret.
- The external catalog supplies the expected repository epoch. A restored
  database is not trusted to declare its own freshness.
- Each producer owns a contiguous sequence. Different concurrent agents use
  different producer identities.
- All business effects tested here are rows in the same Dolt transaction.
  External effects require a transactional outbox.
- Serialization errors 1205/1213 retry the entire transaction with a fresh
  connection. No other database error is silently retried except duplicate-key
  convergence, which re-reads canonical state.
- Dolt/go-mysql-server does not provide useful serialization through `SELECT
  ... FOR UPDATE`. Capacity authority is therefore a mutated, versioned ledger
  row, never a preceding `COUNT(*)`.

## Architecture Recommendation

Retain the Fenced Segmented Producer Log contract and implement its storage
adapter inside the same Dolt unit of work as the Beads mutation:

```text
recompute payload digest and canonical operation identity
  -> verify signature over payload, digest, operation identity, and epochs
  -> compare external expected repository epoch with canonical epoch
  -> read stored limits, counters, repository epoch, and ledger_version
  -> read producer subject, producer epoch, next sequence, watermark
  -> old epoch or sequence <= watermark: Gone, rollback read transaction
  -> retained same digest: Replay, rollback read transaction
  -> retained different digest: Conflict
  -> future sequence: Gap
  -> exact next sequence:
       CAS receipt_count + 1 and ledger_version + 1 below stored cap
       INSERT business effect
       INSERT immutable receipt
       CAS producer next_sequence
       CALL DOLT_COMMIT
```

Producer registration reserves `producer_count` with the same ledger CAS before
inserting the head. Compaction updates `compacted_through`, deletes the same
contiguous receipt prefix, and CAS-decrements `receipt_count` before one
`DOLT_COMMIT`. Producer-epoch rotation decrements the exact deleted receipt
count. Repository-epoch rotation verifies physical/counter parity, deletes the
active namespace, and CAS-resets both counters with the epoch in one
transaction. Production should fence the epoch first and clean one bounded
retired generation incrementally if the namespace is too large for one commit.

## Component or Module Boundaries

| Component | Responsibility | Mutation authority |
| --- | --- | --- |
| `keys.go` | Fixed synthetic bounded verification ring | none |
| `db.go` | Endpoint/database guard, synthetic schema, run initialization | approved lab DB only |
| `ledger.go` | Stored-policy validation and shared-row optimistic CAS | one UUID run ledger |
| `operation.go` | Canonical decision and atomic business/receipt/head UOW | one run and producer |
| `management.go` | Producer compaction/epoch and repository epoch fences | one UUID run |
| `process.go` | Separate workers and bounded process-kill coordination | client processes only |
| `inspect.go` | Fixed canonical row/commit inspection | read-only |
| `proof.go` | Scenario orchestration and promotion gate | calls fixed interfaces only |

## Interface Contracts

| Result | Meaning | Mutation/commit |
| --- | --- | --- |
| `executed` | exact next sequence committed | exactly one |
| `replay` | identical retained receipt | none |
| `gone` | compacted sequence or retired epoch | none |
| `conflict` | prior sequence bound to another digest | none |
| `sequence_gap` | predecessor is not terminal | none |
| `fenced` | expected epoch is ahead of stale database | none |
| `unauthorized` | signature or subject rejected | none |
| `capacity_exhausted` | stored producer, receipt, or outcome cap reached | none |

An acknowledged execution exists only after `CALL DOLT_COMMIT` returns. A
client loss after that point retries the same identity and receives `replay`.

## Trade-offs

- Every capacity-changing transaction mutates the same per-run ledger row.
  This makes the cap real under Dolt but deliberately serializes disjoint
  admissions within one capacity domain. Production can segment ledgers only
  if it pre-allocates a strict parent budget; independent child counts without
  a parent reservation would recreate the overshoot bug.
- Repository epoch fencing makes stale restore safe only when the expected
  epoch comes from a stronger external catalog.
- UUID run namespaces avoid destructive resets and preserve failed evidence,
  but the lab database accumulates bounded test runs until TTL cleanup.
- Direct relational tables are ideal as an oracle. Production still needs a
  decision between segmented metadata and a widened driver-owned ledger.

## Rejected Alternatives

- **Gateway-local receipt authority:** cannot share a transaction with Dolt.
- **Retrying only `DOLT_COMMIT`:** a serialization failure requires a fresh
  transaction and canonical re-read.
- **Trusting the restored database epoch:** a stale snapshot would accept old
  requests; the expected epoch must be external.
- **Deleting receipts without a watermark:** permits duplicate execution.
- **`COUNT(*)` then disjoint inserts:** both transactions can observe one free
  slot because Dolt's `FOR UPDATE` is a no-op; only a shared-row mutation/CAS
  makes the limit atomic.
- **Trusting a configured maximum without reading it:** lets code silently use
  a larger compile-time default. Stored policy is read every attempt and is
  rejected if it exceeds the binary ceiling.
- **Signing a caller-supplied digest without recomputing it:** allows the
  payload and claimed request identity to diverge. Validation recomputes the
  payload SHA-256 and operation ID, while the v2 HMAC covers the payload and
  both canonical identities.
- **Mocked or goroutine-only concurrency:** cannot establish real server
  serialization behavior.
- **Simulated error returns instead of process death:** misses connection-close
  rollback and post-commit response-loss behavior.

## Migration / Rollback Plan

1. Keep this package as the storage oracle outside Beads core.
2. Add a gateway storage adapter using namespaced metadata segments and a
   versioned per-repository capacity ledger in one real Beads/Dolt UOW.
3. Shadow decisions against existing deterministic receipts without compacting.
4. Run the same scenarios with two gateway processes in the disposable GCP
   lab, then add leader/standby, partition, actual restore, and GC tests.
5. Activate producer envelopes for synthetic clients only.
6. Start compaction only after a full retry horizon has zero decision mismatch.

The isolated lab migrated schema v1 to v2 by adding counters/version, deriving
each legacy run's counters from physical rows, and only then advancing the lab
identity. A crash while the identity remains v1 re-runs the backfill. This is
lab migration evidence, not authorization to alter a production schema.

Before compaction, rollback disables the new adapter. Afterwards, rollback must
preserve repository and producer watermarks plus ledger counters. Restoring
below the external repository epoch must remain fail-closed.

## Risks & Validation

Final retained run: `c26c8325-f640-451a-bef6-09d30f86388d`.
This run is authoritative for the package. It repeats the complete v2 proof
from a source-manifested binary after verifying the exact pre-existing
`beads_perf_lab_control.lab_identity` singleton before setup, proof, and
inspection. The older
`9a2ef6c1-0e52-4d43-99d6-d17294b32cad` and
`fad687fb-99f1-4308-a46d-ac2a350c4bd3` evidence is diagnostic and superseded.
The first used the racy `COUNT(*)` admission check. The second fixed admission
but preceded the v2 canonical payload/hash/signature binding. Run `2d258235`
proved both repairs together but preceded the server-control gate; it is now
superseded by `c26c8325`.

- Dolt: 2.1.10; MySQL compatibility: 8.0.31.
- Two same-key processes: one `executed`, one `replay`; the loser used one real
  serialization retry.
- Deterministic producer boundary: both processes paused after reading 10 of
  stored maximum 11, then released together. Exactly one registered; the loser
  retried after serialization and returned `producer_capacity_exhausted`.
- Deterministic receipt boundary: both processes paused after reading 7 of
  stored maximum 8, then released together. Exactly one business/receipt/commit
  appeared; the loser retried and returned `receipt_capacity_exhausted`.
- A 257-byte outcome was rejected against the stored 256-byte limit although
  it was below the 4096-byte binary ceiling; no row, commit, head, counter, or
  ledger-version mutation occurred.
- Tamper tests prove that changing the payload invalidates the v2 HMAC, a
  correctly signed caller-supplied digest that disagrees with the payload is
  rejected by server recomputation, and a signed noncanonical operation ID is
  rejected.
- Canonical counts: one business row, receipt, and operation commit.
- Sequence 2 before sequence 1: `sequence_gap`; both executed in order later.
- Producer epoch 1 after epoch 2 activation: `gone`.
- Injected rollback: zero business rows, receipts, commits; next sequence stayed
  1; the receipt counter/version did not change; restart executed once.
- Compaction process death after watermark update and after receipt deletion:
  both reopened with watermark 0, both receipts, and unchanged shared counter
  and version. Committed compaction moved watermark to 1 and decremented the
  counter by one; sequence 1 became `gone`; sequence 2 replayed.
- Worker death after `START TRANSACTION`: zero rows/commits; restart executed.
- Worker death after all mutations but before `DOLT_COMMIT`: zero rows/commits;
  unchanged counter/version; restart executed.
- Worker death after `DOLT_COMMIT` but before response: one row/receipt/commit;
  restart replayed.
- Writable stale branch at the exact epoch-1 head was fenced by external epoch
  2 before mutation. Epoch-1 client against main returned `gone`.
- Repository epoch reset atomically changed physical and stored producer/receipt
  counts from 11/8 to 0/0; epoch-2 registration and execution rebuilt 1/1.
- Independent inspector reproduced state exactly except its timestamp: epoch 2,
  one current producer, one current receipt, matching stored counters, 12
  business effects, 18 run commits, zero dirty tables, and exactly one commit
  per business operation.

Evidence:

- `/private/tmp/beads-fleet-scale-20260712/retention-dolt-proof-c26c8325.json`
- `/private/tmp/beads-fleet-scale-20260712/retention-dolt-inspection-c26c8325.json`
- `/private/tmp/beads-fleet-scale-20260712/retention-dolt-source-c26c8325.sha256`
- `/private/tmp/beads-fleet-scale-20260712/retention-dolt-c26c8325.sha256`

## Scope

Unit guards, real single-node Dolt integration, deterministic two-process
same-key and exact-cap concurrency, stored-policy enforcement, rollback,
compaction, key rotation, worker-kill recovery, stale-restore branch, and
independent canonical/counter inspection.

## Assumptions

The stale branch is a writable restore analogue, not a disk/snapshot restore.
Worker process death is real; Dolt server death is not included.

## Recommended Tests or Gaps

P0 next tests:

- Two gateway instances against a primary/standby pair.
- Kill primary during `DOLT_COMMIT`, promote standby, retry same operation.
- Partition client, gateway, primary, and standby independently.
- Restore an older disk snapshot while the signed catalog retains epoch 2.
- Rotate KMS keys while old retained and compacted retries arrive.
- Run metadata segment sizes 4/8/16 at the realistic 24-hour cap.
- Track Dolt table size, commit history, GC, and restore time for 24 hours.

## Priority or Risk

P0: external catalog durability and restore fencing, server ambiguous-commit
recovery, and HA leader fencing. P1: storage/GC plateau and segment performance.
P2: least-privilege/KMS operational polish after correctness.

## Framework Notes

Use `go test -tags gms_pure_go`, the race detector, and `go vet`. The proof
binary accepts no endpoint/database flags and exposes no arbitrary SQL.

## Follow-up Work

Promote this exact scenario matrix to the disposable GCP multi-node lab. Do not
promote the prototype to production or upstream Beads yet.

## Decision Log

| Decision | Recommendation | Evidence | Remaining risk |
| --- | --- | --- | --- |
| Atomic authority | same Dolt UOW | rollback and kill tests pass | server-loss ambiguity |
| Capacity concurrency | shared ledger counter/version CAS | deterministic producer and receipt at-cap races each admitted exactly one | per-ledger write hotspot |
| Request concurrency | fresh transaction retry on 1205/1213 | real loser retried once and replayed | hot-producer serialization |
| Compaction | watermark, prefix delete, and counter decrement in one commit | two real kill points rolled back with unchanged counters | large-prefix cost |
| Restore | external repository epoch | stale writable branch fenced | external catalog HA |
| Promotion | GCP multi-node proof only | independent inspection passes | no production approval |

## Confidence

High for single-node Dolt atomicity, exact stored hard caps, counter/row parity,
concurrency, worker-death recovery, compaction rollback, and epoch decisions.
Medium for the production metadata mapping. Low for HA, actual restore,
server/disk loss, and long-horizon storage until the GCP tests pass.

## Architecture Quality Scorecard

- Output Completeness: 2
- Scope Discipline: 2
- Technical Specificity: 2
- Evidence Quality: 2
- Failure-Aware Decisions: 2
- Migration Clarity: 2
- Benchmark Fit: 2
- Overall Score: 14
- Pass: true
- Rationale: The recommendation is evidence-backed but explicitly stops before production promotion.

## Commands

```bash
go test -tags gms_pure_go ./scripts/bench-remote-server/idempotency-retention-dolt
go test -tags gms_pure_go -race ./scripts/bench-remote-server/idempotency-retention-dolt
go vet -tags gms_pure_go ./scripts/bench-remote-server/idempotency-retention-dolt
```

The proof and inspector require explicit loopback-network permission. Output
files are created with mode `0600` and must not already exist.
