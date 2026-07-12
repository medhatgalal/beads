# Remote Server Lab — Evidence Index

This directory records how to interpret and reproduce the Beads remote-server performance laboratory without requiring access to any single operator workstation.

**Terminal verdict:** `GATEWAY_ARCHITECTURE_REQUIRED`  
**Production ready:** no  
**Do not** combine historical gateway latency with current hardened gateway source without a rerun.

## Documents

| Path | Role |
| --- | --- |
| [`docs/performance/remote-server-lab.md`](../../docs/performance/remote-server-lab.md) | Problem, SLOs, hypothesis matrix, results, verdict |
| [`docs/design/remote-server-gateway.md`](../../docs/design/remote-server-gateway.md) | Contracts, threat model, migration, promotion gates |
| [`evidence/remote-server-lab-provenance.json`](evidence/remote-server-lab-provenance.json) | Compact machine-readable provenance |
| [`evidence/experiment-ledger-decisions.json`](evidence/experiment-ledger-decisions.json) | F00–F36 decisions without large blobs |
| [`evidence/beads-fleet-scale-5min.html`](evidence/beads-fleet-scale-5min.html) | Five-minute decision brief |
| [`evidence/beads-fleet-scale-final-evidence.sha256`](evidence/beads-fleet-scale-final-evidence.sha256) | 104-entry final packet manifest |
| [`evidence/retention-dolt-c26c8325.sha256`](evidence/retention-dolt-c26c8325.sha256) | Real-Dolt retention sub-manifest |
| [`evidence/realistic-workload-scorecard.json`](evidence/realistic-workload-scorecard.json) | Realistic workload scorecard |
| [`evidence/realistic-workload-goal-contract.md`](evidence/realistic-workload-goal-contract.md) | Goal contract text |

## Checksums (authoritative)

These are hashes of the **original lab packet artifacts** (for provenance). Committed HTML and manifest files are byte-identical to those originals.

| Artifact | SHA-256 |
| --- | --- |
| Final evidence manifest file | `116894101563bbf34f057fd3c1257df6714ea298b80294944c50221d04a335f1` |
| Five-minute HTML | `1a63e08a3182ffbd8ca3031e17be1c3a7eedab26817d97940483c094a8393a25` |
| Original experiment ledger (`experiment-ledger.jsonl`, operator archive) | `87e2289313d30717f13c919a9dd9e75b8c49ae4d0e1e048cc583e60bc9c87582` |
| Retention sub-manifest file | `c70257657add914c1798b55b255ee39416b4e8bb6edd922d66a74bb77cfe2fbb` |

Verify committed copies that must match the original packet:

```sh
cd scripts/bench-remote-server/evidence
shasum -a 256 beads-fleet-scale-5min.html \
  beads-fleet-scale-final-evidence.sha256 retention-dolt-c26c8325.sha256
```

Expected:

- HTML → `1a63e08a…`
- final manifest file → `11689410…`
- retention manifest file → `c7025765…`

The in-repo `experiment-ledger-decisions.json` is a sanitized compact extract of F00–F36 and is **not** byte-identical to the original jsonl.

## Source commits

| Role | Commit |
| --- | --- |
| Upstream / lab base used for many matrices | `64a136d56e8ae2b89071e57f90f57255e56c9ad9` |
| Operator gateway research head | `de720d7c0b0fc66df26b19fa809d625a19cb7e9a` |
| Operator UOW parity head | `41a82cbf765fce177e3f15d8e27c52ab6559751c` |
| UOW contributor base (upstream PR lineage) | `7d7b064f1…` |

**Boundary:** Historical gateway p95 success is tied to base `64a136d56…` harness/prototype context. It is **not** a performance certificate for `de720d7c0…`.

## What is in this git tree vs full packet

### In git (this PR)

- Narrative RFC docs.
- Compact provenance JSON.
- Decision ledger and sanitized scorecards.
- Manifest **files** (lists of relative paths + hashes) and the small HTML/report that fit review.

### Not in git (by design)

- Full 2.8 GiB lab packet (`gocache`, raw Dolt data dirs, large JSON matrices, binaries).
- Production credentials, production DSNs, EngOS trees.
- `scripts/bench-remote-server/cluster-lab` (unsafe root/global mutation surface; excluded).

The full multi-gigabyte lab packet (raw Dolt dirs, build caches, binaries, large matrices) is **not** in git and is **not** required to review this PR. Reviewer-accessible proof is limited to files in this tree. Operators who retain a private durable archive can re-verify the 104-entry and retention manifests offline; absolute workstation paths are intentionally omitted from this document.

## Key measured facts (quick)

**Direct 150 ms p95 (failed SLOs):** ping 2.481 s, list 3.853 s, ready 3.255 s, show 2.938 s.

**Historical gateway 150 ms p95 (passed SLOs; historical source):** ping 162 ms, list 187 ms, ready 203 ms, show 160 ms, create 228 ms, update 206 ms, close 298 ms, graph 100/200 7.50 s.

**Current-source retention oracle:** run `c26c8325-f640-451a-bef6-09d30f86388d`, 11/11 scenarios, zero dirty tables, binary rebuild identical  
`edcc8802eaea66516a95ffc031bdb50ef470c25b57477738aaf8d444f549a447`.

## Reproduction posture

1. Use synthetic databases only; never production endpoints.
2. Freeze commit, binary hash, fixture, RTT, and identity set before a run.
3. Run correctness before performance.
4. Record accept/reject in an append-only ledger.
5. Stop a failed hypothesis after two attempts or no material gain.
6. Never stage or execute `cluster-lab`.

## Operator draft PRs

- UOW correctness research: <https://github.com/medhatgalal/beads/pull/1>
- Gateway laboratory research: <https://github.com/medhatgalal/beads/pull/2>

Neither is merge-ready upstream. This evidence/RFC change is intentionally documentation-first.

## Related upstream work (do not supersede silently)

Coordinate rather than overwrite contributor history when overlapping UOW/storage work lands (including lineage around upstream PR #4675 and other remote-server discussions). Preserve attribution and tests.


## Non-claims (must not be read as production readiness)

This package does **not** claim:

1. `PERFORMANCE_FIXED` or a production-ready gateway.
2. Current-source latency proof for hardened gateway head `de720d7c0…`.
3. An integrated multi-database multi-identity cell.
4. 100–300 repository fleet capacity or multi-zone HA/RTO.
5. Merge readiness of operator research Draft PRs #1 or #2.
6. A charter decision that the production cell must live in core Beads versus an external gateway repository.
7. Reviewer ability to re-download the full multi-gigabyte lab packet from GitHub.

Positive claim allowed: the lab supports the **architectural discriminator** that direct WAN SQL is the wrong interactive boundary; a near-data high-level operation plane is the candidate direction.
