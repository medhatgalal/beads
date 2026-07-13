# Integrated repository cell (lab)

**Status:** working lab software under construction  
**Verdict:** still `GATEWAY_ARCHITECTURE_REQUIRED` until current-source promotion gates pass  
**Base:** fork branch `AI/perf-remote-server-integrated-cell` (UOW fresh-retry + selective gateway packages)

## What this is

One lab cell path that will route terminal-v2 operations across multiple
synthetic repository databases using:

- signed fleet catalog (`fleet-catalog`)
- bounded fair scheduler (`fleet-scheduler`)
- hard-capped lazy pools (`fleetlazy`)
- near-data gateway terminal service (`gateway`) with multi-project router
- DirectDoltServer UOW provider + PR #4 fresh-UOW retries

## What this is not

- Not production
- Not `cluster-lab`
- Not a promotion of broad Draft PR #2
- Not OIDC/mTLS complete identity
- Not multi-zone HA

## Current proof surface

| Package | Status |
| --- | --- |
| `fleet-catalog` | unit tests pass |
| `fleet-scheduler` | unit tests pass |
| `fleetlazy` | unit tests pass (library extract) |
| `gateway` | unit tests pass + multi-project router |
| `internal/storage/uow` DirectDoltServer | validation tests pass |
| H1 3-repo loopback end-to-end | next |
| H2 150 ms GCP | lab VM RUNNING with Go 1.24.4 + Dolt 2.1.10 |

## Freeze protocol

Before citing numbers for promotion:

1. `git rev-parse HEAD` on this branch
2. `SOURCE_MANIFEST` sha256 of tree under `scripts/bench-remote-server/{cell,gateway,fleet*,fleetlazy,internal}`
3. `BINARY_MANIFEST` sha256 of built `beads-perf-cell` / gateway / inspect
4. Evidence JSON must embed both manifest hashes

## Run unit gates

```sh
CGO_ENABLED=1 go test -tags gms_pure_go ./scripts/bench-remote-server/fleet-catalog/ \
  ./scripts/bench-remote-server/fleet-scheduler/ \
  ./scripts/bench-remote-server/fleetlazy/ \
  ./scripts/bench-remote-server/gateway/ \
  ./internal/storage/uow/
```

## H1 status (2026-07-13)

**Hypothesis:** three synthetic repositories can be routed and pooled under a
hard budget of two resident pools with signed catalog epoch fencing, without
production endpoints.

**Result:** PASS (composition)

```sh
CGO_ENABLED=1 go test -tags gms_pure_go -count=1 ./scripts/bench-remote-server/cell/
```

Gates proven:

1. Signed catalog resolves 3 repos; stale `database_epoch` fails closed.
2. Multi-project HTTP router dispatches by project id; unknown project 404.
3. Lazy pool budget=2: max resident ≤ 2; hibernate to zero cold pools.

**Not proven by H1 (still open):**

- Terminal-v2 exactly-once against real multi-DB Dolt (H1b)
- 150 ms current-source latency matrix (H2)
- Production readiness

Freeze manifests: write via `cell.WriteFreezeManifest` (see `freeze_test.go`).
