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
