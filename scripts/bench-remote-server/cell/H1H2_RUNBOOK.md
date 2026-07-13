# H1/H2 lab runbook (synthetic, loopback)

## H1c terminal multi-cell (PASS 2026-07-13)

1. Start Dolt multi-db on `127.0.0.1:13360` with `beads_perf_lab_cell_{0,1,2}`.
2. `bd init --server --external --server-port 13360 --database beads_perf_lab_cell_N` per workspace.
3. Set `metadata._project_id` to synthetic UUIDs.
4. `lab-bootstrap` with 0600 password files under `/private/tmp/beads-perf-lab-runtime`.
5. Start one gateway per DB on ports 7707–7709 (`-runtime-root /private/tmp/beads-perf-lab-runtime -subject lab-agent-N`).
6. `gateway-client` terminal-operations `issue.create`.

Proven: 3-cell create; same-key idempotent replay; conflicting body → HTTP 409.

## H2 simulated RTT (PASS under SLO, co-located)

Client `-simulated-rtt-ms` delay model (not netem/WAN). At 150 ms (n=10, fresh issues for mutating ops):

| Op | p50 | p95 | SLO |
| --- | --- | --- | --- |
| ping | ~156 ms | ~156 ms | < 1500 ms |
| create | ~179 ms | ~190 ms | < 3000 ms |
| update | ~179 ms | ~190 ms | < 3000 ms |
| claim | ~180 ms | ~181 ms | < 3000 ms |
| close | ~200 ms | ~201 ms | < 3000 ms |

Evidence: `h1-freeze/h2-ops-matrix/h2-ops-scorecard.json` (decision `H2_OPS_MATRIX_150MS_PASS`).

Still **GATEWAY_ARCHITECTURE_REQUIRED** for production: no real WAN path, incomplete fleet identity/HA/soak, not full command matrix (batch/graph/dep not in this scorecard).
