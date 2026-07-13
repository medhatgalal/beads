# H1/H2 lab runbook (synthetic, loopback)

## H1c terminal multi-cell (PASS 2026-07-13)

1. Start Dolt multi-db on `127.0.0.1:13360` with `beads_perf_lab_cell_{0,1,2}`.
2. `bd init --server --external --server-port 13360 --database beads_perf_lab_cell_N` per workspace.
3. Set `metadata._project_id` to synthetic UUIDs.
4. `lab-bootstrap` with 0600 password files under `/private/tmp/beads-perf-lab-runtime`.
5. Start one gateway per DB on ports 7707–7709.
6. `gateway-client` terminal-operations `issue.create`.

Proven: 3-cell create; same-key idempotent replay; conflicting body → HTTP 409.

## H2 simulated RTT (PASS under SLO, co-located)

Client `-simulated-rtt-ms` delay model (not netem/WAN). At 150 ms:

| Op | p95 | SLO |
| --- | --- | --- |
| ping | ~157 ms | < 1500 ms |
| create | ~191 ms | < 3000 ms |

Still **GATEWAY_ARCHITECTURE_REQUIRED** for production: no real WAN path, incomplete fleet identity/HA/soak, not full command matrix.
