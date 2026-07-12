# Fleet effect inspector

`fleet-inspect` is an independent, read-only correctness gate for a complete
`realistic-fleet` schema-v2 report. It accepts no SQL input. Its MySQL and
SQLite statements are fixed in the binary, parameterized, and bounded.

## Usage

```sh
go run ./scripts/bench-remote-server/fleet-inspect \
  --inventory /private/tmp/beads-perf-lab-runtime/private/fleet-inventory.json \
  --runner-report /private/tmp/beads-perf-lab-runtime/results/realistic-run.json \
  --output /private/tmp/beads-perf-lab-runtime/results/fleet-inspection.json
```

Inventory files must be `0600` (or stricter), regular, non-symlink files. Every
path is absolute. Password files, SQLite queue files, and existing SQLite WAL
or SHM sidecars must also be private regular files with no symlink component.
Only numeric loopback hosts are accepted; ports `3306` and `3307` and every
database name outside `beads_perf_lab_*` are rejected. Each connection derives
its complete 32-hex SQL user from the target's canonical `project_id`; no
shared SQL principal or user field is accepted from inventory.

```json
{
  "schema_version": 1,
  "lab_id": "dc6020ef-b433-41b6-9426-cedd9bf40502",
  "targets": [
    {
      "host": "127.0.0.1",
      "port": 13360,
      "database_id": "beads_perf_lab_realistic_ae",
      "project_id": "093828a8-233f-4fc9-a8b7-5b5f77a48c0b",
      "password_file": "/private/tmp/beads-perf-lab-runtime/local/dolt-password",
      "queue_path": "/private/tmp/beads-perf-lab-runtime/local/ae-operations.db"
    },
    {
      "host": "127.0.0.1",
      "port": 13360,
      "database_id": "beads_perf_lab_realistic_repo_a",
      "project_id": "80d22c54-066c-4f89-b6e8-d94e19b4e902",
      "password_file": "/private/tmp/beads-perf-lab-runtime/local/dolt-password",
      "queue_path": "/private/tmp/beads-perf-lab-runtime/local/repo-a-operations.db"
    }
  ]
}
```

## Preconditions and caveats

- Stop workload producers and gateways before inspection. Checkpoint every
  SQLite WAL first. Otherwise the report and queue/Dolt snapshots are not one
  atomic point in time, and inspection is expected to fail closed.
- The runner report must retain every result sample and every successful write
  operation ID. Any drop, failure, ambiguity, duplicate, missing ID, truncated
  reservoir, or sampled-away record rejects the report before database access.
- Each queue is opened read-only. Each Dolt database is inspected with one
  connection; at most eight databases are inspected concurrently.
- Durable marker scans are capped at 100,000 records per marker class per
  database. A long-lived lab that exceeds the bound must rotate to a fresh
  disposable database; the inspector will not silently sample history.
- Queue and Dolt are separate stores, so this is a quiescent reconciliation
  proof, not a cross-store snapshot transaction. Crash/restart experiments
  must quiesce after deterministic recovery and then run this inspector.
