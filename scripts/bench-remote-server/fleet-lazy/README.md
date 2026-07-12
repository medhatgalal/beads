# Lazy fleet connection experiment

This disposable command tests the smallest connection-control mechanism needed
for hundreds of repository databases: one serial SQL connection per resident
database, a hard cell-wide resident/connection budget, idle LRU eviction, a
bounded worker set, a bounded admission queue, and explicit hibernation to zero
connections for cold databases.

The application pool budget is deliberately smaller than the server's hard
`max_connections` cap. Four slots are reserved for the sampler/control path and
server-side close-drain overlap observed during eviction churn. The harness
reads `@@max_connections` and fails before load if the server is configured
above the approved cap; Dolt therefore enforces the physical ceiling even when
TCP session teardown trails `sql.DB.Close`.

Closing pools continue to consume a budget slot until `Close` succeeds. Failed
closes remain quarantined and referenced for an explicit retry. State changes
broadcast to every waiter, avoiding a coalesced-notification deadlock. The live
harness samples both manager state and `SHOW PROCESSLIST` every 5 ms throughout
the workload; the process peak gate is the configured budget plus the one
administrative sampler connection.

Manager shutdown rejects new acquisitions and drains active leases before
closing any pool. The default shutdown has a five-second bound; callers that
need a different deadline use `CloseContext`. If the deadline expires, in-use
pools remain open and referenced, and shutdown can be retried after leases are
released.

It uses only existing `beads_perf_lab_scale_%03d` synthetic raw-probe databases
created by `fleet-scale` on a numeric loopback Dolt port. Ports 3306 and 3307
are rejected. Before any probe mutation it requires the caller's canonical
`--lab-id` to match the exact singleton synthetic marker in
`beads_perf_lab_control.lab_identity`. It does not implement Beads semantics;
the realistic gateway and independent inspector cover those gates.

```sh
go run ./scripts/bench-remote-server/fleet-lazy \
  --host 127.0.0.1 --port 13360 \
  --lab-id dc6020ef-b433-41b6-9426-cedd9bf40502 \
  --databases 100 --active-sets 5,30,100 \
  --connection-budget 28 --server-connection-cap 32 \
  --workers 64 --queue-capacity 256 \
  --operations 640 --execute-synthetic \
  --output /private/tmp/beads-fleet-scale-20260712/lazy-pools.json
```

Promotion gates for each case:

- every synthetic commit succeeds;
- maximum resident pools and observed open connections never exceed the
  application budget, and sampled server processes never exceed the separately
  enforced Dolt cap;
- close-in-progress and close-failed resources remain inside that budget;
- the admission queue never exceeds its configured capacity;
- hibernation leaves zero resident pools and zero open connections.
- server-side process count returns to its pre-case baseline within the bounded
  close-drain window; process-list query or row iteration errors fail the case.

This mechanism is evidence for a fleet cell, not production routing code. A
production cell still needs a signed catalog/database epoch, per-database
durable actors, fair class/team scheduling, canonical terminal outcomes,
retention, HA fencing, and dedicated `ae` execution and storage.

The existing 100-database JSON results predate the close-accounting and
continuous-sampling repair. They remain evidence of one local raw-SQL run, not
evidence that the repaired invariant or a full Beads cell has passed a soak.
