# Bounded hierarchical fleet scheduler

This prototype is the admission/fairness half of a team cell. Each cell accepts
jobs only for its signed team/repository inventory, bounds both job count and
actual copied payload plus bounded identity bytes globally and per repository,
and schedules repositories with bounded deficit round robin. Queue and
in-flight counts and bytes have separate ceilings. Interactive/write work has
reserved queue and execution capacity; bulk/reconciliation cannot consume it.
Within a repository, interactive, write, bulk, and reconciliation classes
receive explicit weighted turns.

Every job carries project ID, database epoch, catalog version, and signing-key
epoch. An atomic catalog update purges stale queued work and marks stale
in-flight work. Deficits cap at 64 cost units and reset when a queue drains.
Drained queues clear payload references and shed oversized backing arrays.

The tests cover:

- a noisy bulk repository while nine peers receive interactive service;
- `ae` interactive capacity while graph/bulk and reconciliation still progress;
- wrong-team and unknown-repository rejection;
- byte and job capacity rejection;
- exact serialized-size, duplicate-ID, and overflow-safe admission rejection;
- catalog/key/database-epoch replacement for queued and in-flight work;
- interactive queue and in-flight reservations;
- 30 concurrent team cells, 300 repositories, and 300,000 enqueue/dequeue
  operations without residual queue state or capacity escape.

This scheduler does not replace canonical durable admission. It selects work
only after terminal/Dolt or replicated-log admission has established identity
and recovery semantics. The special `ae` cell remains a separate scheduler and
dedicated storage bulkhead.

This remains an in-memory scheduling model. Authentication is not yet wired to
the Ed25519 catalog, running jobs require a transactional database-side epoch
check, worker permits need durable leases/reaping after process loss, and the
fairness tests measure dequeue order rather than end-to-end completion latency.
