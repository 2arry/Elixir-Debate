# Fairway

[![CI](https://github.com/2arry/Elixir-Debate/actions/workflows/ci.yml/badge.svg)](https://github.com/2arry/Elixir-Debate/actions/workflows/ci.yml)

A fair, distributed, multi-tenant job orchestrator in Elixir.

Jobs belong to tenants. Each queue has a fairness mode that decides whose job
starts next, so one tenant's backlog cannot starve everyone else. Jobs run on
any node of a BEAM cluster, are delivered at least once, are retried with
backoff, and are recovered when the node running them dies.

This repository is the answer to a brief, which is reproduced verbatim in
[SPEC.md](SPEC.md) together with the design decisions and acceptance criteria.
SPEC.md was committed before any code. It was written by an AI model (Claude)
in one run without questions, as the brief required; what was and was not
verified is listed under [What was verified](#what-was-verified).

```yaml
# fairway.yml
queues:
  emails:
    mode: per_tenant
    concurrency: 8
```

```elixir
defmodule MyApp.SendEmail do
  @behaviour Fairway.Worker

  @impl true
  def perform(%Fairway.Job{args: %{"to" => to}}), do: MyApp.Mailer.deliver(to)
end

# in the application's supervision tree
{Fairway, config_file: "fairway.yml"}

Fairway.enqueue(queue: "emails", tenant: "acme", worker: MyApp.SendEmail, args: %{to: "a@b.c"})
```

A fuller configuration is in [examples/fairway.yml](examples/fairway.yml).

## Architecture

```
          node A                      node B                      node C
 +----------------------+    +----------------------+    +----------------------+
 | Scheduler  [LEADER]  |    | Scheduler [standby]  |    | Scheduler [standby]  |
 |   fairness mode      |    |   watches the leader |    |   watches the leader |
 |   slots              |    |                      |    |                      |
 |      | claimed jobs  |    |                      |    |                      |
 |      +---------------------------+---------------------------+               |
 |      v               |    |      v               |    |      v               |
 | Runner               |    | Runner               |    | Runner               |
 |   Task.Supervisor    |    |   Task.Supervisor    |    |   Task.Supervisor    |
 |     job  job         |    |     job  job         |    |     job  job         |
 +----------+-----------+    +----------+-----------+    +----------+-----------+
            |                           |                           |
            +---------------------------+---------------------------+
                                        |
                       store: SQLite file or PostgreSQL
```

That is one queue. Every queue has its own scheduler, runner and task
supervisor on every node.

**The life of a job**

1. `Fairway.enqueue/1` validates the job, saves it as `available` and sends the
   queue's leader a hint.
2. The leader asks the store which tenants have ready jobs, asks the queue's
   fairness mode which tenant goes next and in which slot, and claims that
   tenant's oldest job. The store marks it `running`, adds one to its `attempt`
   and records the node and slot.
3. The leader casts the job to the runner on the node that owns the slot. The
   runner starts it as a task.
4. When the task ends, the runner writes the outcome to the store (completed,
   retry later, discarded, or yielded with a cursor) and tells the leader the
   slot is free.

The job is `running` in the store before any node hears of it. That ordering
is the at-least-once guarantee: a job can be claimed and never run, in which
case it is recovered, but it cannot run without having been claimed.

**The pieces, and why these**

| Piece | Choice | Why |
|---|---|---|
| One scheduler per queue | `:gen_statem` with states `standby` and `leader`; the leader holds a `:global` name | All fairness state (round-robin position, token buckets, slot occupancy) lives in one process, so decisions need no coordination between nodes. `:global` registration takes a cluster-wide lock, so two connected nodes cannot both win, and it resolves the conflict when two partitions rejoin. |
| Not Horde | | Horde's supervisor and registry are delta-CRDTs: eventually consistent, built for many dynamic processes spread over a cluster. Here there is one singleton per queue. `:global` is in OTP, is stricter, and failover is a monitor firing. |
| Membership | `:pg` | Each node's runner joins a process group. The leader monitors the group and is told of joins and leaves, including nodes going down. Built in, and eventually consistent is fine for "who can take work". |
| Discovery | libcluster (`epmd` or `gossip`, chosen in YAML) | It connects nodes and does nothing else. Fairway depends on distributed Erlang, not on libcluster; the dependency is optional. |
| No `:erpc` | | Dispatch is a cast to a runner and completion is a cast to the global name. Nothing on the hot path waits on another node. The one synchronous cross-node call is the leader asking each runner what it is running, during recovery. |
| Fairness | A behaviour of pure functions (`Fairway.Fairness`) | A mode sees a snapshot and answers `{:run, tenant, slot}`, `{:wait, ms}` or `:idle`. No processes, no I/O, so modes are unit- and property-tested directly and can be rebuilt from nothing after a failover. |
| Store | A behaviour of eleven callbacks (`Fairway.Store`) | The scheduler needs two questions answered (which tenants have ready jobs, oldest first; claim the oldest ready job of this tenant) and fenced acknowledgements. Fairness logic stays out of the store. |
| No Ecto | `postgrex` and `exqlite` used directly | About ten fixed statements per adapter. The SQL is in the adapter file, and both drivers stay optional dependencies. |
| Config | YAML via `yaml_elixir`, validated by a small path-aware validator | Queue names are dynamic keys and every error has to name the full key path. |

Supervision, on each node:

```
Fairway.Supervisor                  rest_for_one
├── Registry                        local names; holds the validated config
├── :pg scope
├── store adapter                   Memory | SQLite | Postgres
├── Cluster.Supervisor              only when a cluster strategy is configured
└── Fairway.Queue.Supervisor        one per queue, one_for_one
    ├── execution                   one_for_all
    │   ├── Task.Supervisor         the jobs
    │   └── Fairway.Queue.Runner
    └── Fairway.Queue.Scheduler
```

The runner and its tasks restart together, because tasks whose runner is gone
would finish with nobody to record them. The scheduler is independent of both:
restarting it changes who decides, not what is running.

## Fairness modes

A queue has `concurrency` slots, across the whole cluster, and one mode. In all
of them a tenant's own jobs run oldest first.

### `fifo` (the baseline)

Oldest ready job first, whoever owns it. Not fair, and not meant to be: it is
what the other modes are compared with in the tests.

### `per_tenant`: per-tenant queues

Tenants with ready jobs are served round-robin. The ring is the tenant ids in
sorted order and the only state is the tenant served last.

- **Guarantee.** Between two consecutive starts for one tenant, every other
  tenant with ready jobs gets a start. With `T` tenants waiting, a tenant's next
  job is at most `T - 1` starts away, whatever the others have queued.
- **Limits.** Fair in job starts, not in time: a tenant with long jobs holds
  its slots longer. No weights or priorities. A running job is never displaced.
  The list of waiting tenants is read once per round of dispatching, so a
  tenant that enqueues while several free slots are being filled takes its
  first turn in the next round.
- **Test.** 200 noisy jobs, then 10 quiet ones, on 4 slots: all 10 quiet jobs
  are within the first 30 starts (in fact the first 24, alternating).

### `throttle`: per-tenant limits

Each tenant may have at most `max_concurrency` jobs running, and has a token
bucket of `burst` tokens refilled at `rate` per second. Starting a job takes a
token. Tenants under both limits are served round-robin.

- **Guarantee.** At every moment a tenant has at most `max_concurrency` jobs
  running, and in any window of `w` seconds it has started at most
  `burst + rate * w`. Bucket arithmetic is in integers, so this holds exactly.
- **Limits.** The bucket meters the scheduler's decision to start a job;
  `perform/1` is entered one message hop later. Buckets are leader memory, so
  after a failover every bucket is full and a tenant can get one extra burst.
  The limits are the same for every tenant. Rates are honoured to three
  decimal places.
- **Test.** The noisy tenant is never above its cap (and reaches it), no
  one-second window holds more than `rate + burst` of its starts, and the quiet
  tenant finishes while noisy jobs are still waiting.

### `shuffle_shard`: shuffle sharding

Each tenant may only use `shard_size` of the queue's slots, chosen by
rendezvous hashing of the seed, the slot and the tenant with SHA-256. The
assignment is the same on every node and after every restart. Slots map to
nodes, so a shard is also a small set of nodes: a tenant whose jobs exhaust
memory or crash the VM takes down only the nodes behind its own slots.

- **Guarantee.** A tenant's jobs never run outside its shard, so it can hold at
  most `shard_size` slots. A tenant sharing fewer than `shard_size` slots with
  a noisy tenant keeps at least one slot the noisy tenant cannot touch.
- **Limits.** Isolation is a matter of odds: two tenants draw the same shard
  with probability `1 / C(concurrency, shard_size)` (1 in 28 for 2 of 8), and
  then share everything. In a shared slot the oldest job goes first, so the
  noisy tenant wins the slots it shares. A tenant never uses more than its
  shard even when the rest of the queue is idle.
- **Test.** Shards are computed twice and compared with pinned values; every
  noisy start is in the noisy shard; a quiet tenant sharing exactly one slot
  with the noisy one finishes with at least half the noisy backlog waiting.

### `interruptible`: interruptible iteration

A job written as a `Fairway.IterableWorker` is a cursor and a `step/2`
function. It holds its slot for one `slice_ms`. Then, between steps, its cursor
is saved, the slot is released and re-assigned round-robin across tenants. The
job later resumes from the cursor, possibly on another node.

- **Guarantee.** When every slot is held by iterable jobs, an arriving tenant
  waits at most one slice, plus the step in progress, for a slot. Without
  failures every step runs exactly once.
- **Limits.** Only iterable workers can be interrupted; a plain `Fairway.Worker`
  keeps its slot until it returns. A step is never cut short, so one slow step
  delays the hand-over. The cursor is saved at the end of a slice and when a
  step fails, not after every step: if the node dies mid-slice, that slice's
  steps run again. A job is interrupted at the end of every slice even when
  nobody is waiting, which costs one store write per slice.
- **Test.** With both slots held by long iterable jobs and two more queued, a
  quiet job is the next to start and starts within 5 slices; each noisy job's
  steps are observed exactly once, in order, across several executions.

## Failure semantics

Delivery is **at-least-once**. A worker must be safe to run twice.

Every claim increments the job's `attempt`, and every acknowledgement carries
it. The store applies an acknowledgement only if the job is `running` with that
attempt. An execution that was given up on and replaced can therefore still
finish, but its report changes nothing.

A job has `max_attempts` executions that may fail (default 5). Between them it
waits `base_backoff_ms * 2^(failures - 1)`, capped at `max_backoff_ms`, plus up
to 10% jitter. After the last one it is `discarded`, with the error kept.
Resuming an interrupted job is a new attempt but not a failure.

| What happens | What Fairway does |
|---|---|
| A worker returns `{:error, _}`, raises, throws, exits, or is killed | Counted as a failure. Retried after backoff, or discarded when attempts are spent. For iterable jobs the cursor before the failing step is kept. |
| A job names a worker that does not exist | Fails like any other error, with a message saying so. No atom is created. |
| The runner crashes | Its tasks are stopped with it. The leader sees it leave and re-queues its jobs at once. |
| The scheduler crashes | It is restarted and re-elected, or another node's standby takes over. The new leader reads the `running` jobs from the store, asks each runner what it is running, and carries on. Running jobs are not disturbed. |
| A worker node dies | The leader sees its runner leave and re-queues its running jobs at once. They run on other nodes. Each counts one failure, so a job that kills its node is discarded after `max_attempts` rather than retried forever. |
| The scheduler's node dies | A standby wins the name. Jobs on surviving nodes are adopted. Jobs that were on the dead node are re-queued when its runner is seen to leave, or after `orphan_grace_ms` if the new leader never saw it. |
| A leader dies between claiming a job and sending it | The next reconciliation finds a `running` job that its node says it is not running, and re-queues it at once. |
| A runner is alive but does not answer within a second | It is given no new work until it answers. Its jobs are left alone for `orphan_grace_ms`, then re-queued. |
| A node boots and elects itself before it has joined the cluster | It sees `running` jobs on nodes it does not know. It leaves them alone for `orphan_grace_ms`, by which time it has met the cluster and stepped down or adopted them. |
| The network partitions | Each side elects a leader. Each may, after the grace period, re-queue the other side's jobs, so jobs can run twice; fencing keeps one outcome. Per-tenant limits can be exceeded while split. When the sides rejoin, `:global` keeps one leader and the other exits and returns as a standby. |
| The store is unreachable | Calls fail, the processes making them crash, and supervisors restart them. If it lasts, the restart intensity is exceeded and Fairway's supervisor exits; what happens next is up to the host application. Nothing is buffered in memory. |
| A node shuts down cleanly | The same as dying: its running jobs are stopped and re-run elsewhere. There is no drain. |

How fast a dead node is noticed is distributed Erlang's business: at once when
its connection closes (the process was killed), and up to `net_ticktime`,
60 seconds by default, when a host disappears without closing it. Until then
the leader treats its runner as one that does not answer, and waits up to a
second for it on every poll.

Fairness state lives in the leader's memory and is rebuilt empty on failover:
round-robin starts again from the first tenant and throttle buckets start full.

## Stores

| Adapter | For | Notes |
|---|---|---|
| `memory` | Development, tests, one node | An ETS table behind a GenServer. Not durable and not shared between nodes. |
| `sqlite` | One host | `exqlite`, one connection per node, WAL. Several nodes on the same host can share the file; that is how the cluster tests run with no external service. |
| `postgres` | Clusters across hosts | `postgrex`. Claims use `FOR UPDATE SKIP LOCKED`. `args` and the cursor are `jsonb`. |

All three pass the same contract suite, `test/support/store_contract.ex`, which
is also the specification for writing another adapter.

## Running the checks

Developed and tested on Elixir 1.20.4 and Erlang/OTP 29.1.1. `mix.exs` allows
Elixir 1.18 and later (the built-in `JSON` module is required), but nothing
older than 1.20.4 has been tried.

```sh
mix deps.get
mix compile --warnings-as-errors
mix format --check-formatted
mix credo --strict
mix dialyzer            # the first run builds the PLTs, a few minutes
mix test
```

`mix test` needs no external service and takes about 25 seconds. Most of that
is the cluster tests, which start three Erlang nodes as separate OS processes
with `:peer` on `127.0.0.1`; they need `epmd`, which ships with Erlang.

The PostgreSQL tests are tagged `:postgres` and run only when a database is
given:

```sh
FAIRWAY_PG_URL=postgres://postgres:postgres@localhost:5432/fairway_test mix test
```

They create the `fairway_jobs` table and truncate it, so use a database that is
only for this.

[The CI workflow](.github/workflows/ci.yml) runs the same five commands on
every push, with a PostgreSQL 17 service, so there the `:postgres` tests run.

### Where each acceptance criterion is tested

| | Criterion | Test |
|---|---|---|
| A | The five commands pass, locally and in CI with Postgres | `.github/workflows/ci.yml` |
| B | One end-to-end test per mode | `test/fairway/fairness_e2e_test.exs` |
| C | A real 3-node cluster | `test/fairway/cluster_test.exs` |
| D | One contract suite, three adapters | `test/support/store_contract.ex`, `test/fairway/store/` |
| E | Invalid YAML rejected at boot, naming the key | `test/fairway/boot_test.exs`, `test/fairway/config_test.exs` |

The thresholds in the tests are the ones in the brief. None was loosened.

Two things about how the tests measure, so that they can be judged:

- **"Start" is the scheduler's decision.** Start order and start times come
  from the `[:fairway, :job, :start]` telemetry event, which the leader emits
  when it claims a job. That is where a start is decided and where the throttle
  meters it, and one process emitting them gives a true order. Messages from
  worker processes are used for what only a worker knows: how many jobs were
  inside `perform/1` at once, and which steps ran.
- **"Killing a node" is `kill -9`** on the node's OS process. The test node is
  not part of the cluster; it controls the three nodes over stdio, and they
  find each other through libcluster and share one SQLite file (or, with
  `FAIRWAY_PG_URL`, PostgreSQL).

## What was verified

On the development machine (WSL2, Ubuntu 24.04, 8 cores):

- All five commands pass.
- `mix test` without a database: 176 tests (10 doctests, 9 properties).
- `mix test` with PostgreSQL 17.11: 204 tests. The extra 28 are the contract
  suite against Postgres, three Postgres-specific tests and one cluster test.
- To look for timing-dependent failures, the full suite with PostgreSQL was
  run repeatedly with the whole process tree restricted to 2 CPUs. That found
  two problems that never showed at full speed, about one run in ten each: a
  flaw in the scheduler, and a test whose setup raced the scheduler. Both are
  described in [SPEC.md](SPEC.md#amendments). The scheduler was fixed and the
  test's setup was made deterministic; no assertion was loosened. The final
  code then passed 12 restricted runs in a row. That is evidence, not proof,
  that the suite is not flaky.

In CI (GitHub Actions, `ubuntu-24.04`, a PostgreSQL 17 service):

- All five steps pass on `main`. The first run after the code was pushed was
  green without changes.
- The test step posts its result line as a notice on the run, so the number of
  tests that ran there, Postgres ones included, is visible on the run's page.

Not verified, and so not claimed:

- **Partitions.** The behaviour in the table above is reasoned from how
  `:global`, `:pg` and the fencing work. No test partitions a cluster.
- **The `gossip` strategy.** The cluster tests use `epmd`.
- **Performance.** Nothing was benchmarked. One process per queue makes every
  claim, with a store round trip each, and the leader lists ready tenants with
  a `GROUP BY` on every dispatch. That is a deliberate trade of throughput for
  simple, exact fairness and has not been measured.
- **Multi-host.** Three nodes on one machine is the largest cluster it has run
  on.
- **Elixir or OTP versions other than the pair above.**

## Out of scope

- Redis, Kafka and RabbitMQ stores. The `Fairway.Store` behaviour is the
  extension point. Redis would fit it. Kafka and RabbitMQ would need a second
  index, because "the oldest ready job of tenant T" is not a question a log or
  a broker queue can answer.
- `.conf` files. Configuration is YAML only.
- Combining modes on one queue; tenant weights and priorities; per-tenant
  throttle overrides; per-node concurrency limits.
- Job timeouts, cron schedules, unique jobs, and pruning of finished jobs.
- Draining running jobs on shutdown.
- Versioned migrations. Adapters create their table on start.
- More than one Fairway per node.
- Quorum-based leader election. `:global` cannot prevent two leaders across a
  partition; the design tolerates that instead (fencing, at-least-once).
- A web UI. Telemetry events are there to build on: `[:fairway, :job, :start]`
  and `[:fairway, :job, :stop]`.

## Layout

```
lib/fairway.ex                     public API: start_link, enqueue, fetch, counts
lib/fairway/config.ex              YAML loading and validation
lib/fairway/job.ex                 the job struct
lib/fairway/worker.ex              behaviour for plain jobs
lib/fairway/iterable_worker.ex     behaviour for interruptible jobs
lib/fairway/backoff.ex             retry policy
lib/fairway/fairness.ex            the mode behaviour
lib/fairway/fairness/*.ex          fifo, round_robin, throttle, shuffle_shard, interruptible
lib/fairway/store.ex               the store behaviour
lib/fairway/store/*.ex             memory, sqlite, postgres
lib/fairway/queue/scheduler.ex     leader election, dispatch, recovery
lib/fairway/queue/runner.ex        runs jobs on a node, records outcomes
lib/fairway/queue/executor.ex      runs one job; time slices
lib/fairway/supervisor.ex          the tree
```
