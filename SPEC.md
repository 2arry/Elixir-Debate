# SPEC

This file was committed before any code. It holds the brief exactly as it was
given, the design decisions made in response, and the acceptance criteria the
result is measured against. Decisions that changed during implementation are
recorded under [Amendments](#amendments) rather than edited in place.

## 1. The brief, verbatim

~~~text
Build a fair, distributed, multi-tenant job orchestrator in Elixir in this repo
(github.com/2arry/Elixir-Debate) and push it. This is a public test of whether
an AI model writes good Elixir. An experienced Elixir developer will review the
result, so idiomatic OTP design and honest tests matter more than feature count.
Do it in one run with no questions back to me: make each call yourself and
record it.

ORIGINAL TASK, VERBATIM
"make fair distributed job/task orchestrator in which there are 4 modes of
fairness either using shuffle-sharding, interruptible iteration, throttling
and/or possibly using per tenant queues, Note it might be configurable by the
user as each modes have additional deps"

Clarifications from the challenger: it needs a pluggable store ("PostgreSQL,
SQLite, Redis, Kafka, RabbitMQ"); it is configurable via YAML or .conf; it can
be multi-node ("libcluster with horde or something", or built on erpc); "it is
like building advanced Oban"; "do what it thinks is best".

REQUIREMENTS
- All four fairness modes, selectable per queue from a YAML file, plus a plain
  FIFO mode used only as the unfair baseline in tests.
- A multi-node BEAM cluster. Library choices are yours; justify them in the
  README.
- A store behaviour with in-memory, SQLite and PostgreSQL adapters. The default
  `mix test` must need no external service. Postgres tests run only in CI or
  when a database is available.
- At-least-once delivery, retries with backoff, and recovery of jobs whose node
  died.

BEFORE WRITING CODE
1. Check that recent stable Erlang and Elixir are installed; install them if not.
2. Commit SPEC.md containing this prompt verbatim, your design decisions and the
   acceptance criteria below, and push it. Code comes after that commit.

ACCEPTANCE CRITERIA
A. `mix compile --warnings-as-errors`, `mix format --check-formatted`,
   `mix credo --strict`, `mix dialyzer` and `mix test` all pass, locally and in
   a GitHub Actions workflow that includes a Postgres service.
B. One end-to-end test per mode, with a noisy tenant flooding the queue and a
   quiet tenant arriving afterwards:
   - fifo: quiet jobs start only after every earlier noisy job (the control).
   - per-tenant queues: 200 noisy then 10 quiet jobs on 4 slots; all 10 quiet
     jobs start within the first 30 starts.
   - throttling: the noisy tenant never exceeds its concurrency cap, nor its
     rate plus burst in any one-second window, and the quiet tenant finishes
     while the noisy backlog remains.
   - shuffle sharding: assignment is deterministic; the noisy tenant never runs
     outside its shard; a quiet tenant sharing at most one shard member with it
     finishes while at least half the noisy backlog is still waiting.
   - interruptible iteration: with every slot held by long iterable noisy jobs,
     a quiet job starts within 5 time slices; interrupted jobs resume from
     their saved cursor and run every step exactly once.
C. Tests that start a real 3-node cluster: jobs run on more than one node;
   there is one scheduler per queue cluster-wide; killing the scheduler's node
   loses no jobs; killing a worker node re-runs its in-flight jobs elsewhere.
D. One shared contract test suite passes against all three store adapters.
E. An invalid YAML file is rejected at boot with an error naming the bad key.

FINISH
- README: architecture, each mode's guarantee and its limits, failure
  semantics, how to run the checks, and what is out of scope.
- Run every check in A yourself and fix what fails. Do not weaken a threshold
  to make a test pass; if one cannot be met, say so in the README.
- Push to origin main the same way the README was pushed (plain `git push` from
  WSL has no credentials). Then confirm the Actions run is green, and fix it if
  not.
- End with a short report: what was built, what was verified, what is not done.
~~~

## 2. Design decisions

Each entry is a call made without asking, with the reason.

### 2.1 Toolchain

- **Elixir 1.20.4 on Erlang/OTP 29.1.1**, the newest stable releases on
  2026-10-03. Neither was installed; both were installed from the precompiled
  builds on builds.hex.pm. CI pins the same versions.
- The project is called **Fairway**; the OTP application is `:fairway`.

### 2.2 Shape

- Fairway is a **library with a supervision tree**, started as `{Fairway, opts}`
  in a host application, or automatically at boot when `:fairway, :config_file`
  is set. One instance per node.
- Configuration is **YAML only** (`yaml_elixir`). `.conf` is not supported: one
  format, validated strictly, is better than two validated loosely.
- Validation is a small hand-written, path-aware validator, not NimbleOptions.
  Queue names are dynamic map keys and every error has to name the full key
  path (`queues.emails.concurency`), which is what criterion E asks for.

### 2.3 Job model and delivery

- A job has a `queue`, a `tenant`, a `worker` module and JSON `args`. Args and
  cursors are JSON values in every adapter, so all three stores behave the same.
- States: `available` → `running` → `completed` | `discarded`. A retry is
  `available` with a future `run_at`.
- **At-least-once.** A job is marked `running` in the store before it is sent
  to a node, and only an acknowledgement moves it on. Anything that dies in
  between is re-run.
- **Fenced acknowledgements.** `attempt` is incremented on every claim and
  every acknowledgement carries it. An acknowledgement from an execution that
  has since been superseded matches nothing and is dropped, so a job that was
  presumed dead and re-run cannot have its outcome recorded twice.
- `failures` is counted separately from `attempt`, because resuming an
  interrupted job is a new claim but not a failure. A job is discarded when
  `failures` reaches `max_attempts`. Losing a node counts as a failure, so a
  job that kills its node cannot loop forever.
- Retries use exponential backoff with jitter, configured per queue.

### 2.4 Store

- `Fairway.Store` is a behaviour of eleven callbacks. The scheduler asks it two
  questions (which tenants have ready jobs, oldest first; claim the oldest ready
  job of this tenant) and all fairness logic stays out of the store.
- **Memory**: an ETS table owned by a GenServer. One node only.
- **SQLite**: `exqlite` directly, one connection per node, WAL mode. Nodes on
  one host can share the file, which is how the cluster tests run with no
  external service.
- **PostgreSQL**: `postgrex` directly; claims use `FOR UPDATE SKIP LOCKED`.
- **No Ecto.** The store needs about ten fixed statements. Two small adapters
  with visible SQL are easier to review than a Repo, schemas and migrations,
  and `exqlite` and `postgrex` stay optional dependencies.
- Adapters create their table on start. Versioned migrations are out of scope.
- **Redis, Kafka and RabbitMQ adapters are out of scope.** The behaviour is the
  extension point. Redis would fit it. Kafka and RabbitMQ would not without a
  second index: fair scheduling needs "oldest ready job for tenant T", which a
  log or a broker queue cannot answer.

### 2.5 Fairness

- A mode is a module implementing `Fairway.Fairness`: pure functions over a
  snapshot (`ready` tenants, `running` slots, `free` slots, `now`) that return
  `{:run, tenant, slot}`, `{:wait, ms}` or `:idle`. No processes, no I/O, so
  policies are unit- and property-tested directly.
- A queue has `concurrency` **slots**, cluster-wide. One mode per queue;
  composing modes is out of scope.
- `fifo`: oldest ready job first. The unfair baseline.
- `per_tenant`: round-robin over tenants that have ready jobs, in tenant-id
  ring order from the last tenant served. Stateless per tenant, so a tenant
  cannot gain share by draining and re-filling its queue.
- `throttle`: per-tenant concurrency cap plus a token bucket (`rate` per second,
  `burst` capacity) metering job starts; round-robin among eligible tenants.
- `shuffle_shard`: each tenant may only use `shard_size` of the queue's slots,
  chosen by rendezvous hashing of `(seed, tenant, slot)` with `:erlang.phash2`,
  which is stable across nodes and releases. Oldest job first within a slot.
- `interruptible`: workers that implement `Fairway.IterableWorker` run in time
  slices. At the end of a slice the cursor is saved, the slot is released and
  re-assigned round-robin across tenants.

### 2.6 Processes and cluster

- **One scheduler per queue, cluster-wide.** Every node runs a
  `Fairway.Queue.Scheduler` (`:gen_statem`, states `standby` and `leader`). The
  leader holds a `:global` name; standbys monitor it and campaign when it goes.
  All fairness state lives in the leader, so decisions need no coordination.
- **`:global`, not Horde.** There is one singleton per queue, not a population
  of dynamic processes. `:global` gives a locked, cluster-wide registration with
  conflict resolution on partition heal and no dependency. Horde's CRDT-based
  supervisor and registry are eventually consistent and solve a larger problem.
- **`:pg` for membership.** Each node's `Fairway.Queue.Runner` joins a process
  group; the leader monitors the group and learns of joins and leaves, including
  node death.
- **libcluster for discovery** (`epmd` and `gossip` strategies, chosen in YAML).
  It only connects nodes; nothing else depends on it.
- **No `:erpc`.** Dispatch is a cast to a runner pid and completion is a cast to
  the global name. There is no synchronous cross-node call on the hot path.
- Jobs run as tasks under a per-queue `Task.Supervisor` on the node that owns
  the slot (`slot rem node_count`, nodes sorted). The runner writes the outcome
  to the store itself, so a finished job is recorded even while there is no
  leader.

### 2.7 Failure handling

- **Worker node dies**: the leader sees its runner leave the group and
  re-queues that node's running jobs at once.
- **Scheduler node dies**: a standby wins the name, rebuilds its view from the
  store's `running` jobs and each runner's in-flight list, and continues.
- **Running job on a node the leader has never seen**: re-queued only after
  `orphan_grace_ms`, so a node that boots before it has joined the cluster does
  not steal work that is still running elsewhere.
- **Netsplit**: each side elects a leader and may re-run the other side's jobs.
  Fencing keeps the record consistent; duplicate execution is possible and is
  what at-least-once permits. Per-tenant limits can be exceeded while split.
- Throttle buckets and round-robin position are leader memory and reset on
  failover.

### 2.8 Testing

- Fairness tests (B) run on one node with the memory store. **Start order and
  start times are taken from a telemetry event the scheduler emits when it
  claims a job**, because that is where a start is decided; messages from
  worker processes can arrive out of order. Workers are used to observe
  concurrency, steps and completion.
- Cluster tests (C) start three peer nodes with `:peer`, controlled over stdio
  so the test node is not part of the cluster. The peers find each other
  through libcluster and share one SQLite file. "Killing" a node is
  `kill -9` on its OS process.
- Store tests (D) are one `use`-able contract module run against each adapter.
  Postgres tests are tagged and run when `FAIRWAY_PG_URL` is set.
- Thresholds in the tests are the ones in the brief, not looser ones.
- Tooling: `credo`, `dialyxir`, `stream_data`.

### 2.9 Out of scope

Redis, Kafka and RabbitMQ stores; `.conf` files; composing modes on one queue;
tenant weights and priorities; per-tenant throttle overrides; per-node
concurrency limits; job timeouts; cron and unique jobs; graceful drain on
shutdown; a web UI; versioned migrations; quorum-based leader election.

## 3. Acceptance criteria

Copied from the brief, with where each is checked.

| | Criterion | Checked by |
|---|---|---|
| A | `mix compile --warnings-as-errors`, `mix format --check-formatted`, `mix credo --strict`, `mix dialyzer` and `mix test` all pass, locally and in a GitHub Actions workflow that includes a Postgres service. | `.github/workflows/ci.yml` and a local run of each command |
| B | One end-to-end test per mode, noisy tenant first, quiet tenant afterwards. | `test/fairway/fairness_e2e_test.exs` |
| B.fifo | Quiet jobs start only after every earlier noisy job. | same |
| B.per-tenant | 200 noisy then 10 quiet jobs on 4 slots; all 10 quiet jobs start within the first 30 starts. | same |
| B.throttle | Noisy never exceeds its concurrency cap, nor rate plus burst in any one-second window; quiet finishes while the noisy backlog remains. | same |
| B.shuffle-shard | Assignment is deterministic; noisy never runs outside its shard; a quiet tenant sharing at most one shard member finishes while at least half the noisy backlog is still waiting. | same |
| B.interruptible | With every slot held by long iterable noisy jobs, a quiet job starts within 5 time slices; interrupted jobs resume from their saved cursor and run every step exactly once. | same |
| C | Real 3-node cluster: jobs run on more than one node; one scheduler per queue cluster-wide; killing the scheduler's node loses no jobs; killing a worker node re-runs its in-flight jobs elsewhere. | `test/fairway/cluster_test.exs` |
| D | One shared contract suite passes against all three store adapters. | `test/support/store_contract.ex`, used by `test/fairway/store/*_test.exs` |
| E | An invalid YAML file is rejected at boot with an error naming the bad key. | `test/fairway/config_test.exs`, `test/fairway/boot_test.exs` |

## Amendments

Changes made during implementation, in the order they happened. Sections 1 to
3 above are as first committed.

1. **Shuffle sharding scores slots with SHA-256, not `:erlang.phash2`** (2.5).
   Before pinning shard assignments in a test, the real assignments were
   printed. With `phash2` over a `{seed, tenant, slot}` tuple, one slot of
   eight was in six of nine tenants' shards: the hash ranks the slots almost
   the same way for every tenant, which is the opposite of shuffling. SHA-256
   over a binary of the same three values puts 2,800 tenants within 15% of
   even over the eight slots and uses all 28 possible shards; a test asserts
   both. The application now depends on `:crypto`.

2. **A runner that is listed but does not answer** (2.7). The spec covered a
   job on a node the leader had never seen. Two more cases turned out to
   matter:
   - The runner answers and does not have the job: re-queued at once. This is
     what a leader that died between claiming and sending leaves behind.
   - The runner is in the group but does not answer: its jobs get the same
     `orphan_grace_ms` as a node that is not there, and it is given no new work
     until it answers. Found by running the suite pinned to two CPUs: the
     leader noticed a killed node's runner was silent before `:pg` reported it
     gone, re-queued the job and dispatched it straight back to the dead
     runner, which cost the job an attempt. One run in eight failed on it. The
     assertion was right and was not changed; the scheduler was.

3. **The "slot free" message carries the attempt.** A superseded execution
   reporting late must not free the slot of the execution that replaced it.

4. **Configuration values are matched against atoms, not converted to them.**
   The first cluster test failed at boot on every node: `mode: fifo` was being
   read with `String.to_existing_atom/1`, which only works once a module that
   mentions the atom has been loaded. On the test node one always had been.
   Not a design change, but it is the kind of bug only a freshly started node
   shows, and the reason criterion C is worth having.

5. **PostgreSQL was tested locally as well as in CI** (2.8). There was no
   database on the development machine, so a throwaway PostgreSQL 17.11 was
   run from unprivileged portable binaries, bound to 127.0.0.1, for the
   `:postgres` tests. It is not part of the repository.

6. **A cluster test's setup raced the scheduler.** The scheduler-kill test
   asserted that the killed node had jobs in flight. With a backlog of short
   jobs that was usually true and not guaranteed: one restricted run in twelve
   killed the node between jobs. Making every slot hold a long job exposed a
   second race, between the test's separate enqueue calls and a dispatch round
   already under way. The test now enqueues six long jobs in one batch, waits
   until they hold all six slots, two per node, and only then adds the
   backlog. The assertion became stricter: exactly two jobs interrupted, both
   re-run by a survivor. After this and amendment 2, twelve consecutive
   restricted runs of the full suite passed.

7. **Not done from section 2:** nothing. **Added beyond it:** `examples/fairway.yml`,
   which the test suite loads.
