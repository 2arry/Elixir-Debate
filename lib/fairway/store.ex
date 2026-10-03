defmodule Fairway.Store do
  @moduledoc """
  Where jobs live. A behaviour with one adapter per backend.

  The store knows nothing about fairness. A scheduler asks it which tenants
  have work (`c:ready_tenants/3`) and to hand over the oldest ready job of the
  tenant its mode chose (`c:claim/4`). Everything else is bookkeeping for one
  job.

  ## What an adapter must get right

    * **Claims are atomic.** Two concurrent `c:claim/4` calls never return the
      same job.
    * **Order.** Ids grow with insertion, and "oldest" means lowest id.
    * **Acknowledgements are fenced.** `c:complete/4`, `c:retry/4`,
      `c:discard/4` and `c:yield/4` apply only if the job is `:running` with
      the given `attempt`, and answer `{:error, :stale}` otherwise. A node that
      was presumed dead may still report on a job that has since been claimed
      again; that report must change nothing.
    * **JSON.** `args` and `cursor` come back as the JSON values that went in.

  Times are Unix milliseconds supplied by the caller, never the backend's
  clock, so readiness is judged by one clock: the scheduler's.

  `Fairway.StoreContract` in the test suite states all of this as tests, and
  runs against every adapter.

  A store is addressed as `{adapter, server}`, where `server` is whatever the
  adapter's process is registered as.
  """

  alias Fairway.Job

  @type server :: GenServer.server()
  @type t :: {adapter :: module(), server()}
  @type queue :: String.t()
  @type tenant :: String.t()
  @type now :: integer()
  @type ack :: :ok | {:error, :stale}
  @type claim :: %{node: String.t(), slot: non_neg_integer(), now: now()}

  @doc "Child spec for the adapter's process tree. Options include `:name`."
  @callback child_spec(opts :: keyword()) :: Supervisor.child_spec()

  @doc "Saves new jobs and returns them, with ids, in the order given."
  @callback insert_all(server(), [Job.t()]) :: {:ok, [Job.t()]}

  @doc """
  Tenants of `queue` with an `:available` job whose `run_at` has passed,
  ordered by the id of their oldest such job.
  """
  @callback ready_tenants(server(), queue(), now()) :: [tenant()]

  @doc """
  Atomically moves the oldest ready job of `tenant` to `:running`, adds one to
  its `attempt` and records the node and slot it was given to.
  """
  @callback claim(server(), queue(), tenant(), claim()) :: {:ok, Job.t()} | :none

  @doc "Marks a running job `:completed`."
  @callback complete(server(), id :: pos_integer(), attempt :: pos_integer(), now()) :: ack()

  @doc """
  Records a failed execution: back to `:available`, not before `run_at`, with
  `failures` incremented and the error and cursor saved.
  """
  @callback retry(
              server(),
              id :: pos_integer(),
              attempt :: pos_integer(),
              %{run_at: now(), error: String.t(), cursor: Job.json()}
            ) :: ack()

  @doc "Records a final failure: the job becomes `:discarded`."
  @callback discard(
              server(),
              id :: pos_integer(),
              attempt :: pos_integer(),
              %{error: String.t(), now: now()}
            ) :: ack()

  @doc """
  Gives up the slot without failing: back to `:available` with `cursor` saved
  and `failures` untouched.
  """
  @callback yield(server(), id :: pos_integer(), attempt :: pos_integer(), cursor :: Job.json()) ::
              ack()

  @doc "Every `:running` job of `queue`, oldest first."
  @callback running(server(), queue()) :: [Job.t()]

  @doc "One job by id."
  @callback fetch(server(), id :: pos_integer()) :: {:ok, Job.t()} | :error

  @doc "How many jobs of `queue` each tenant has in each state."
  @callback counts(server(), queue()) :: %{optional({tenant(), Job.state()}) => pos_integer()}

  @spec insert_all(t(), [Job.t()]) :: {:ok, [Job.t()]}
  def insert_all({adapter, server}, jobs), do: adapter.insert_all(server, jobs)

  @spec ready_tenants(t(), queue(), now()) :: [tenant()]
  def ready_tenants({adapter, server}, queue, now), do: adapter.ready_tenants(server, queue, now)

  @spec claim(t(), queue(), tenant(), claim()) :: {:ok, Job.t()} | :none
  def claim({adapter, server}, queue, tenant, claim) do
    adapter.claim(server, queue, tenant, claim)
  end

  @spec complete(t(), pos_integer(), pos_integer(), now()) :: ack()
  def complete({adapter, server}, id, attempt, now),
    do: adapter.complete(server, id, attempt, now)

  @spec retry(t(), pos_integer(), pos_integer(), map()) :: ack()
  def retry({adapter, server}, id, attempt, change),
    do: adapter.retry(server, id, attempt, change)

  @spec discard(t(), pos_integer(), pos_integer(), map()) :: ack()
  def discard({adapter, server}, id, attempt, change) do
    adapter.discard(server, id, attempt, change)
  end

  @spec yield(t(), pos_integer(), pos_integer(), Job.json()) :: ack()
  def yield({adapter, server}, id, attempt, cursor),
    do: adapter.yield(server, id, attempt, cursor)

  @spec running(t(), queue()) :: [Job.t()]
  def running({adapter, server}, queue), do: adapter.running(server, queue)

  @spec fetch(t(), pos_integer()) :: {:ok, Job.t()} | :error
  def fetch({adapter, server}, id), do: adapter.fetch(server, id)

  @spec counts(t(), queue()) :: %{optional({tenant(), Job.state()}) => pos_integer()}
  def counts({adapter, server}, queue), do: adapter.counts(server, queue)
end
