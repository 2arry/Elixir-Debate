defmodule Fairway.Queue.Runner do
  @moduledoc """
  Runs a queue's jobs on this node. One per queue per node.

  The runner does not choose work. The queue's scheduler, wherever in the
  cluster it is, sends it jobs that are already claimed. The runner starts each
  one as a task under the queue's `Task.Supervisor`, and when the task ends it
  writes the outcome to the store and tells the scheduler the slot is free.

  Two things follow from the runner, not the scheduler, writing the outcome:

    * A job that finishes while there is no scheduler is still recorded.
    * The runner's list of tasks is the truth about what this node is running.
      A new scheduler asks for it (`in_flight/1`) instead of guessing.

  Runners announce themselves by joining a `:pg` group, which is how a
  scheduler knows which nodes can take work and notices when one is gone.

  The runner and the task supervisor are restarted together. If the runner
  crashed alone, its tasks would finish with nobody to record them.
  """

  use GenServer

  alias Fairway.{Backoff, Job, Store}
  alias Fairway.Config.Queue
  alias Fairway.Queue.{Executor, Scheduler}

  require Logger

  @probe_timeout 1_000

  @type t :: %__MODULE__{
          queue: Queue.t(),
          store: Store.t(),
          tasks: %{optional(reference()) => Job.t()}
        }

  @enforce_keys [:queue, :store]
  defstruct [:queue, :store, tasks: %{}]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    queue = Keyword.fetch!(opts, :queue)
    GenServer.start_link(__MODULE__, opts, name: via(queue.name))
  end

  @doc "The `:pg` group that the runners of `queue` join."
  @spec group(String.t()) :: {:runners, String.t()}
  def group(queue), do: {:runners, queue}

  @doc "The name of the task supervisor for `queue` on this node."
  @spec task_supervisor(String.t()) :: {:via, Registry, {Fairway.Registry, term()}}
  def task_supervisor(queue), do: {:via, Registry, {Fairway.Registry, {:tasks, queue}}}

  @doc "Hands a claimed job to `runner`. Asynchronous; the outcome goes to the store."
  @spec run(pid(), Job.t()) :: :ok
  def run(runner, %Job{} = job), do: GenServer.cast(runner, {:run, job})

  @doc """
  The `{id, attempt}` of every job `runner` has been given and not yet
  recorded, or `:unreachable` if it is dead or does not answer within a second.

  A healthy runner answers at once. The timeout is short because the caller is
  a scheduler, which dispatches nothing while it waits.
  """
  @spec in_flight(pid()) :: {:ok, MapSet.t({pos_integer(), pos_integer()})} | :unreachable
  def in_flight(runner) do
    {:ok, GenServer.call(runner, :in_flight, @probe_timeout)}
  catch
    :exit, _reason -> :unreachable
  end

  defp via(queue), do: {:via, Registry, {Fairway.Registry, {:runner, queue}}}

  @impl true
  def init(opts) do
    queue = Keyword.fetch!(opts, :queue)
    :ok = :pg.join(Fairway.PG, group(queue.name), self())
    {:ok, %__MODULE__{queue: queue, store: Keyword.fetch!(opts, :store)}}
  end

  @impl true
  def handle_call(:in_flight, _from, state) do
    {:reply, MapSet.new(state.tasks, fn {_ref, job} -> {job.id, job.attempt} end), state}
  end

  @impl true
  def handle_cast({:run, job}, state) do
    task =
      Task.Supervisor.async_nolink(
        task_supervisor(state.queue.name),
        Executor,
        :run,
        [job, state.queue.slice_ms]
      )

    {:noreply, %{state | tasks: Map.put(state.tasks, task.ref, job)}}
  end

  @impl true
  def handle_info({ref, outcome}, state) when is_map_key(state.tasks, ref) do
    Process.demonitor(ref, [:flush])
    {:noreply, settle(state, ref, outcome)}
  end

  # The task died without returning: it was killed, or something it was linked
  # to went down. `Executor` turns everything else into a return value.
  def handle_info({:DOWN, ref, :process, _pid, reason}, state)
      when is_map_key(state.tasks, ref) do
    job = Map.fetch!(state.tasks, ref)

    {:noreply,
     settle(state, ref, {:error, "exited: " <> Exception.format_exit(reason), job.cursor})}
  end

  def handle_info(_message, state), do: {:noreply, state}

  # The job leaves `tasks` only after its outcome is in the store, so
  # `in_flight/1` never omits a job whose outcome is still unwritten.
  defp settle(state, ref, outcome) do
    {job, tasks} = Map.pop!(state.tasks, ref)
    now = System.system_time(:millisecond)

    outcome =
      case record(state, job, outcome, now) do
        {:ok, recorded} -> recorded
        {{:error, :stale}, _superseded} -> :stale
      end

    if outcome == :stale do
      Logger.warning(
        "fairway: job #{job.id} attempt #{job.attempt} was superseded; outcome dropped"
      )
    end

    :telemetry.execute(
      [:fairway, :job, :stop],
      %{system_time: now},
      %{job: job, queue: job.queue, tenant: job.tenant, outcome: outcome}
    )

    Scheduler.finished(job.queue, job.id, job.attempt)
    %{state | tasks: tasks}
  end

  defp record(state, job, :ok, now) do
    {Store.complete(state.store, job.id, job.attempt, now), :completed}
  end

  defp record(state, job, {:yield, cursor}, _now) do
    {Store.yield(state.store, job.id, job.attempt, cursor), :yielded}
  end

  defp record(state, job, {:error, message, cursor}, now) do
    if Backoff.exhausted?(job) do
      {Store.discard(state.store, job.id, job.attempt, %{error: message, now: now}), :discarded}
    else
      run_at = now + Backoff.delay(job.failures + 1, state.queue.retry)
      change = %{run_at: run_at, error: message, cursor: cursor}
      {Store.retry(state.store, job.id, job.attempt, change), :retried}
    end
  end
end
