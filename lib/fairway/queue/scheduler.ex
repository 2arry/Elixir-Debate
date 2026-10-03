defmodule Fairway.Queue.Scheduler do
  @moduledoc """
  Decides which job of a queue starts next. One per queue per node, of which
  exactly one in the cluster is in charge.

  It is a state machine with two states:

    * `standby` - does nothing but watch the leader.
    * `leader` - holds the queue's `:global` name, owns the fairness state and
      dispatches jobs.

  ## Election

  A scheduler becomes leader by registering the queue's name with `:global`,
  which takes a cluster-wide lock, so two connected nodes cannot both succeed.
  Whoever fails monitors the winner and tries again when it goes down. If two
  partitions each elected a leader, `:global` picks one when they rejoin and
  tells the other, which exits and comes back as a standby.

  ## Dispatch

  The leader keeps a map of the jobs it believes are running. Whenever a slot
  might be free (a job was enqueued or finished, a node joined, a timer fired)
  it asks the store which tenants have ready jobs and asks the queue's
  `Fairway.Fairness` mode what to start. It then claims that tenant's oldest
  job in the store and casts it to the runner on the node that owns the slot:
  slot `s` belongs to node `s rem n` of the `n` nodes whose runners answer,
  in name order.

  The job is `:running` in the store before any node hears of it. That order
  is what makes delivery at-least-once: a job can be claimed and never run,
  and will then be recovered, but it cannot run without being claimed.

  ## Recovery

  On becoming leader, on every poll, and when a runner leaves, the leader
  reconciles its map with the store's `:running` jobs and with what each
  runner says it is running:

    * running on a runner that confirms it - kept.
    * given to a runner that answers and does not have it - re-queued. The
      runner restarted, or the previous leader died between claiming and
      sending.
    * given to a runner that does not answer, or to a node with no runner in
      the group - re-queued once it has been unaccounted for
      `orphan_grace_ms`. The wait matters at boot, when a node can be leader
      for a moment before it has met the rest of the cluster and must not
      conclude that every other node's work is lost. It also covers a runner
      that is busy rather than dead.

  When a runner leaves the group, its node's jobs are re-queued at once: that
  is a node going down, not one still arriving.

  A runner that does not answer is given no new work until it does. Otherwise
  a job taken back from a dead node could be handed to the same dead node, and
  lose an attempt for nothing, in the moment before `:pg` reports it gone.

  A re-queued job counts one failure, so a job that takes its node down with
  it is discarded after `max_attempts`, not retried forever.

  All of this is at-least-once. A job re-queued by mistake runs twice, and
  whichever execution is not current has its acknowledgement refused by the
  store (see `Fairway.Store`).

  ## Telemetry

  `[:fairway, :job, :start]` is emitted when a job is dispatched, with
  measurement `:at` (the leader's monotonic clock in milliseconds, the same
  clock the fairness mode is given) and metadata `:job`, `:queue`, `:tenant`,
  `:slot` and `:node`.
  """

  @behaviour :gen_statem

  alias Fairway.{Backoff, Fairness, Job, Store}
  alias Fairway.Config.Queue
  alias Fairway.Queue.Runner

  require Logger

  # How long to wait before campaigning again when the name was taken but its
  # holder had vanished by the time we looked.
  @campaign_retry_ms 50

  @enforce_keys [:queue, :store, :policy]
  defstruct [
    :queue,
    :store,
    :policy,
    :policy_state,
    :leader_ref,
    :group_ref,
    runners: [],
    unreachable: [],
    running: %{},
    unaccounted: %{},
    last_slot: -1
  ]

  @typep run :: %{job: Job.t(), runner: pid() | nil}

  @typep data :: %__MODULE__{
           queue: Queue.t(),
           store: Store.t(),
           policy: module(),
           policy_state: Fairness.state(),
           leader_ref: reference() | nil,
           group_ref: reference() | nil,
           runners: [pid()],
           unreachable: [pid()],
           running: %{optional(pos_integer()) => run()},
           unaccounted: %{optional(pos_integer()) => integer()},
           last_slot: integer()
         }

  ## API

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    queue = Keyword.fetch!(opts, :queue)
    %{id: {__MODULE__, queue.name}, start: {__MODULE__, :start_link, [opts]}}
  end

  @spec start_link(keyword()) :: :gen_statem.start_ret()
  def start_link(opts) do
    queue = Keyword.fetch!(opts, :queue)
    :gen_statem.start_link(via(queue.name), __MODULE__, opts, [])
  end

  @doc "Tells the leader of `queue` that there may be new work. Never blocks."
  @spec notify(String.t()) :: :ok
  def notify(queue), do: :gen_statem.cast({:global, global_name(queue)}, :dispatch)

  @doc """
  Tells the leader of `queue` that an execution of a job has ended and its
  slot is free. Never blocks.
  """
  @spec finished(String.t(), pos_integer(), pos_integer()) :: :ok
  def finished(queue, job_id, attempt) do
    :gen_statem.cast({:global, global_name(queue)}, {:finished, job_id, attempt})
  end

  @doc "Whether this node's scheduler for `queue` is the leader or a standby."
  @spec role(String.t()) :: :leader | :standby
  def role(queue), do: :gen_statem.call(via(queue), :role)

  @doc "The node whose scheduler leads `queue`, as seen from this node."
  @spec leader_node(String.t()) :: node() | nil
  def leader_node(queue) do
    case :global.whereis_name(global_name(queue)) do
      :undefined -> nil
      pid -> node(pid)
    end
  end

  defp via(queue), do: {:via, Registry, {Fairway.Registry, {:scheduler, queue}}}
  defp global_name(queue), do: {__MODULE__, queue}

  ## Callbacks

  @impl :gen_statem
  def callback_mode, do: [:state_functions, :state_enter]

  @impl :gen_statem
  def init(opts) do
    queue = Keyword.fetch!(opts, :queue)
    store = Keyword.fetch!(opts, :store)
    {:ok, :standby, %__MODULE__{queue: queue, store: store, policy: Fairness.module(queue.mode)}}
  end

  ## State: standby

  @doc false
  @spec standby(:gen_statem.event_type(), term(), data()) ::
          :gen_statem.state_enter_result(:standby) | :gen_statem.event_handler_result(:leader)
  def standby(:enter, _previous, _data) do
    {:keep_state_and_data, [{:state_timeout, 0, :campaign}]}
  end

  def standby(:state_timeout, :campaign, data), do: campaign(data)

  def standby(:info, {:DOWN, ref, :process, _pid, _reason}, %__MODULE__{leader_ref: ref} = data) do
    campaign(%{data | leader_ref: nil})
  end

  def standby({:call, from}, :role, _data) do
    {:keep_state_and_data, [{:reply, from, :standby}]}
  end

  def standby(_type, _event, _data), do: :keep_state_and_data

  defp campaign(data) do
    name = global_name(data.queue.name)

    # Learn of names registered on nodes we connected to a moment ago, so that
    # a node joining a cluster does not elect itself next to the real leader.
    _ = :global.sync()

    case :global.register_name(name, self(), &:global.random_notify_name/3) do
      :yes -> {:next_state, :leader, data}
      :no -> follow(data, :global.whereis_name(name))
    end
  end

  defp follow(data, :undefined) do
    {:keep_state, data, [{:state_timeout, @campaign_retry_ms, :campaign}]}
  end

  defp follow(data, leader) when is_pid(leader) do
    {:keep_state, %{data | leader_ref: Process.monitor(leader)}}
  end

  ## State: leader

  @doc false
  @spec leader(:gen_statem.event_type(), term(), data()) ::
          :gen_statem.state_enter_result(:leader) | :gen_statem.event_handler_result(:leader)
  def leader(:enter, _previous, data) do
    {group_ref, runners} = :pg.monitor(Fairway.PG, Runner.group(data.queue.name))
    Logger.info("fairway: #{node()} now schedules queue #{data.queue.name}")

    data = %{
      data
      | group_ref: group_ref,
        runners: by_node(runners),
        unreachable: [],
        policy_state: data.policy.init(data.queue.policy_opts),
        running: %{},
        unaccounted: %{}
    }

    {:keep_state, data, [{{:timeout, :poll}, 0, :poll}]}
  end

  def leader({:timeout, :poll}, :poll, data) do
    data
    |> reconcile()
    |> dispatch()
    |> keep([{{:timeout, :poll}, data.queue.poll_interval_ms, :poll}])
  end

  def leader({:timeout, :wake}, :wake, data), do: data |> dispatch() |> keep()

  def leader(:cast, :dispatch, data), do: data |> dispatch() |> keep()

  # Only the execution the leader is tracking frees the slot. A superseded one
  # may report after the job has been started again.
  def leader(:cast, {:finished, job_id, attempt}, data) do
    case data.running do
      %{^job_id => %{job: %Job{attempt: ^attempt}}} ->
        %{data | running: Map.delete(data.running, job_id)} |> dispatch() |> keep()

      _superseded_or_unknown ->
        :keep_state_and_data
    end
  end

  def leader(:info, {ref, :join, _group, pids}, %__MODULE__{group_ref: ref} = data) do
    %{data | runners: by_node(pids ++ data.runners)} |> dispatch() |> keep()
  end

  def leader(:info, {ref, :leave, _group, pids}, %__MODULE__{group_ref: ref} = data) do
    data |> abandon(pids) |> dispatch() |> keep()
  end

  def leader(:info, {:global_name_conflict, _name}, data) do
    Logger.warning("fairway: #{node()} lost queue #{data.queue.name} to another leader")
    {:stop, {:shutdown, :name_conflict}}
  end

  def leader({:call, from}, :role, _data) do
    {:keep_state_and_data, [{:reply, from, :leader}]}
  end

  def leader(_type, _event, _data), do: :keep_state_and_data

  defp keep({data, wait}, actions \\ []), do: {:keep_state, data, [wake(wait) | actions]}

  defp wake(nil), do: {{:timeout, :wake}, :cancel}
  defp wake(ms), do: {{:timeout, :wake}, ms, :wake}

  ## Dispatch

  # Returns the new data and, when the mode asked to be consulted again later,
  # in how many milliseconds.
  @spec dispatch(data()) :: {data(), pos_integer() | nil}
  defp dispatch(data) do
    case {data.runners -- data.unreachable, free_slots(data)} do
      {[], _free} ->
        {data, nil}

      {_runners, []} ->
        {data, nil}

      {runners, free} ->
        fill(data, runners, Store.ready_tenants(data.store, data.queue.name, wall_clock()), free)
    end
  end

  # Each pass either starts a job, which uses up a slot, or finds a tenant with
  # nothing left to claim, which shortens `ready`. So it ends.
  defp fill(data, _runners, [], _free), do: {data, nil}
  defp fill(data, _runners, _ready, []), do: {data, nil}

  defp fill(data, runners, ready, free) do
    now = monotonic()
    view = %{ready: ready, running: occupied(data), free: free, now: now}

    case data.policy.select(data.policy_state, view) do
      {:run, tenant, slot} ->
        # Slot `s` belongs to runner `s rem n`, in node order.
        runner = Enum.at(runners, rem(slot, length(runners)))

        case start(data, runner, tenant, slot, now) do
          {:ok, data} -> fill(data, runners, ready, List.delete(free, slot))
          :none -> fill(data, runners, List.delete(ready, tenant), free)
        end

      {:wait, ms} ->
        {data, ms}

      :idle ->
        {data, nil}
    end
  end

  defp start(data, runner, tenant, slot, now) do
    claim = %{node: Atom.to_string(node(runner)), slot: slot, now: wall_clock()}

    with {:ok, job} <- Store.claim(data.store, data.queue.name, tenant, claim) do
      Runner.run(runner, job)

      :telemetry.execute(
        [:fairway, :job, :start],
        %{at: now},
        %{job: job, queue: job.queue, tenant: tenant, slot: slot, node: node(runner)}
      )

      {:ok,
       %{
         data
         | running: Map.put(data.running, job.id, %{job: job, runner: runner}),
           policy_state: data.policy.started(data.policy_state, tenant, slot, now),
           last_slot: slot
       }}
    end
  end

  # Free slots, starting after the one used last, so that work spreads over
  # the nodes when the queue is not full.
  defp free_slots(data) do
    busy = MapSet.new(data.running, fn {_id, run} -> run.job.slot end)
    free = Enum.reject(0..(data.queue.concurrency - 1), &MapSet.member?(busy, &1))
    {upto, after_last} = Enum.split_while(free, &(&1 <= data.last_slot))
    after_last ++ upto
  end

  defp occupied(data) do
    Map.new(data.running, fn {_id, run} -> {run.job.slot, run.job.tenant} end)
  end

  ## Recovery

  # Runners are asked before the store is read. A job that finishes in between
  # is then missing from the store's list, which is harmless, and not missing
  # from the runner's, which would look like a lost job.
  #
  # The group is read afresh as well, so a join or leave whose message has not
  # arrived yet, or never does, is still seen within a poll.
  defp reconcile(data) do
    runners = by_node(:pg.get_members(Fairway.PG, Runner.group(data.queue.name)))
    probes = Map.new(runners, &{Atom.to_string(node(&1)), {&1, Runner.in_flight(&1)}})
    now = monotonic()

    {running, unaccounted} =
      data.store
      |> Store.running(data.queue.name)
      |> Enum.reduce({%{}, %{}}, fn job, {running, unaccounted} ->
        case locate(job, probes, Map.get(data.unaccounted, job.id, now), now, data.queue) do
          {:running, runner} ->
            {Map.put(running, job.id, %{job: job, runner: runner}), unaccounted}

          {:unaccounted, since, runner} ->
            {Map.put(running, job.id, %{job: job, runner: runner}),
             Map.put(unaccounted, job.id, since)}

          {:lost, why} ->
            _ = requeue(data, job, why)
            {running, unaccounted}
        end
      end)

    %{
      data
      | runners: runners,
        unreachable: for({_node, {runner, :unreachable}} <- probes, do: runner),
        running: running,
        unaccounted: unaccounted
    }
  end

  defp locate(%Job{node: node} = job, probes, since, now, queue) do
    case probes do
      %{^node => {runner, {:ok, jobs}}} ->
        if MapSet.member?(jobs, {job.id, job.attempt}),
          do: {:running, runner},
          else: {:lost, "#{node} is not running it"}

      %{^node => {runner, :unreachable}} ->
        wait_for(runner, since, now, queue, "#{node} does not answer")

      _no_runner ->
        wait_for(nil, since, now, queue, "#{node} is not in the cluster")
    end
  end

  defp wait_for(runner, since, now, queue, why) do
    if now - since >= queue.orphan_grace_ms, do: {:lost, why}, else: {:unaccounted, since, runner}
  end

  # Jobs are matched by node as well as by runner, because a job that was
  # unaccounted for may have no runner recorded against it.
  defp abandon(data, gone) do
    nodes = MapSet.new(gone, &Atom.to_string(node(&1)))

    {lost, running} =
      Map.split_with(data.running, fn {_id, run} ->
        run.runner in gone or (run.runner == nil and MapSet.member?(nodes, run.job.node))
      end)

    Enum.each(lost, fn {_id, run} -> requeue(data, run.job, "#{run.job.node} went down") end)

    %{
      data
      | runners: data.runners -- gone,
        unreachable: data.unreachable -- gone,
        running: running,
        unaccounted: Map.drop(data.unaccounted, Map.keys(lost))
    }
  end

  defp requeue(data, job, why) do
    now = wall_clock()
    error = "lost: #{why}"

    {result, outcome} =
      if Backoff.exhausted?(job) do
        {Store.discard(data.store, job.id, job.attempt, %{error: error, now: now}), :discarded}
      else
        change = %{run_at: now, error: error, cursor: job.cursor}
        {Store.retry(data.store, job.id, job.attempt, change), :retried}
      end

    # `:stale` means the job reported in after all, which is the good case.
    if result == :ok do
      Logger.warning("fairway: job #{job.id} on queue #{job.queue} #{outcome}; #{why}")

      :telemetry.execute(
        [:fairway, :job, :stop],
        %{system_time: now},
        %{job: job, queue: job.queue, tenant: job.tenant, outcome: {:lost, outcome}}
      )
    end

    result
  end

  defp by_node(runners), do: runners |> Enum.uniq() |> Enum.sort_by(&node/1)

  defp monotonic, do: System.monotonic_time(:millisecond)
  defp wall_clock, do: System.system_time(:millisecond)
end
