defmodule Fairway do
  @moduledoc """
  A fair, distributed, multi-tenant job orchestrator.

  Jobs belong to tenants. Each queue has a fairness mode that decides whose
  job starts next, so that one tenant's backlog cannot starve the others.
  Jobs run on any node of a BEAM cluster, are delivered at least once, are
  retried with backoff, and are recovered when the node running them dies.

  ## Starting

      children = [
        {Fairway, config_file: "/etc/fairway.yml"}
      ]

  See `Fairway.Config` for the file, and `Fairway.Application` for starting at
  boot instead. One Fairway runs per node.

  ## Enqueueing

      Fairway.enqueue(queue: "emails", tenant: "acme", worker: MyApp.SendEmail, args: %{to: "a@b.c"})

  See `Fairway.Worker` and `Fairway.IterableWorker` for writing workers, and
  `Fairway.Fairness` for the modes.

  ## Telemetry

    * `[:fairway, :job, :start]` - the queue's scheduler has claimed a job and
      sent it to a node. Emitted on the scheduler's node. Measurement `:at` is
      the scheduler's monotonic clock in milliseconds; metadata is `:job`,
      `:queue`, `:tenant`, `:slot` and `:node`.
    * `[:fairway, :job, :stop]` - an execution has ended and its outcome is in
      the store. Emitted on the node that ran the job. Measurement
      `:system_time` is in milliseconds; metadata is `:job`, `:queue`,
      `:tenant` and `:outcome`, one of `:completed`, `:retried`, `:discarded`,
      `:yielded` (interrupted, to be resumed) and `:stale` (the execution had
      been superseded and its report was dropped). When a scheduler gives up
      on an execution whose node is gone, it emits the event itself with
      `{:lost, :retried}` or `{:lost, :discarded}`.
  """

  alias Fairway.{Config, Job, Store}
  alias Fairway.Queue.Scheduler

  @type attrs :: Enumerable.t()

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}, type: :supervisor}
  end

  @doc """
  Starts Fairway from `config_file: path` or from `config: %Fairway.Config{}`.

  An invalid file is not started: the result is
  `{:error, %Fairway.Config.Error{}}`, whose `:path` is the key at fault.
  """
  @spec start_link(keyword()) :: Supervisor.on_start() | {:error, Config.Error.t()}
  def start_link(opts) do
    with {:ok, config} <- config(opts) do
      Fairway.Supervisor.start_link(config)
    end
  end

  @doc """
  Saves one job and tells its queue's scheduler.

  `attrs` needs `:queue`, `:tenant` and `:worker`, and may have:

    * `:args` - a map of JSON values, `%{}` by default.
    * `:max_attempts` - overrides the queue's `retry.max_attempts`.
    * `:schedule_in` - milliseconds to wait before the job may start.
  """
  @spec enqueue(attrs()) :: {:ok, Job.t()} | {:error, String.t()}
  def enqueue(attrs) do
    with {:ok, [job]} <- enqueue_all([attrs]), do: {:ok, job}
  end

  @doc """
  Saves many jobs at once. Either all are saved or, if any is invalid, none.
  Ids follow the order of the list.
  """
  @spec enqueue_all([attrs()]) :: {:ok, [Job.t()]} | {:error, String.t()}
  def enqueue_all(entries) when is_list(entries) do
    with {:ok, config} <- running(),
         {:ok, jobs} <- build_all(entries, config, System.system_time(:millisecond)),
         {:ok, saved} <- Store.insert_all(Fairway.Supervisor.store(config), jobs) do
      saved |> Enum.map(& &1.queue) |> Enum.uniq() |> Enum.each(&Scheduler.notify/1)
      {:ok, saved}
    end
  end

  @doc "Looks a job up by id."
  @spec fetch(pos_integer()) :: {:ok, Job.t()} | :error
  def fetch(id), do: Store.fetch(store!(), id)

  @doc """
  How many jobs of `queue` each tenant has in each state.

      Fairway.counts("emails")
      #=> %{{"acme", :available} => 190, {"acme", :running} => 2, {"zen", :completed} => 10}
  """
  @spec counts(String.t()) :: %{optional({String.t(), Job.state()}) => pos_integer()}
  def counts(queue), do: Store.counts(store!(), queue)

  @doc "The node whose scheduler currently leads `queue`, or `nil` during an election."
  @spec leader(String.t()) :: node() | nil
  defdelegate leader(queue), to: Scheduler, as: :leader_node

  defp config(opts) do
    case Keyword.take(opts, [:config, :config_file]) do
      [config: %Config{} = config] -> {:ok, config}
      [config_file: path] when is_binary(path) -> Config.load(path)
      _other -> raise ArgumentError, "expected config: %Fairway.Config{} or config_file: path"
    end
  end

  defp running do
    with :error <- Fairway.Supervisor.config() do
      {:error, "Fairway is not running on #{node()}"}
    end
  end

  defp store! do
    case running() do
      {:ok, config} -> Fairway.Supervisor.store(config)
      {:error, message} -> raise RuntimeError, message
    end
  end

  defp build_all(entries, config, now) do
    entries
    |> Enum.reduce_while({:ok, []}, fn attrs, {:ok, jobs} ->
      case build(Map.new(attrs), config, now) do
        {:ok, job} -> {:cont, {:ok, [job | jobs]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, jobs} -> {:ok, Enum.reverse(jobs)}
      {:error, _reason} = error -> error
    end
  end

  defp build(attrs, config, now) do
    with {:ok, job} <- Job.new(attrs),
         {:ok, queue} <- queue(config, job.queue),
         {:ok, delay} <- schedule_in(attrs) do
      {:ok,
       %{
         job
         | max_attempts: job.max_attempts || queue.retry.max_attempts,
           run_at: now + delay,
           inserted_at: now
       }}
    end
  end

  defp queue(config, name) do
    with :error <- Map.fetch(config.queues, name) do
      {:error,
       "unknown queue #{inspect(name)}; configured: #{config.queues |> Map.keys() |> Enum.join(", ")}"}
    end
  end

  defp schedule_in(attrs) do
    case Map.get(attrs, :schedule_in, 0) do
      ms when is_integer(ms) and ms >= 0 ->
        {:ok, ms}

      other ->
        {:error,
         "schedule_in must be a non-negative number of milliseconds, got: #{inspect(other)}"}
    end
  end
end
