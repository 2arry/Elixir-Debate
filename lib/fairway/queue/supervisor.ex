defmodule Fairway.Queue.Supervisor do
  @moduledoc """
  The processes one queue needs on one node.

      Fairway.Queue.Supervisor            one_for_one
      ├── execution                       one_for_all
      │   ├── Task.Supervisor             the jobs
      │   └── Fairway.Queue.Runner        starts jobs, records outcomes
      └── Fairway.Queue.Scheduler         leader or standby

  The runner and its tasks live and die together: tasks whose runner is gone
  would finish with nobody to record them. The scheduler is independent of
  both. Restarting it changes who decides, not what is running.
  """

  use Supervisor

  alias Fairway.Queue.{Runner, Scheduler}

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    queue = Keyword.fetch!(opts, :queue)

    %{
      id: {__MODULE__, queue.name},
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor
    }
  end

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts), do: Supervisor.start_link(__MODULE__, opts)

  @impl true
  def init(opts) do
    queue = Keyword.fetch!(opts, :queue)

    execution = [
      {Task.Supervisor, name: Runner.task_supervisor(queue.name)},
      {Runner, opts}
    ]

    children = [
      %{
        id: :execution,
        start: {Supervisor, :start_link, [execution, [strategy: :one_for_all]]},
        type: :supervisor
      },
      {Scheduler, opts}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
