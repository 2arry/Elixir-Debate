defmodule Fairway.Supervisor do
  @moduledoc """
  The root of a running Fairway.

      Fairway.Supervisor                  rest_for_one
      ├── Registry                        local names, and the configuration
      ├── :pg scope                       which nodes run which queues
      ├── store adapter                   Memory, SQLite or Postgres
      ├── Cluster.Supervisor              libcluster, when a strategy is set
      └── Fairway.Queue.Supervisor        one per queue

  The order is the dependency order, and `rest_for_one` enforces it: if the
  store goes down, everything that reads it is restarted after it.

  The validated configuration is kept as metadata of the registry. It lives
  exactly as long as the tree does and is read without a process call.
  """

  use Supervisor

  alias Fairway.Config

  @registry Fairway.Registry

  @spec start_link(Config.t()) :: Supervisor.on_start()
  def start_link(%Config{} = config) do
    Supervisor.start_link(__MODULE__, config, name: __MODULE__)
  end

  @doc "The configuration of the Fairway running on this node."
  @spec config() :: {:ok, Config.t()} | :error
  def config do
    Registry.meta(@registry, :config)
  rescue
    # The registry, and so Fairway, is not running.
    ArgumentError -> :error
  end

  @doc "The handle the store was started under."
  @spec store(Config.t()) :: Fairway.Store.t()
  def store(%Config{store: {adapter, _opts}}), do: {adapter, Fairway.Store}

  @impl true
  def init(%Config{store: {adapter, store_opts}} = config) do
    {_adapter, store_name} = store = store(config)

    queues =
      for {_name, queue} <- Enum.sort(config.queues) do
        {Fairway.Queue.Supervisor, queue: queue, store: store}
      end

    children =
      [
        {Registry, keys: :unique, name: @registry, meta: [config: config]},
        %{id: :pg, start: {:pg, :start_link, [Fairway.PG]}},
        {adapter, Keyword.put(store_opts, :name, store_name)}
      ] ++ cluster(config.topologies) ++ queues

    Supervisor.init(children, strategy: :rest_for_one)
  end

  defp cluster([]), do: []

  defp cluster(topologies) do
    [{Cluster.Supervisor, [topologies, [name: Fairway.ClusterSupervisor]]}]
  end
end
