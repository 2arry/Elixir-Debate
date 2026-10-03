defmodule Fairway.Config do
  @moduledoc """
  Fairway's configuration, read from YAML and checked before anything starts.

      store:
        adapter: postgres            # memory (default) | sqlite | postgres
        url: postgres://fairway:secret@db.internal/fairway
        pool_size: 10

      cluster:
        strategy: epmd               # none (default) | epmd | gossip
        hosts: ["fairway@10.0.0.1", "fairway@10.0.0.2"]

      queues:
        emails:
          mode: per_tenant
          concurrency: 8
        webhooks:
          mode: throttle
          concurrency: 16
          throttle: {max_concurrency: 2, rate: 10, burst: 5}
        imports:
          mode: shuffle_shard
          concurrency: 8
          shuffle_shard: {shard_size: 2}
        exports:
          mode: interruptible
          concurrency: 4
          interruptible: {slice_ms: 500}
          retry: {max_attempts: 3, base_backoff_ms: 500, max_backoff_ms: 30000}

  ## Queue options

    * `mode` (required) - `fifo`, `per_tenant`, `throttle`, `shuffle_shard` or
      `interruptible`. See `Fairway.Fairness`.
    * `concurrency` (4) - slots, across the whole cluster.
    * `throttle`, `shuffle_shard`, `interruptible` - the section for the mode.
      Required for that mode and rejected for any other.
    * `retry` - `max_attempts` (5), `base_backoff_ms` (1000) and
      `max_backoff_ms` (60000). See `Fairway.Backoff`.
    * `poll_interval_ms` (1000) - how often the scheduler looks for jobs whose
      time has come and checks on running ones.
    * `orphan_grace_ms` (10000) - how long a running job may sit on a node the
      scheduler cannot see before it is re-queued.

  ## Validation

  Nothing is guessed. An unknown key, a wrong type, a missing section, a
  section for a different mode or an adapter whose driver is not installed is
  an error, and the error names the key:

      iex> {:error, error} = Fairway.Config.parse("queues: {emails: {mode: per_tenant, concurency: 8}}")
      iex> error.path
      "queues.emails.concurency"
  """

  alias Fairway.Fairness

  defmodule Error do
    @moduledoc """
    Why a configuration was rejected. `:path` is the offending key, written as
    it would be read in the file (`queues.emails.mode`), or `nil` when the
    document as a whole is at fault.
    """

    defexception [:path, :reason]

    @type t :: %__MODULE__{path: String.t() | nil, reason: String.t()}

    @impl true
    def message(%__MODULE__{path: nil, reason: reason}) do
      "invalid Fairway configuration: #{reason}"
    end

    def message(%__MODULE__{path: path, reason: reason}) do
      "invalid Fairway configuration at #{path}: #{reason}"
    end
  end

  defmodule Queue do
    @moduledoc """
    One queue's settings, as its processes receive them.

    `:policy_opts` is the argument for the mode's `c:Fairway.Fairness.init/1`.
    `:slice_ms` is `:infinity` unless the mode is `:interruptible`.
    """

    @enforce_keys [
      :name,
      :mode,
      :concurrency,
      :policy_opts,
      :slice_ms,
      :retry,
      :poll_interval_ms,
      :orphan_grace_ms
    ]
    defstruct @enforce_keys

    @type t :: %__MODULE__{
            name: String.t(),
            mode: Fairness.mode(),
            concurrency: pos_integer(),
            policy_opts: map(),
            slice_ms: pos_integer() | :infinity,
            retry: Fairway.Backoff.retry(),
            poll_interval_ms: pos_integer(),
            orphan_grace_ms: non_neg_integer()
          }
  end

  @enforce_keys [:store, :queues]
  defstruct [:store, :queues, topologies: []]

  @type t :: %__MODULE__{
          store: {adapter :: module(), opts :: keyword()},
          queues: %{optional(String.t()) => Queue.t()},
          topologies: keyword()
        }

  @typep result(value) :: {:ok, value} | {:error, Error.t()}

  # A schema is a keyword list of `key: {type, default}`. The default is
  # `:required`, a value, or `nil` for an optional section.
  @retry [
    max_attempts: {:pos_integer, 5},
    base_backoff_ms: {:pos_integer, 1_000},
    max_backoff_ms: {:pos_integer, 60_000}
  ]

  @throttle [
    max_concurrency: {:pos_integer, :required},
    rate: {:pos_number, :required},
    burst: {:pos_integer, :required}
  ]

  @shuffle_shard [shard_size: {:pos_integer, :required}, seed: {:integer, 0}]

  @interruptible [slice_ms: {:pos_integer, :required}]

  @queue [
    mode: {{:one_of, Fairness.modes()}, :required},
    concurrency: {:pos_integer, 4},
    poll_interval_ms: {:pos_integer, 1_000},
    orphan_grace_ms: {:non_neg_integer, 10_000},
    retry: {{:map, @retry}, %{}},
    throttle: {{:map, @throttle}, nil},
    shuffle_shard: {{:map, @shuffle_shard}, nil},
    interruptible: {{:map, @interruptible}, nil}
  ]

  @store [
    adapter: {{:one_of, [:memory, :sqlite, :postgres]}, :memory},
    path: {:string, nil},
    url: {:string, nil},
    pool_size: {:pos_integer, nil}
  ]

  @cluster [
    strategy: {{:one_of, [:none, :epmd, :gossip]}, :none},
    hosts: {{:list, :string}, nil}
  ]

  @root [
    store: {{:map, @store}, %{}},
    cluster: {{:map, @cluster}, %{}},
    queues: {{:each, {:map, @queue}}, :required}
  ]

  # The section each mode requires, and which no other mode may have.
  @sections [:throttle, :shuffle_shard, :interruptible]

  # Options that belong to one adapter only.
  @store_options [path: :sqlite, url: :postgres, pool_size: :postgres]

  @drivers %{
    memory: {Fairway.Store.Memory, nil},
    sqlite: {Fairway.Store.SQLite, :exqlite},
    postgres: {Fairway.Store.Postgres, :postgrex}
  }

  @doc "Reads and validates the YAML file at `path`."
  @spec load(Path.t()) :: result(t())
  def load(path) do
    case YamlElixir.read_from_file(path) do
      {:ok, document} -> from_map(document)
      {:error, exception} -> error(nil, "cannot read #{path}: #{Exception.message(exception)}")
    end
  end

  @doc "Validates a YAML document given as a string."
  @spec parse(String.t()) :: result(t())
  def parse(yaml) when is_binary(yaml) do
    case YamlElixir.read_from_string(yaml) do
      {:ok, document} -> from_map(document)
      {:error, exception} -> error(nil, "not valid YAML: #{Exception.message(exception)}")
    end
  end

  @doc "Like `parse/1`, but raises `Fairway.Config.Error`."
  @spec parse!(String.t()) :: t()
  def parse!(yaml) do
    case parse(yaml) do
      {:ok, config} -> config
      {:error, error} -> raise error
    end
  end

  @doc "Validates an already decoded document: a map with string keys."
  @spec from_map(term()) :: result(t())
  def from_map(document) when is_map(document) do
    with {:ok, root} <- cast(document, {:map, @root}, nil),
         {:ok, store} <- store(root.store),
         {:ok, topologies} <- topologies(root.cluster),
         {:ok, queues} <- queues(root.queues) do
      {:ok, %__MODULE__{store: store, topologies: topologies, queues: queues}}
    end
  end

  def from_map(other) do
    error(nil, "expected a mapping at the top level, got: #{inspect(other)}")
  end

  ## Shape: types, required keys, unknown keys

  defp cast(value, {:map, schema}, path) when is_map(value) do
    with :ok <- known_keys(value, schema, path) do
      collect(schema, fn key, {type, default} ->
        field(value, key, type, default, join(path, key))
      end)
    end
  end

  defp cast(value, {:each, type}, path) when is_map(value) and map_size(value) > 0 do
    value
    |> Enum.sort_by(fn {name, _entry} -> to_string(name) end)
    |> collect(fn name, entry -> named(name, entry, type, join(path, name)) end)
  end

  defp cast(value, {:list, type}, path) when is_list(value) and value != [] do
    indexed = Enum.with_index(value, fn item, index -> {index, item} end)

    with {:ok, cast} <-
           collect(indexed, fn index, item -> cast(item, type, "#{path}[#{index}]") end) do
      {:ok, Enum.map(indexed, fn {index, _item} -> Map.fetch!(cast, index) end)}
    end
  end

  # The choices are atoms and the value is looked up among them. Converting the
  # value with `String.to_existing_atom/1` would only work on a node that
  # happened to have loaded a module mentioning the atom already.
  defp cast(value, {:one_of, choices}, path) do
    case Enum.find(choices, &(Atom.to_string(&1) == value)) do
      nil -> error(path, "expected one of #{Enum.join(choices, ", ")}; got: #{inspect(value)}")
      choice -> {:ok, choice}
    end
  end

  defp cast(value, :string, _path) when is_binary(value) and value != "", do: {:ok, value}
  defp cast(value, :integer, _path) when is_integer(value), do: {:ok, value}
  defp cast(value, :pos_integer, _path) when is_integer(value) and value > 0, do: {:ok, value}

  defp cast(value, :non_neg_integer, _path) when is_integer(value) and value >= 0,
    do: {:ok, value}

  defp cast(value, :pos_number, _path) when is_number(value) and value > 0, do: {:ok, value}

  defp cast(value, type, path),
    do: error(path, "expected #{describe(type)}; got: #{inspect(value)}")

  defp field(map, key, type, default, path) do
    case {Map.get(map, Atom.to_string(key)), default} do
      {nil, :required} -> error(path, "is required")
      {nil, %{} = empty} -> cast(empty, type, path)
      {nil, default} -> {:ok, default}
      {value, _default} -> cast(value, type, path)
    end
  end

  defp known_keys(map, schema, path) do
    known = Enum.map(schema, fn {key, _spec} -> Atom.to_string(key) end)

    case map |> Map.keys() |> Enum.sort_by(&to_string/1) |> Enum.find(&(&1 not in known)) do
      nil -> :ok
      key -> error(join(path, key), "unknown key; expected one of: #{Enum.join(known, ", ")}")
    end
  end

  # A queue or any other user-named entry: the name must be a usable string.
  defp named(name, entry, type, path) when is_binary(name) and name != "" do
    cast(entry, type, path)
  end

  defp named(_name, _entry, _type, path), do: error(path, "names must be non-empty strings")

  # Casts every `{key, input}` pair with `fun` and gathers the results into a
  # map under the same keys, stopping at the first error.
  defp collect(pairs, fun) do
    Enum.reduce_while(pairs, {:ok, %{}}, fn {key, input}, {:ok, acc} ->
      case fun.(key, input) do
        {:ok, value} -> {:cont, {:ok, Map.put(acc, key, value)}}
        {:error, %Error{}} = error -> {:halt, error}
      end
    end)
  end

  defp describe({:map, _schema}), do: "a mapping"
  defp describe({:each, _type}), do: "a non-empty mapping"
  defp describe({:list, _type}), do: "a non-empty list"
  defp describe(:string), do: "a non-empty string"
  defp describe(:integer), do: "an integer"
  defp describe(:pos_integer), do: "a positive integer"
  defp describe(:non_neg_integer), do: "a non-negative integer"
  defp describe(:pos_number), do: "a positive number"

  ## Meaning: rules that span more than one key

  defp store(%{adapter: adapter} = store) do
    {module, driver} = Map.fetch!(@drivers, adapter)

    with :ok <- own_options(store, adapter),
         :ok <- driver_loaded(module, adapter, driver),
         {:ok, opts} <- store_opts(store) do
      {:ok, {module, opts}}
    end
  end

  defp own_options(store, adapter) do
    case Enum.find(@store_options, fn {key, owner} -> owner != adapter and store[key] != nil end) do
      nil -> :ok
      {key, owner} -> error("store.#{key}", "only applies to the #{owner} adapter")
    end
  end

  defp driver_loaded(module, adapter, driver) do
    if Code.ensure_loaded?(module) do
      :ok
    else
      error("store.adapter", "#{adapter} needs the optional :#{driver} dependency")
    end
  end

  defp store_opts(%{adapter: :memory}), do: {:ok, []}
  defp store_opts(%{adapter: :sqlite, path: nil}), do: error("store.path", "is required")
  defp store_opts(%{adapter: :sqlite, path: path}), do: {:ok, [path: path]}
  defp store_opts(%{adapter: :postgres, url: nil}), do: error("store.url", "is required")

  defp store_opts(%{adapter: :postgres, url: url, pool_size: pool_size}) do
    {:ok, [url: url, pool_size: pool_size || 5]}
  end

  defp topologies(%{strategy: :none, hosts: nil}), do: {:ok, []}

  defp topologies(%{strategy: strategy, hosts: hosts}) when strategy != :epmd and hosts != nil do
    error("cluster.hosts", "only applies to the epmd strategy")
  end

  defp topologies(%{strategy: :epmd, hosts: nil}), do: error("cluster.hosts", "is required")

  defp topologies(%{strategy: strategy, hosts: hosts}) do
    if Code.ensure_loaded?(Cluster.Supervisor) do
      {:ok, [fairway: topology(strategy, hosts)]}
    else
      error("cluster.strategy", "#{strategy} needs the optional :libcluster dependency")
    end
  end

  # Node names become atoms here. They come from the operator's own file and
  # there are as many as there are nodes.
  defp topology(:epmd, hosts) do
    [strategy: Cluster.Strategy.Epmd, config: [hosts: Enum.map(hosts, &String.to_atom/1)]]
  end

  defp topology(:gossip, nil), do: [strategy: Cluster.Strategy.Gossip]

  defp queues(queues) do
    collect(queues, fn name, settings -> queue(name, settings, "queues.#{name}") end)
  end

  defp queue(name, settings, path) do
    with :ok <- sections(settings, path),
         :ok <- backoff(settings.retry, path),
         {:ok, policy_opts} <- policy_opts(settings, path) do
      {:ok,
       %Queue{
         name: name,
         mode: settings.mode,
         concurrency: settings.concurrency,
         policy_opts: policy_opts,
         slice_ms: slice_ms(settings),
         retry: settings.retry,
         poll_interval_ms: settings.poll_interval_ms,
         orphan_grace_ms: settings.orphan_grace_ms
       }}
    end
  end

  defp sections(%{mode: mode} = settings, path) do
    Enum.find_value(@sections, :ok, fn section ->
      case {section == mode, Map.fetch!(settings, section)} do
        {true, nil} -> error(join(path, section), "is required when mode is #{mode}")
        {false, %{}} -> error(join(path, section), "only applies when mode is #{section}")
        _consistent -> nil
      end
    end)
  end

  defp backoff(%{base_backoff_ms: base, max_backoff_ms: max}, path) when base > max do
    error("#{path}.retry.base_backoff_ms", "must not exceed max_backoff_ms (#{max})")
  end

  defp backoff(_retry, _path), do: :ok

  defp policy_opts(%{mode: :throttle, throttle: throttle}, _path), do: {:ok, throttle}

  defp policy_opts(%{mode: :shuffle_shard, shuffle_shard: shard, concurrency: slots}, path) do
    if shard.shard_size <= slots do
      {:ok, Map.put(shard, :slots, slots)}
    else
      error("#{path}.shuffle_shard.shard_size", "must not exceed concurrency (#{slots})")
    end
  end

  defp policy_opts(_settings, _path), do: {:ok, %{}}

  defp slice_ms(%{mode: :interruptible, interruptible: %{slice_ms: slice_ms}}), do: slice_ms
  defp slice_ms(_settings), do: :infinity

  defp join(nil, key), do: to_string(key)
  defp join(path, key), do: "#{path}.#{key}"

  defp error(path, reason), do: {:error, %Error{path: path, reason: reason}}
end
