defmodule Fairway.Job do
  @moduledoc """
  A unit of work that belongs to one tenant on one queue.

  ## Fields that matter to delivery

    * `:state` - `:available` (waiting, possibly for `:run_at`), `:running`,
      `:completed` or `:discarded`.
    * `:attempt` - incremented every time the job is claimed, including when an
      interrupted job is resumed. Every acknowledgement carries it, so the
      store can tell the current execution from one it has already given up on.
    * `:failures` - executions that ended in an error or were lost with their
      node. The job is discarded when this reaches `:max_attempts`.
    * `:cursor` - progress saved by a `Fairway.IterableWorker`, `nil` until the
      job has been interrupted once.

  `:args` and `:cursor` are JSON values. They are normalised when a job is
  built, so a worker sees string keys whichever store the job came from.
  """

  @type state :: :available | :running | :completed | :discarded

  @type json ::
          nil | boolean() | number() | String.t() | [json()] | %{optional(String.t()) => json()}

  @type t :: %__MODULE__{
          id: pos_integer() | nil,
          queue: String.t(),
          tenant: String.t(),
          worker: String.t(),
          args: %{optional(String.t()) => json()},
          state: state(),
          attempt: non_neg_integer(),
          failures: non_neg_integer(),
          max_attempts: pos_integer() | nil,
          run_at: integer() | nil,
          cursor: json(),
          slot: non_neg_integer() | nil,
          node: String.t() | nil,
          last_error: String.t() | nil,
          inserted_at: integer() | nil,
          finished_at: integer() | nil
        }

  @enforce_keys [:queue, :tenant, :worker]
  defstruct [
    :id,
    :queue,
    :tenant,
    :worker,
    :max_attempts,
    :run_at,
    :cursor,
    :slot,
    :node,
    :last_error,
    :inserted_at,
    :finished_at,
    args: %{},
    state: :available,
    attempt: 0,
    failures: 0
  ]

  @doc """
  Builds an unsaved job from `attrs`.

  Required: `:queue`, `:tenant`, `:worker` (a module or its name).
  Optional: `:args` (a map, default `%{}`) and `:max_attempts`.

      iex> {:ok, job} = Fairway.Job.new(queue: "mail", tenant: "acme", worker: MyWorker, args: %{id: 1})
      iex> {job.worker, job.args}
      {"MyWorker", %{"id" => 1}}

      iex> Fairway.Job.new(queue: "mail", tenant: "", worker: MyWorker)
      {:error, "tenant must be a non-empty string"}
  """
  @spec new(Enumerable.t()) :: {:ok, t()} | {:error, String.t()}
  def new(attrs) do
    attrs = Map.new(attrs)

    with {:ok, queue} <- text(attrs, :queue),
         {:ok, tenant} <- text(attrs, :tenant),
         {:ok, worker} <- worker(attrs),
         {:ok, args} <- args(attrs),
         {:ok, max_attempts} <- max_attempts(attrs) do
      {:ok,
       %__MODULE__{
         queue: queue,
         tenant: tenant,
         worker: worker,
         args: args,
         max_attempts: max_attempts
       }}
    end
  end

  @doc """
  Round-trips `term` through JSON so that it is exactly what a store would
  hand back.

      iex> Fairway.Job.normalize(%{step: 3, tags: [:a]})
      {:ok, %{"step" => 3, "tags" => ["a"]}}

      iex> Fairway.Job.normalize({:not, :json})
      {:error, "is not a JSON value: {:not, :json}"}
  """
  @spec normalize(term()) :: {:ok, json()} | {:error, String.t()}
  def normalize(term) do
    {:ok, term |> JSON.encode!() |> JSON.decode!()}
  rescue
    _error in [Protocol.UndefinedError, ArgumentError, JSON.DecodeError] ->
      {:error, "is not a JSON value: #{inspect(term)}"}
  end

  defp text(attrs, key) do
    case attrs do
      %{^key => value} when is_binary(value) and value != "" -> {:ok, value}
      _other -> {:error, "#{key} must be a non-empty string"}
    end
  end

  defp worker(%{worker: worker}) when is_atom(worker) and not is_nil(worker) do
    {:ok, inspect(worker)}
  end

  defp worker(%{worker: worker}) when is_binary(worker) and worker != "", do: {:ok, worker}
  defp worker(_attrs), do: {:error, "worker must be a module or its name"}

  defp args(attrs) do
    case Map.get(attrs, :args, %{}) do
      args when is_map(args) and not is_struct(args) ->
        with {:error, reason} <- normalize(args), do: {:error, "args " <> reason}

      other ->
        {:error, "args must be a map, got: #{inspect(other)}"}
    end
  end

  defp max_attempts(attrs) do
    case Map.get(attrs, :max_attempts) do
      nil -> {:ok, nil}
      n when is_integer(n) and n > 0 -> {:ok, n}
      other -> {:error, "max_attempts must be a positive integer, got: #{inspect(other)}"}
    end
  end
end
