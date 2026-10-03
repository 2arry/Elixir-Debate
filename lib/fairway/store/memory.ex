defmodule Fairway.Store.Memory do
  @moduledoc """
  Jobs in an ETS table. For development, tests and single-node use.

  The table belongs to this process and every operation is a call to it, which
  is what makes a claim atomic. Nothing is written to disk and nothing is
  shared between nodes: in a cluster each node would have its own, different,
  set of jobs. Lookups scan the table in id order, which is fine for thousands
  of jobs and not meant for millions.
  """

  @behaviour Fairway.Store

  use GenServer

  alias Fairway.Job

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, :ok, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl Fairway.Store
  def insert_all(server, jobs), do: GenServer.call(server, {:insert_all, jobs})

  @impl Fairway.Store
  def ready_tenants(server, queue, now), do: GenServer.call(server, {:ready_tenants, queue, now})

  @impl Fairway.Store
  def claim(server, queue, tenant, claim),
    do: GenServer.call(server, {:claim, queue, tenant, claim})

  @impl Fairway.Store
  def complete(server, id, attempt, now) do
    ack(server, id, attempt, &%{&1 | state: :completed, slot: nil, finished_at: now})
  end

  @impl Fairway.Store
  def retry(server, id, attempt, %{run_at: run_at, error: error, cursor: cursor}) do
    ack(server, id, attempt, fn job ->
      %{
        job
        | state: :available,
          slot: nil,
          failures: job.failures + 1,
          run_at: run_at,
          last_error: error,
          cursor: cursor
      }
    end)
  end

  @impl Fairway.Store
  def discard(server, id, attempt, %{error: error, now: now}) do
    ack(server, id, attempt, fn job ->
      %{
        job
        | state: :discarded,
          slot: nil,
          failures: job.failures + 1,
          last_error: error,
          finished_at: now
      }
    end)
  end

  @impl Fairway.Store
  def yield(server, id, attempt, cursor) do
    ack(server, id, attempt, &%{&1 | state: :available, slot: nil, cursor: cursor})
  end

  @impl Fairway.Store
  def running(server, queue), do: GenServer.call(server, {:running, queue})

  @impl Fairway.Store
  def fetch(server, id), do: GenServer.call(server, {:fetch, id})

  @impl Fairway.Store
  def counts(server, queue), do: GenServer.call(server, {:counts, queue})

  defp ack(server, id, attempt, change), do: GenServer.call(server, {:ack, id, attempt, change})

  @impl GenServer
  def init(:ok) do
    {:ok, %{table: :ets.new(__MODULE__, [:ordered_set, :private]), next_id: 1}}
  end

  @impl GenServer
  def handle_call({:insert_all, jobs}, _from, %{table: table, next_id: next_id} = state) do
    saved = jobs |> Enum.with_index(next_id) |> Enum.map(fn {job, id} -> %{job | id: id} end)
    true = :ets.insert(table, Enum.map(saved, &{&1.id, &1}))
    {:reply, {:ok, saved}, %{state | next_id: next_id + length(saved)}}
  end

  def handle_call({:ready_tenants, queue, now}, _from, %{table: table} = state) do
    spec = [{{:_, available(queue, :"$1")}, [{:"=<", :"$2", now}], [:"$1"]}]
    {:reply, table |> :ets.select(spec) |> Enum.uniq(), state}
  end

  def handle_call({:claim, queue, tenant, claim}, _from, %{table: table} = state) do
    spec = [{{:_, available(queue, tenant)}, [{:"=<", :"$2", claim.now}], [:"$_"]}]

    reply =
      case :ets.select(table, spec, 1) do
        {[{id, job}], _continuation} ->
          job = %{
            job
            | state: :running,
              attempt: job.attempt + 1,
              node: claim.node,
              slot: claim.slot
          }

          true = :ets.insert(table, {id, job})
          {:ok, job}

        :"$end_of_table" ->
          :none
      end

    {:reply, reply, state}
  end

  def handle_call({:ack, id, attempt, change}, _from, %{table: table} = state) do
    reply =
      case :ets.lookup(table, id) do
        [{^id, %Job{state: :running, attempt: ^attempt} = job}] ->
          true = :ets.insert(table, {id, change.(job)})
          :ok

        _missing_or_superseded ->
          {:error, :stale}
      end

    {:reply, reply, state}
  end

  def handle_call({:running, queue}, _from, %{table: table} = state) do
    spec = [{{:_, %{queue: queue, state: :running}}, [], [:"$_"]}]
    {:reply, table |> :ets.select(spec) |> Enum.map(&elem(&1, 1)), state}
  end

  def handle_call({:fetch, id}, _from, %{table: table} = state) do
    reply =
      case :ets.lookup(table, id) do
        [{^id, job}] -> {:ok, job}
        [] -> :error
      end

    {:reply, reply, state}
  end

  def handle_call({:counts, queue}, _from, %{table: table} = state) do
    spec = [{{:_, %{queue: queue, tenant: :"$1", state: :"$2"}}, [], [{{:"$1", :"$2"}}]}]
    {:reply, table |> :ets.select(spec) |> Enum.frequencies(), state}
  end

  # The match-spec pattern for a tenant's waiting jobs, binding `run_at` to `$2`
  # for the caller's guard. An `:ordered_set` is traversed in key order, so the
  # first match is the oldest.
  defp available(queue, tenant) do
    %{queue: queue, tenant: tenant, state: :available, run_at: :"$2"}
  end
end
