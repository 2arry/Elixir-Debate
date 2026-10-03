defmodule Fairway.Store.Row do
  @moduledoc false
  # Shared by the SQL adapters: one column order, one JSON codec.

  alias Fairway.Job

  @columns ~w(id queue tenant worker args state attempt failures max_attempts run_at
              progress slot node last_error inserted_at finished_at)

  @doc "Column names in the order `to_job/1` expects. The cursor is stored as `progress`."
  @spec columns() :: [String.t()]
  def columns, do: @columns

  @spec to_job([term()]) :: Job.t()
  def to_job([
        id,
        queue,
        tenant,
        worker,
        args,
        state,
        attempt,
        failures,
        max_attempts,
        run_at,
        progress,
        slot,
        node,
        last_error,
        inserted_at,
        finished_at
      ]) do
    %Job{
      id: id,
      queue: queue,
      tenant: tenant,
      worker: worker,
      args: JSON.decode!(args),
      state: state(state),
      attempt: attempt,
      failures: failures,
      max_attempts: max_attempts,
      run_at: run_at,
      cursor: decode(progress),
      slot: slot,
      node: node,
      last_error: last_error,
      inserted_at: inserted_at,
      finished_at: finished_at
    }
  end

  @spec encode(Job.json()) :: String.t() | nil
  def encode(nil), do: nil
  def encode(json), do: JSON.encode!(json)

  @spec counts([[term()]]) :: %{optional({String.t(), Job.state()}) => pos_integer()}
  def counts(rows) do
    Map.new(rows, fn [tenant, state, count] -> {{tenant, state(state)}, count} end)
  end

  defp decode(nil), do: nil
  defp decode(json), do: JSON.decode!(json)

  defp state("available"), do: :available
  defp state("running"), do: :running
  defp state("completed"), do: :completed
  defp state("discarded"), do: :discarded
end
