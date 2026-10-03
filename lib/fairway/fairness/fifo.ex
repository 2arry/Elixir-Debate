defmodule Fairway.Fairness.FIFO do
  @moduledoc """
  Oldest ready job first, whoever owns it.

  This is the unfair baseline. A tenant that enqueues a thousand jobs is served
  a thousand times before the tenant that arrived a moment later is served
  once. It exists so the tests have something to compare the other modes with.
  """

  @behaviour Fairway.Fairness

  @impl true
  def init(_opts), do: nil

  @impl true
  def select(_state, %{ready: [tenant | _], free: [slot | _]}), do: {:run, tenant, slot}
  def select(_state, _view), do: :idle

  @impl true
  def started(state, _tenant, _slot, _now), do: state
end
