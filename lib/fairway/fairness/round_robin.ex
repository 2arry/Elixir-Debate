defmodule Fairway.Fairness.RoundRobin do
  @moduledoc """
  Per-tenant queues, served round-robin. Configured as `mode: per_tenant`.

  Each tenant's jobs form their own queue, oldest first. Whenever a slot is
  free, the next tenant on the ring that has a ready job starts one.

  ## Guarantee

  Between two consecutive starts for one tenant, every other tenant with ready
  jobs gets a start. So with `T` tenants waiting, a tenant's next job is at
  most `T - 1` starts away, however many jobs the others have queued.

  ## Limits

    * Fair in job starts, not in time. A tenant with long jobs holds its slots
      longer than a tenant with short ones; `interruptible` mode addresses that.
    * No weights or priorities: every tenant counts the same.
    * A job that is already running is never displaced.
  """

  @behaviour Fairway.Fairness

  alias Fairway.Fairness

  @impl true
  def init(_opts), do: nil

  @impl true
  def select(_last, %{ready: []}), do: :idle
  def select(_last, %{free: []}), do: :idle

  def select(last, %{ready: ready, free: [slot | _]}) do
    [tenant | _] = Fairness.ring(ready, last)
    {:run, tenant, slot}
  end

  @impl true
  def started(_last, tenant, _slot, _now), do: tenant
end
