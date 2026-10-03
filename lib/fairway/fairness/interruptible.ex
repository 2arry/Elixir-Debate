defmodule Fairway.Fairness.Interruptible do
  @moduledoc """
  Time-sliced execution, with slots re-assigned round-robin across tenants.
  Configured as `mode: interruptible`.

      interruptible:
        slice_ms: 500

  Round-robin over starts is not enough when jobs are long: once every slot is
  held by one tenant's hour-long jobs, nobody else starts anything for an hour.
  In this mode a job written as a `Fairway.IterableWorker` holds its slot for
  one slice. Then its cursor is saved, the slot is released, and the next
  tenant on the ring gets it. The interrupted job waits its turn again and
  resumes where it stopped.

  Which tenant gets a slot is decided exactly as in
  `Fairway.Fairness.RoundRobin`; this module adds nothing to that. The slicing
  is done where the job runs, by `Fairway.Queue.Executor`.

  ## Guarantee

  When every slot is held by iterable jobs, a tenant that arrives waits at most
  one slice, plus the step in progress, for a slot.

  ## Limits

    * Only iterable workers can be interrupted. A plain `Fairway.Worker` keeps
      its slot until it returns.
    * A step is never cut short. One slow step delays the hand-over by as long
      as it takes.
    * The cursor is saved at the end of a slice, not after every step. If the
      node dies mid-slice the steps of that slice run again.
    * A job is interrupted at the end of its slice even when nobody is waiting.
      It is picked up again at once; the cost is one store write per slice.
  """

  @behaviour Fairway.Fairness

  alias Fairway.Fairness.RoundRobin

  @impl true
  defdelegate init(opts), to: RoundRobin

  @impl true
  defdelegate select(state, view), to: RoundRobin

  @impl true
  defdelegate started(state, tenant, slot, now), to: RoundRobin
end
