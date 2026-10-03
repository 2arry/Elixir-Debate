defmodule Fairway.IterableWorker do
  @moduledoc """
  A job made of steps, which can be interrupted between any two of them.

      defmodule MyApp.Export do
        @behaviour Fairway.IterableWorker

        @impl true
        def init(_job), do: 0

        @impl true
        def step(%Fairway.Job{args: %{"rows" => rows}}, offset) when offset >= rows, do: :done

        def step(job, offset) do
          MyApp.Exports.write_batch(job.args["export_id"], offset, 500)
          {:cont, offset + 500}
        end
      end

  `init/1` returns the starting cursor. `step/2` does one piece of work and
  returns `{:cont, next_cursor}`, `:done` or `{:error, reason}`.

  On a queue in `interruptible` mode the job runs for one time slice. When the
  slice is over, and after the step in progress has returned, the cursor is
  saved, the slot is given up and the job goes back to its tenant's queue. It
  later resumes from that cursor, possibly on another node. On queues in other
  modes the steps simply run to completion.

  ## What a worker can rely on

    * A step is never cut short; only the gap between steps is an interruption
      point. Keep steps short relative to the slice.
    * Without failures every step runs exactly once.
    * The cursor is saved when a slice ends and when a step returns an error or
      raises. If the node dies, steps since the last saved cursor run again.
    * The cursor must be a JSON value and comes back with string keys. `nil`
      means "not started", so `init/1` should not return `nil`.
  """

  alias Fairway.Job

  @callback init(job :: Job.t()) :: Job.json()

  @callback step(job :: Job.t(), cursor :: Job.json()) ::
              {:cont, Job.json()} | :done | {:error, term()}
end
