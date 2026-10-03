defmodule Fairway.Backoff do
  @moduledoc """
  Decides what happens to a job after a failed execution.

  A job gets `max_attempts` executions that may fail. Between them it waits
  `base_backoff_ms * 2^(failures - 1)`, capped at `max_backoff_ms`, plus up to
  10% of jitter so that jobs which failed together do not retry together.
  """

  alias Fairway.Job

  @type retry :: %{
          max_attempts: pos_integer(),
          base_backoff_ms: pos_integer(),
          max_backoff_ms: pos_integer()
        }

  # 2^40 ms is about 35 years; past that the cap has long since applied.
  @max_shift 40

  @doc """
  Whether one more failure uses up the job's attempts.

      iex> Fairway.Backoff.exhausted?(%Fairway.Job{queue: "q", tenant: "t", worker: "W", failures: 4, max_attempts: 5})
      true

      iex> Fairway.Backoff.exhausted?(%Fairway.Job{queue: "q", tenant: "t", worker: "W", failures: 0, max_attempts: 5})
      false
  """
  @spec exhausted?(Job.t()) :: boolean()
  def exhausted?(%Job{failures: failures, max_attempts: max_attempts}) do
    failures + 1 >= max_attempts
  end

  @doc """
  Milliseconds to wait before the retry that follows failure number `failures`.
  """
  @spec delay(pos_integer(), retry()) :: pos_integer()
  def delay(failures, %{base_backoff_ms: base, max_backoff_ms: max}) when failures >= 1 do
    backoff = min(max, base * Integer.pow(2, min(failures - 1, @max_shift)))
    backoff + :rand.uniform(div(backoff, 10) + 1) - 1
  end
end
