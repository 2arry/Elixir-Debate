defmodule Fairway.Queue.Executor do
  @moduledoc """
  Runs one job inside its task and reduces whatever happens to an outcome.

  A plain `Fairway.Worker` is called once. A `Fairway.IterableWorker` is
  stepped until it is done or until `slice_ms` have passed, in which case the
  outcome is `{:yield, cursor}` and the job will be resumed from that cursor.

  Errors, raises, throws and exits inside the worker all become
  `{:error, message, cursor}`, where the cursor is the progress made before the
  failing step, so a retry does not repeat steps that succeeded. Only the death
  of the task itself, which this code cannot observe, loses the slice.
  """

  alias Fairway.{Job, Worker}

  @type outcome :: :ok | {:yield, Job.json()} | {:error, String.t(), Job.json()}

  @spec run(Job.t(), pos_integer() | :infinity) :: outcome()
  def run(%Job{} = job, slice_ms) do
    case Worker.resolve(job.worker) do
      {:ok, {:perform, module}} -> perform(module, job)
      {:ok, {:iterable, module}} -> iterate(module, job, deadline(slice_ms))
      {:error, message} -> {:error, message, job.cursor}
    end
  end

  defp perform(module, job) do
    case guarded(fn -> module.perform(job) end) do
      :ok ->
        :ok

      {:ok, _value} ->
        :ok

      {:error, reason} ->
        {:error, describe(reason), job.cursor}

      {:crashed, message} ->
        {:error, message, job.cursor}

      other ->
        {:error, "bad return from #{inspect(module)}.perform/1: #{inspect(other)}", job.cursor}
    end
  end

  defp iterate(module, %Job{cursor: nil} = job, deadline) do
    case guarded(fn -> module.init(job) end) do
      {:crashed, message} ->
        {:error, message, nil}

      initial ->
        with {:ok, cursor} <- checked(initial, "#{inspect(module)}.init/1", nil) do
          step(module, job, cursor, deadline)
        end
    end
  end

  defp iterate(module, %Job{cursor: cursor} = job, deadline) do
    step(module, job, cursor, deadline)
  end

  # `saved` is the cursor before this step: what a retry should resume from.
  defp step(module, job, saved, deadline) do
    case guarded(fn -> module.step(job, saved) end) do
      {:cont, next} -> continue(module, job, next, saved, deadline)
      :done -> :ok
      {:error, reason} -> {:error, describe(reason), saved}
      {:crashed, message} -> {:error, message, saved}
      other -> {:error, "bad return from #{inspect(module)}.step/2: #{inspect(other)}", saved}
    end
  end

  defp continue(module, job, next, saved, deadline) do
    with {:ok, cursor} <- checked(next, "#{inspect(module)}.step/2", saved) do
      if expired?(deadline), do: {:yield, cursor}, else: step(module, job, cursor, deadline)
    end
  end

  # Every cursor goes through JSON as soon as the worker returns it, not only
  # when it is about to be stored. The worker then sees the same shape whether
  # or not the job was interrupted, and a cursor that cannot be stored fails
  # the step that produced it.
  defp checked(cursor, source, saved) do
    case Job.normalize(cursor) do
      {:ok, nil} -> {:error, "cursor from #{source} must not be nil", saved}
      {:ok, cursor} -> {:ok, cursor}
      {:error, reason} -> {:error, "cursor from #{source} #{reason}", saved}
    end
  end

  defp guarded(fun) do
    fun.()
  catch
    kind, reason -> {:crashed, Exception.format(kind, reason, __STACKTRACE__)}
  end

  defp describe(reason) when is_binary(reason), do: reason
  defp describe(reason) when is_exception(reason), do: Exception.message(reason)
  defp describe(reason), do: inspect(reason)

  defp deadline(:infinity), do: :infinity
  defp deadline(slice_ms), do: System.monotonic_time(:millisecond) + slice_ms

  defp expired?(:infinity), do: false
  defp expired?(deadline), do: System.monotonic_time(:millisecond) >= deadline
end
