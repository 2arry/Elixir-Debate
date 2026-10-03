defmodule Fairway.Worker do
  @moduledoc """
  A job that runs to completion in one call.

      defmodule MyApp.SendEmail do
        @behaviour Fairway.Worker

        @impl true
        def perform(%Fairway.Job{args: %{"to" => to}}) do
          MyApp.Mailer.deliver(to)
        end
      end

  Return `:ok` or `{:ok, term}` to complete the job and `{:error, reason}` to
  fail it. Raising, throwing and exiting are failures too. A failed job is
  retried with backoff until its `max_attempts` is spent, then discarded.

  Delivery is at-least-once, so `perform/1` must be safe to run again.

  A worker of this kind cannot be interrupted. For long jobs that should give
  up their slot to other tenants, see `Fairway.IterableWorker`.
  """

  alias Fairway.Job

  @callback perform(job :: Job.t()) :: :ok | {:ok, term()} | {:error, term()}

  @type kind :: :perform | :iterable

  @doc """
  Finds the module a job names and how to run it.

  Job rows outlive deployments, so the name may refer to a module that no
  longer exists. That is an error for the job, never a new atom.
  """
  @spec resolve(String.t()) :: {:ok, {kind(), module()}} | {:error, String.t()}
  def resolve(name) when is_binary(name) do
    module = Module.safe_concat([name])

    with {:module, ^module} <- Code.ensure_loaded(module),
         {:ok, kind} <- kind(module) do
      {:ok, {kind, module}}
    else
      _unusable -> {:error, unusable(name)}
    end
  rescue
    ArgumentError -> {:error, unusable(name)}
  end

  defp kind(module) do
    cond do
      function_exported?(module, :step, 2) -> {:ok, :iterable}
      function_exported?(module, :perform, 1) -> {:ok, :perform}
      true -> :error
    end
  end

  defp unusable(name) do
    "worker #{name} is not a Fairway.Worker or Fairway.IterableWorker on #{node()}"
  end
end
