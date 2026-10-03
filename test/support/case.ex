defmodule Fairway.Test.Case do
  @moduledoc false
  # For tests that start a whole Fairway on the test node. There is one
  # Fairway per node, so these tests cannot be async.

  use ExUnit.CaseTemplate

  alias Fairway.Test.Workers

  using do
    quote do
      import Fairway.Test.Case

      alias Fairway.Test.Workers
    end
  end

  @doc "Starts Fairway from a YAML document, under the test's supervisor."
  @spec start_fairway!(String.t()) :: pid()
  def start_fairway!(yaml) do
    start_supervised!({Fairway, config: Fairway.Config.parse!(yaml)})
  end

  @doc """
  Forwards Fairway's telemetry to the calling test as
  `{:job_start, measurements, metadata}` and `{:job_stop, measurements, metadata}`.

  Start events come from the scheduler, one process, so they arrive in the
  order jobs were dispatched.
  """
  @spec forward_telemetry() :: :ok
  def forward_telemetry do
    id = {__MODULE__, self()}
    events = [[:fairway, :job, :start], [:fairway, :job, :stop]]
    :ok = :telemetry.attach_many(id, events, &__MODULE__.forward/4, self())
    ExUnit.Callbacks.on_exit(fn -> :telemetry.detach(id) end)
  end

  @doc false
  def forward([:fairway, :job, :start], measurements, metadata, test) do
    send(test, {:job_start, measurements, metadata})
  end

  def forward([:fairway, :job, :stop], measurements, metadata, test) do
    send(test, {:job_stop, measurements, metadata})
  end

  @doc "Job attributes for `count` jobs of `tenant` on queue `q`, run by `worker`."
  @spec jobs(String.t(), pos_integer(), module(), map()) :: [map()]
  def jobs(tenant, count, worker, args \\ %{}) do
    args = Map.put(args, :test, Workers.pid_arg(self()))
    for _n <- 1..count, do: %{queue: "q", tenant: tenant, worker: worker, args: args}
  end

  @doc "The next `count` dispatches, in order, as `{tenant, measurements, metadata}`."
  @spec starts(non_neg_integer(), timeout()) :: [{String.t(), map(), map()}]
  def starts(count, timeout \\ 5_000) do
    for _n <- 1..count//1 do
      receive do
        {:job_start, measurements, metadata} -> {metadata.tenant, measurements, metadata}
      after
        timeout ->
          ExUnit.Assertions.flunk("expected #{count} job starts within #{timeout} ms each")
      end
    end
  end

  @doc "Waits until `count` jobs of `tenant` have stopped with `outcome`."
  @spec await_stops(String.t(), atom(), non_neg_integer(), timeout()) :: :ok
  def await_stops(tenant, outcome, count, timeout \\ 5_000) do
    for _n <- 1..count//1 do
      receive do
        {:job_stop, _measurements, %{tenant: ^tenant, outcome: ^outcome}} -> :ok
      after
        timeout ->
          ExUnit.Assertions.flunk(
            "expected #{count} #{outcome} jobs of #{tenant} within #{timeout} ms each"
          )
      end
    end

    :ok
  end

  @doc "Retries `fun` until it returns a truthy value, for up to `timeout` ms."
  @spec eventually((-> term()), timeout()) :: term()
  def eventually(fun, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    poll(fun, deadline)
  end

  defp poll(fun, deadline) do
    case fun.() do
      falsy when falsy in [false, nil] ->
        if System.monotonic_time(:millisecond) > deadline do
          ExUnit.Assertions.flunk("condition was not met in time")
        else
          Process.sleep(20)
          poll(fun, deadline)
        end

      value ->
        value
    end
  end
end
