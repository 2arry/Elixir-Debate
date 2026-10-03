defmodule Fairway.Test.Workers do
  @moduledoc false
  # Workers for the tests. Job args are JSON, so the test process travels in
  # them as a Base64 term.

  alias Fairway.Job

  @spec pid_arg(pid()) :: String.t()
  def pid_arg(pid), do: pid |> :erlang.term_to_binary() |> Base.encode64()

  @spec test_pid(Job.t()) :: pid()
  def test_pid(%Job{args: %{"test" => arg}}),
    do: arg |> Base.decode64!() |> :erlang.binary_to_term()

  defmodule Echo do
    @moduledoc false
    # Reports when it starts and finishes. Args:
    #   "hold"     - wait for `:release` from the test before doing anything else
    #   "sleep_ms" - how long the job takes
    #   "gauge"    - name of a public ETS table in which to count jobs of this
    #                tenant that are inside `perform/1` at once
    @behaviour Fairway.Worker

    alias Fairway.Test.Workers

    @impl true
    def perform(%Job{args: args} = job) do
      test = Workers.test_pid(job)
      running = enter(args["gauge"], job.tenant)
      send(test, {:started, job.tenant, job.id, self(), running})

      if args["hold"] do
        receive do
          :release -> :ok
        after
          30_000 -> raise "never released"
        end
      end

      Process.sleep(args["sleep_ms"] || 0)
      leave(args["gauge"], job.tenant)
      send(test, {:finished, job.tenant, job.id})
      :ok
    end

    defp enter(nil, _tenant), do: nil

    defp enter(gauge, tenant) do
      :ets.update_counter(String.to_existing_atom(gauge), tenant, 1, {tenant, 0})
    end

    defp leave(nil, _tenant), do: nil
    defp leave(gauge, tenant), do: :ets.update_counter(String.to_existing_atom(gauge), tenant, -1)
  end

  defmodule Flaky do
    @moduledoc false
    # Fails its first `"fail_times"` executions, then succeeds.
    @behaviour Fairway.Worker

    alias Fairway.Test.Workers

    @impl true
    def perform(%Job{args: %{"fail_times" => fail_times}} = job) do
      at = System.monotonic_time(:millisecond)
      send(Workers.test_pid(job), {:attempt, job.id, job.attempt, job.failures, at})
      if job.failures < fail_times, do: {:error, "failure #{job.failures + 1}"}, else: :ok
    end
  end

  defmodule Crash do
    @moduledoc false
    # Dies in the way `"how"` names, on its first `"crash_times"` executions.
    @behaviour Fairway.Worker

    alias Fairway.Test.Workers

    @impl true
    def perform(%Job{args: args} = job) do
      send(Workers.test_pid(job), {:attempt, job.id, job.attempt})
      if job.failures < Map.get(args, "crash_times", 1), do: crash(args["how"]), else: :ok
    end

    defp crash("raise"), do: raise("kaboom")
    defp crash("throw"), do: throw(:kaboom)
    defp crash("exit"), do: exit(:kaboom)
    defp crash("kill"), do: Process.exit(self(), :kill)
    defp crash("bad_return"), do: :whatever
  end

  defmodule Stepper do
    @moduledoc false
    # An iterable job of `"steps"` steps, each taking `"step_ms"`. Reports every
    # step with the attempt it ran in. `"fail_at"` makes that step fail once.
    @behaviour Fairway.IterableWorker

    alias Fairway.Test.Workers

    @impl true
    def init(_job), do: %{step: 0}

    @impl true
    def step(%Job{args: %{"steps" => steps}}, %{"step" => step}) when step >= steps, do: :done

    def step(%Job{args: args} = job, %{"step" => step}) do
      if args["fail_at"] == step and job.failures == 0 do
        {:error, "step #{step} failed"}
      else
        send(Workers.test_pid(job), {:step, job.id, step, job.attempt})
        Process.sleep(args["step_ms"] || 0)
        {:cont, %{step: step + 1}}
      end
    end
  end

  defmodule Journal do
    @moduledoc false
    # For cluster tests, where the test process is not reachable from the
    # nodes: appends a line to the file `"journal"` when it starts and when it
    # finishes. Each line is one small write, which O_APPEND keeps whole.
    @behaviour Fairway.Worker

    @impl true
    def perform(%Job{args: %{"journal" => journal} = args} = job) do
      log(journal, "start", job)
      Process.sleep(args["sleep_ms"] || 0)
      log(journal, "finish", job)
      :ok
    end

    defp log(journal, event, job) do
      File.write!(journal, "#{event} #{job.id} #{job.attempt} #{node()}\n", [:append])
    end
  end
end
