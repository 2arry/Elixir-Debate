defmodule Fairway.JobTest do
  # The pure pieces around a job: building one, deciding its retries, finding
  # its worker and running it.
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Fairway.{Backoff, Job, Worker}
  alias Fairway.Queue.Executor

  doctest Job
  doctest Backoff

  defmodule Plain do
    @behaviour Fairway.Worker

    @impl true
    def perform(%Job{args: %{"return" => "ok"}}), do: :ok
    def perform(%Job{args: %{"return" => "value"}}), do: {:ok, 42}
    def perform(%Job{args: %{"return" => "error"}}), do: {:error, :nope}

    def perform(%Job{args: %{"return" => "exception"}}),
      do: {:error, %ArgumentError{message: "bad arg"}}

    def perform(%Job{args: %{"return" => "raise"}}), do: raise("boom")
  end

  defmodule Counter do
    @behaviour Fairway.IterableWorker

    @impl true
    def init(%Job{args: args}), do: Map.get(args, "from", 0)

    @impl true
    def step(%Job{args: %{"upto" => upto}}, n) when n >= upto, do: :done
    def step(%Job{args: %{"bad_cursor_at" => n}}, n), do: {:cont, {:not, :json}}
    def step(%Job{args: %{"nil_cursor_at" => n}}, n), do: {:cont, nil}
    def step(%Job{args: %{"raise_at" => n}}, n), do: raise("step #{n} blew up")

    def step(%Job{args: args}, n) do
      Process.sleep(Map.get(args, "slow", 0))
      {:cont, n + 1}
    end
  end

  defmodule NotAWorker do
    def hello, do: :world
  end

  defp job(worker, args, attrs \\ []) do
    struct!(
      %Job{queue: "q", tenant: "t", worker: inspect(worker), args: args, max_attempts: 3},
      attrs
    )
  end

  describe "Job.new/1" do
    test "accepts a keyword list or a map, and a worker as a module or a name" do
      assert {:ok,
              %Job{
                queue: "q",
                tenant: "t",
                worker: "Fairway.JobTest.Plain",
                args: %{},
                state: :available
              }} =
               Job.new(queue: "q", tenant: "t", worker: Plain)

      assert {:ok, %Job{worker: "Some.Worker", max_attempts: 2}} =
               Job.new(%{queue: "q", tenant: "t", worker: "Some.Worker", max_attempts: 2})
    end

    test "normalises args to the JSON a store would return" do
      assert {:ok, %Job{args: %{"a" => %{"b" => ["c", 1, nil, true]}}}} =
               Job.new(
                 queue: "q",
                 tenant: "t",
                 worker: Plain,
                 args: %{a: %{b: [:c, 1, nil, true]}}
               )
    end

    test "says what is wrong" do
      assert Job.new(tenant: "t", worker: Plain) == {:error, "queue must be a non-empty string"}

      assert Job.new(queue: "q", tenant: :t, worker: Plain) ==
               {:error, "tenant must be a non-empty string"}

      assert Job.new(queue: "q", tenant: "t", worker: nil) ==
               {:error, "worker must be a module or its name"}

      assert Job.new(queue: "q", tenant: "t", worker: Plain, args: [1]) ==
               {:error, "args must be a map, got: [1]"}

      assert {:error, "args is not a JSON value: " <> _} =
               Job.new(queue: "q", tenant: "t", worker: Plain, args: %{t: {1, 2}})

      assert {:error, "max_attempts must be a positive integer, got: 0"} =
               Job.new(queue: "q", tenant: "t", worker: Plain, max_attempts: 0)
    end
  end

  describe "Backoff" do
    @retry %{max_attempts: 5, base_backoff_ms: 100, max_backoff_ms: 1_000}

    test "a job is spent when its next failure is its last attempt" do
      refute Backoff.exhausted?(job(Plain, %{}, failures: 0))
      refute Backoff.exhausted?(job(Plain, %{}, failures: 1))
      assert Backoff.exhausted?(job(Plain, %{}, failures: 2))
      assert Backoff.exhausted?(job(Plain, %{}, failures: 0, max_attempts: 1))
    end

    property "the delay doubles from the base, stops at the cap, and carries up to 10% of jitter" do
      check all(failures <- integer(1..200)) do
        expected = min(1_000, 100 * Integer.pow(2, min(failures - 1, 40)))
        delay = Backoff.delay(failures, @retry)

        assert delay >= expected
        assert delay <= expected + div(expected, 10)
      end
    end

    test "jitter spreads jobs that failed together" do
      delays = for _n <- 1..200, do: Backoff.delay(3, @retry)
      assert delays |> Enum.uniq() |> length() > 10
    end
  end

  describe "Worker.resolve/1" do
    test "finds both kinds of worker" do
      assert Worker.resolve("Fairway.JobTest.Plain") == {:ok, {:perform, Plain}}
      assert Worker.resolve("Fairway.JobTest.Counter") == {:ok, {:iterable, Counter}}
    end

    test "refuses a module that is not a worker, and a name that is not a module" do
      assert {:error, message} = Worker.resolve("Fairway.JobTest.NotAWorker")
      assert message =~ "worker Fairway.JobTest.NotAWorker is not a Fairway.Worker"

      assert {:error, _message} = Worker.resolve("Fairway.JobTest.NeverDefined")
      assert {:error, _message} = Worker.resolve("not even a module name")
    end
  end

  describe "Executor.run/2 with a plain worker" do
    test "completes on :ok and on {:ok, value}" do
      assert Executor.run(job(Plain, %{"return" => "ok"}), :infinity) == :ok
      assert Executor.run(job(Plain, %{"return" => "value"}), :infinity) == :ok
    end

    test "turns an error return into a message" do
      assert Executor.run(job(Plain, %{"return" => "error"}), :infinity) == {:error, ":nope", nil}

      assert Executor.run(job(Plain, %{"return" => "exception"}), :infinity) ==
               {:error, "bad arg", nil}
    end

    test "turns a raise into a message with the stacktrace" do
      assert {:error, message, nil} = Executor.run(job(Plain, %{"return" => "raise"}), :infinity)
      assert message =~ "** (RuntimeError) boom"
      assert message =~ "job_test.exs"
    end

    test "fails a job whose worker cannot be found, keeping its cursor" do
      assert {:error, message, 7} = Executor.run(job(NotAWorker, %{}, cursor: 7), :infinity)
      assert message =~ "is not a Fairway.Worker"
    end
  end

  describe "Executor.run/2 with an iterable worker" do
    test "runs every step when there is no slice" do
      assert Executor.run(job(Counter, %{"upto" => 1_000}), :infinity) == :ok
    end

    test "starts from init/1, or from the saved cursor when there is one" do
      assert Executor.run(job(Counter, %{"upto" => 5, "raise_at" => 3}), :infinity) |> elem(2) ==
               3

      assert Executor.run(job(Counter, %{"upto" => 5, "raise_at" => 3}, cursor: 4), :infinity) ==
               :ok

      assert Executor.run(job(Counter, %{"upto" => 5, "from" => 4, "raise_at" => 3}), :infinity) ==
               :ok
    end

    test "yields the cursor when the slice is over, after at least one step" do
      # Steps of at least 5 ms in a 20 ms slice: at least one, at most four.
      assert {:yield, cursor} = Executor.run(job(Counter, %{"upto" => 1_000, "slow" => 5}), 20)
      assert cursor in 1..4

      # A slice too short for any step still makes progress.
      assert Executor.run(job(Counter, %{"upto" => 1_000, "slow" => 5}, cursor: 10), 1) ==
               {:yield, 11}
    end

    test "a failing step reports the cursor to resume from" do
      assert {:error, message, 3} =
               Executor.run(job(Counter, %{"upto" => 5, "raise_at" => 3}), :infinity)

      assert message =~ "step 3 blew up"
    end

    test "a cursor that cannot be stored fails the step that returned it" do
      assert {:error, message, 2} =
               Executor.run(job(Counter, %{"upto" => 5, "bad_cursor_at" => 2}), :infinity)

      assert message =~ "cursor from Fairway.JobTest.Counter.step/2 is not a JSON value"

      assert {:error, message, 2} =
               Executor.run(job(Counter, %{"upto" => 5, "nil_cursor_at" => 2}), :infinity)

      assert message =~ "must not be nil"
    end
  end
end
