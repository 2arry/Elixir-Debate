defmodule Fairway.LifecycleTest do
  # What happens to one job: delivery, retries with backoff, discarding, and
  # recovery when the processes around it die. On one node, with the memory
  # store; node death is covered by Fairway.ClusterTest.
  use Fairway.Test.Case, async: false

  alias Fairway.{Job, Store}
  alias Fairway.Queue.Scheduler
  alias Fairway.Test.Workers.{Crash, Echo, Flaky, Stepper}

  setup context do
    forward_telemetry()

    start_fairway!("""
    queues:
      q:
        mode: per_tenant
        concurrency: 2
        poll_interval_ms: 20
        orphan_grace_ms: #{Map.get(context, :orphan_grace_ms, 300)}
        retry: {max_attempts: 3, base_backoff_ms: 100, max_backoff_ms: 150}
    """)

    :ok
  end

  defp enqueue!(worker, args \\ %{}, extra \\ %{}) do
    [attrs] = jobs("acme", 1, worker, args)
    {:ok, job} = Fairway.enqueue(Map.merge(attrs, extra))
    job
  end

  defp stop(job) do
    id = job.id
    assert_receive {:job_stop, _measurements, %{job: %Job{id: ^id}, outcome: outcome}}, 5_000
    outcome
  end

  defp fetch!(job) do
    {:ok, saved} = Fairway.fetch(job.id)
    saved
  end

  defp store do
    {:ok, config} = Fairway.Supervisor.config()
    Fairway.Supervisor.store(config)
  end

  defp process(key) do
    [{pid, _value}] = Registry.lookup(Fairway.Registry, {key, "q"})
    pid
  end

  describe "delivery" do
    test "a job runs once and is completed" do
      job = enqueue!(Echo)

      assert stop(job) == :completed

      assert %Job{state: :completed, attempt: 1, failures: 0, last_error: nil, finished_at: at} =
               fetch!(job)

      assert is_integer(at)

      assert_received {:started, "acme", _id, _pid, _running}
      refute_receive {:started, _tenant, _id, _pid, _running}, 100
    end

    test "the worker receives its args as JSON" do
      job = enqueue!(Echo, %{nested: %{list: [1, :two, "three"]}})

      assert %{"nested" => %{"list" => [1, "two", "three"]}} = job.args
      assert stop(job) == :completed
    end

    test "a job scheduled for later does not start early" do
      enqueued_at = System.monotonic_time(:millisecond)
      job = enqueue!(Echo, %{}, %{schedule_in: 300})

      assert %Job{state: :available} = fetch!(job)
      assert [{"acme", %{at: started_at}, _metadata}] = starts(1)
      assert started_at - enqueued_at >= 300
      assert stop(job) == :completed
    end

    test "this node leads the queue" do
      eventually(fn -> Scheduler.role("q") == :leader end)
      assert Fairway.leader("q") == node()
    end
  end

  describe "retries" do
    test "a failing job is retried after a backoff that grows, and can then succeed" do
      job = enqueue!(Flaky, %{fail_times: 2})

      assert [stop(job), stop(job), stop(job)] == [:retried, :retried, :completed]

      assert [{1, 0, first}, {2, 1, second}, {3, 2, third}] =
               for({:attempt, _id, attempt, failures, at} <- drain(), do: {attempt, failures, at})

      # 100 ms, then min(200, 150) ms, each with up to 10% added.
      assert second - first >= 100
      assert third - second >= 150

      assert %Job{state: :completed, attempt: 3, failures: 2, last_error: "failure 2"} =
               fetch!(job)
    end

    test "a job that keeps failing is discarded when its attempts are spent" do
      job = enqueue!(Flaky, %{fail_times: 99})

      assert [stop(job), stop(job), stop(job)] == [:retried, :retried, :discarded]

      assert %Job{
               state: :discarded,
               attempt: 3,
               failures: 3,
               last_error: "failure 3",
               finished_at: at
             } = fetch!(job)

      assert is_integer(at)

      # Three executions, and no fourth however long we wait.
      assert [1, 2, 3] == for({:attempt, _id, attempt, _failures, _at} <- drain(), do: attempt)
      refute_receive {:attempt, _id, _attempt, _failures, _at}, 400
    end

    test "max_attempts can be set per job" do
      job = enqueue!(Flaky, %{fail_times: 99}, %{max_attempts: 1})

      assert stop(job) == :discarded
      assert %Job{state: :discarded, attempt: 1, failures: 1} = fetch!(job)
    end

    for {how, error} <- [
          {"raise", "(RuntimeError) kaboom"},
          {"throw", "(throw) :kaboom"},
          {"exit", "(exit) :kaboom"},
          {"kill", "exited: killed"},
          {"bad_return", "bad return from Fairway.Test.Workers.Crash.perform/1: :whatever"}
        ] do
      test "a job that fails by #{how} is retried like any other failure" do
        job = enqueue!(Crash, %{how: unquote(how)})

        assert [stop(job), stop(job)] == [:retried, :completed]
        assert %Job{state: :completed, attempt: 2, failures: 1, last_error: error} = fetch!(job)
        assert error =~ unquote(error)
      end
    end

    test "a job whose worker does not exist fails with a reason, and is not an atom leak" do
      job = enqueue!("No.Such.Worker", %{}, %{max_attempts: 1})

      assert stop(job) == :discarded
      assert %Job{state: :discarded, last_error: error} = fetch!(job)
      assert error =~ "worker No.Such.Worker is not a Fairway.Worker"
      assert_raise ArgumentError, fn -> String.to_existing_atom("Elixir.No.Such.Worker") end
    end

    test "an iterable job that fails resumes at the step that failed" do
      job = enqueue!(Stepper, %{steps: 6, fail_at: 3})

      assert [stop(job), stop(job)] == [:retried, :completed]

      # Steps before the failure are not repeated; every step runs once.
      assert [{0, 1}, {1, 1}, {2, 1}, {3, 2}, {4, 2}, {5, 2}] =
               for({:step, _id, step, attempt} <- drain(), do: {step, attempt})

      assert %Job{state: :completed, failures: 1, cursor: %{"step" => 3}} = fetch!(job)
    end
  end

  describe "recovery" do
    test "jobs running when the runner crashes are run again" do
      jobs = [enqueue!(Echo, %{hold: true}), enqueue!(Echo, %{hold: true})]
      for _job <- jobs, do: assert_receive({:started, "acme", _id, _pid, _running}, 5_000)

      Process.exit(process(:runner), :kill)

      # The tasks died with their runner; the same two jobs start again.
      restarted =
        for _job <- jobs, do: assert_receive({:started, "acme", _id, _pid, _running}, 5_000)

      assert restarted |> Enum.map(&elem(&1, 2)) |> Enum.sort() == Enum.map(jobs, & &1.id)
      for {:started, _tenant, _id, pid, _running} <- restarted, do: send(pid, :release)

      await_stops("acme", :completed, 2)

      for job <- jobs do
        assert %Job{state: :completed, attempt: 2, failures: 1, last_error: "lost: " <> _why} =
                 fetch!(job)
      end
    end

    test "a scheduler crash does not disturb the jobs that are running" do
      held = [enqueue!(Echo, %{hold: true}), enqueue!(Echo, %{hold: true})]
      waiting = [enqueue!(Echo), enqueue!(Echo)]

      holders =
        for _job <- held, do: assert_receive({:started, "acme", _id, _pid, _running}, 5_000)

      before = process(:scheduler)
      Process.exit(before, :kill)

      eventually(fn ->
        match?(
          [{pid, _value}] when pid != before,
          Registry.lookup(Fairway.Registry, {:scheduler, "q"})
        ) and
          Fairway.leader("q") == node()
      end)

      # The new scheduler found the running jobs and left them alone, even
      # after several polls.
      refute_receive {:started, _tenant, _id, _pid, _running}, 200
      for {:started, _tenant, _id, pid, _running} <- holders, do: send(pid, :release)

      await_stops("acme", :completed, 4)

      for job <- held ++ waiting do
        assert %Job{state: :completed, attempt: 1, failures: 0} = fetch!(job)
      end
    end

    test "a job that was claimed but never reached its node is run" do
      job = strand!(Atom.to_string(node()))

      assert stop(job) == {:lost, :retried}
      assert stop(job) == :completed
      assert %Job{state: :completed, attempt: 2, failures: 1, last_error: error} = fetch!(job)
      assert error =~ "is not running it"
    end

    test "a job on a node that is not in the cluster is run after the grace period, not before" do
      stranded_at = System.monotonic_time(:millisecond)
      job = strand!("ghost@nowhere")

      assert [{"acme", %{at: restarted_at}, %{job: %Job{attempt: 2}}}] = starts(1)
      assert restarted_at - stranded_at >= 300

      assert stop(job) == {:lost, :retried}
      assert stop(job) == :completed
      assert %Job{last_error: "lost: ghost@nowhere is not in the cluster"} = fetch!(job)
    end

    test "a job that is lost with its node too often is discarded" do
      job = strand!(Atom.to_string(node()), 1)

      assert stop(job) == {:lost, :discarded}
      assert %Job{state: :discarded, failures: 1} = fetch!(job)
    end

    @tag orphan_grace_ms: 10_000
    test "a runner that stops answering gets no new work, and keeps its jobs if it recovers" do
      held = enqueue!(Echo, %{hold: true})
      assert_receive {:started, "acme", _id, holder, _running}, 5_000
      assert [{"acme", _measurements, %{slot: 0}}] = starts(1)

      # The runner is alive but answers nothing, like a node that is swamped.
      # Give the scheduler time to notice: one poll and one unanswered probe.
      runner = process(:runner)
      :ok = :sys.suspend(runner)
      Process.sleep(1_300)

      # A second slot is free, but nothing is sent to a runner that is silent.
      waiting = enqueue!(Echo)
      refute_receive {:job_start, _measurements, _metadata}, 500
      assert %Job{state: :available, attempt: 0} = fetch!(waiting)

      # Within the grace period its job is neither given up on nor run again.
      assert %Job{state: :running, attempt: 1, failures: 0} = fetch!(held)

      :ok = :sys.resume(runner)
      assert stop(waiting) == :completed

      send(holder, :release)
      assert stop(held) == :completed
      assert %Job{state: :completed, attempt: 1, failures: 0} = fetch!(held)
    end

    test "an outcome reported for a superseded execution is dropped" do
      job = enqueue!(Echo, %{hold: true})
      assert_receive {:started, "acme", _id, zombie, _running}, 5_000

      # Give the job to someone else behind the runner's back, as a scheduler
      # on the far side of a partition would.
      :ok = Store.retry(store(), job.id, 1, %{run_at: 0, error: "presumed dead", cursor: nil})
      assert_receive {:started, "acme", _id, current, _running} when current != zombie, 5_000

      send(zombie, :release)
      assert stop(job) == :stale
      assert %Job{state: :running, attempt: 2} = fetch!(job)

      send(current, :release)
      assert stop(job) == :completed
      assert %Job{state: :completed, attempt: 2, failures: 1} = fetch!(job)
    end
  end

  describe "enqueue" do
    test "rejects a queue that is not configured" do
      assert {:error, message} = Fairway.enqueue(queue: "nope", tenant: "acme", worker: Echo)
      assert message == ~s(unknown queue "nope"; configured: q)
    end

    test "rejects a job that is not well-formed" do
      assert {:error, "tenant must be a non-empty string"} =
               Fairway.enqueue(queue: "q", worker: Echo)

      assert {:error, "worker must be a module or its name"} =
               Fairway.enqueue(queue: "q", tenant: "acme")

      assert {:error, "args is not a JSON value: " <> _} =
               Fairway.enqueue(queue: "q", tenant: "acme", worker: Echo, args: %{pid: self()})

      assert {:error, "schedule_in must be" <> _} =
               Fairway.enqueue(queue: "q", tenant: "acme", worker: Echo, schedule_in: -1)

      assert {:error, "max_attempts must be" <> _} =
               Fairway.enqueue(queue: "q", tenant: "acme", worker: Echo, max_attempts: 0)
    end

    test "saves a batch whole or not at all" do
      good = jobs("acme", 3, Echo, %{hold: true})

      assert {:error, _reason} =
               Fairway.enqueue_all(good ++ [%{queue: "q", tenant: "", worker: Echo}])

      assert Fairway.counts("q") == %{}

      assert {:ok, [first, second, third]} = Fairway.enqueue_all(good)
      assert first.id < second.id and second.id < third.id
      assert Fairway.counts("q") |> Map.values() |> Enum.sum() == 3
    end

    test "says so when Fairway is not running" do
      stop_supervised!(Fairway)

      assert {:error, message} = Fairway.enqueue(queue: "q", tenant: "acme", worker: Echo)
      assert message =~ "Fairway is not running"
      assert_raise RuntimeError, ~r/Fairway is not running/, fn -> Fairway.counts("q") end
    end
  end

  # Puts a job in the store as `:running` on `node` without telling anyone:
  # what a scheduler leaves behind if it dies between claiming a job and
  # sending it. The job is dated in the future, and claimed as of then, so that
  # the live scheduler cannot pick it up first.
  defp strand!(node, max_attempts \\ 3) do
    [attrs] = jobs("acme", 1, Echo)
    {:ok, job} = Job.new(attrs)
    later = System.system_time(:millisecond) + 60_000
    stranded = %{job | max_attempts: max_attempts, run_at: later, inserted_at: later}

    {:ok, [saved]} = Store.insert_all(store(), [stranded])

    {:ok, %Job{state: :running, attempt: 1}} =
      Store.claim(store(), "q", "acme", %{node: node, slot: 0, now: later})

    saved
  end

  defp drain do
    receive do
      message -> [message | drain()]
    after
      0 -> []
    end
  end
end
