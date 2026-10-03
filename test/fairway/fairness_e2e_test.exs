defmodule Fairway.FairnessE2ETest do
  # Acceptance criterion B: one end-to-end test per mode. A noisy tenant floods
  # the queue, a quiet tenant arrives afterwards, and the test checks what the
  # mode promises about the quiet tenant.
  #
  # "Start" means the scheduler's decision to start a job, observed through
  # the `[:fairway, :job, :start]` telemetry event. The scheduler is one
  # process, so those events arrive in dispatch order and carry the clock the
  # fairness mode saw. Messages from worker processes are used for what only a
  # worker can know: how many jobs were inside `perform/1` at once, and which
  # steps ran.
  use Fairway.Test.Case, async: false

  alias Fairway.Fairness.ShuffleShard
  alias Fairway.Test.Workers.{Echo, Stepper}

  setup do
    forward_telemetry()
  end

  defp tenants(starts), do: Enum.map(starts, fn {tenant, _measurements, _metadata} -> tenant end)

  # 200 noisy jobs, then 10 quiet ones, on 4 slots. The first four noisy jobs
  # are held until the quiet tenant has enqueued, so "the quiet tenant arrived
  # while the queue was full of noisy work" is a fact, not a race.
  defp flood_then_quiet(mode) do
    start_fairway!("""
    queues:
      q:
        mode: #{mode}
        concurrency: 4
    """)

    held = jobs("noisy", 4, Echo, %{hold: true})
    {:ok, _noisy} = Fairway.enqueue_all(held ++ jobs("noisy", 196, Echo, %{sleep_ms: 2}))

    holders =
      for _n <- 1..4,
          do: assert_receive({:started, "noisy", _id, pid, _running} when is_pid(pid), 5_000)

    {:ok, _quiet} = Fairway.enqueue_all(jobs("quiet", 10, Echo, %{sleep_ms: 2}))
    for {:started, "noisy", _id, pid, _running} <- holders, do: send(pid, :release)

    210 |> starts() |> tenants()
  end

  describe "fifo (the control)" do
    test "quiet jobs start only after every earlier noisy job" do
      order = flood_then_quiet("fifo")

      assert Enum.take(order, 200) == List.duplicate("noisy", 200)
      assert Enum.drop(order, 200) == List.duplicate("quiet", 10)
    end
  end

  describe "per-tenant queues" do
    test "200 noisy then 10 quiet jobs on 4 slots: all 10 quiet jobs start within the first 30 starts" do
      order = flood_then_quiet("per_tenant")

      assert order |> Enum.take(30) |> Enum.count(&(&1 == "quiet")) == 10

      # And why: once the quiet tenant is waiting, the two take turns.
      assert order |> Enum.drop(4) |> Enum.take(20) ==
               List.flatten(List.duplicate(["quiet", "noisy"], 10))
    end
  end

  describe "throttling" do
    @max_concurrency 2
    @rate 20
    @burst 5

    setup do
      gauge = :ets.new(:fairway_e2e_gauge, [:named_table, :public])

      start_fairway!("""
      queues:
        q:
          mode: throttle
          concurrency: 8
          poll_interval_ms: 50
          throttle: {max_concurrency: #{@max_concurrency}, rate: #{@rate}, burst: #{@burst}}
      """)

      %{gauge: Atom.to_string(gauge)}
    end

    test "noisy stays under its concurrency cap and under rate plus burst in any second; " <>
           "quiet finishes while the noisy backlog remains",
         %{gauge: gauge} do
      # Unthrottled, two at a time, these 60 jobs would take under a second.
      {:ok, _noisy} = Fairway.enqueue_all(jobs("noisy", 60, Echo, %{sleep_ms: 25, gauge: gauge}))
      assert_receive {:started, "noisy", _id, _pid, first}, 5_000

      {:ok, _quiet} = Fairway.enqueue_all(jobs("quiet", 5, Echo, %{sleep_ms: 25, gauge: gauge}))

      # The quiet tenant finishes while the noisy backlog remains.
      await_stops("quiet", :completed, 5)
      backlog = Fairway.counts("q")[{"noisy", :available}]
      assert backlog > 0

      await_stops("noisy", :completed, 60, 10_000)
      starts = starts(65)

      # Concurrency: the count each job saw on entering perform/1. The cap is
      # reached, so it is the cap that holds, and it is never passed.
      running = [first | for({:started, "noisy", _id, _pid, running} <- drain(), do: running)]
      assert length(running) == 60
      assert Enum.max(running) == @max_concurrency

      # Rate: no window of one second holds more than rate + burst starts.
      at = for {"noisy", %{at: at}, _metadata} <- starts, do: at
      assert length(at) == 60

      for from <- at do
        in_window = Enum.count(at, &(&1 >= from and &1 < from + 1_000))
        assert in_window <= @rate + @burst
      end

      # And the limit is what held the tenant back, not a slow test machine: a
      # bucket that starts full cannot let 60 jobs go in less than
      # (60 - burst) / rate seconds. Unthrottled they would take under one.
      assert List.last(at) - hd(at) >= div((60 - @burst) * 1_000, @rate)
    end
  end

  describe "shuffle sharding" do
    @shards %{slots: 8, shard_size: 2, seed: 0}

    setup do
      start_fairway!("""
      queues:
        q:
          mode: shuffle_shard
          concurrency: #{@shards.slots}
          shuffle_shard: {shard_size: #{@shards.shard_size}, seed: #{@shards.seed}}
      """)

      :ok
    end

    test "assignment is deterministic; noisy never leaves its shard; a quiet tenant sharing " <>
           "one shard member finishes while at least half the noisy backlog is waiting" do
      state = ShuffleShard.init(@shards)
      noisy_shard = ShuffleShard.shard(state, "noisy")
      quiet_shard = ShuffleShard.shard(state, "quiet")

      # Deterministic: computed again from scratch, and equal to known values.
      assert noisy_shard == ShuffleShard.shard(ShuffleShard.init(@shards), "noisy")
      assert {noisy_shard, quiet_shard} == {[4, 5], [5, 7]}

      # The hardest case the criterion allows: exactly one member in common.
      assert length(noisy_shard -- quiet_shard) == 1

      {:ok, _noisy} = Fairway.enqueue_all(jobs("noisy", 200, Echo, %{sleep_ms: 10}))
      for _n <- 1..2, do: assert_receive({:started, "noisy", _id, _pid, _running}, 5_000)

      {:ok, _quiet} = Fairway.enqueue_all(jobs("quiet", 10, Echo, %{sleep_ms: 10}))
      await_stops("quiet", :completed, 10)

      waiting = Fairway.counts("q")[{"noisy", :available}]
      assert waiting >= 100

      # Every start so far, and every start from here to the end of the noisy
      # backlog, is inside the tenant's own shard.
      await_stops("noisy", :completed, 200, 10_000)
      starts = starts(210)

      assert starts |> Enum.filter(&(elem(&1, 0) == "noisy")) |> length() == 200

      for {tenant, _measurements, %{slot: slot}} <- starts do
        assert slot in ShuffleShard.shard(state, tenant)
      end

      # The slot they share went to whichever had the older job: the noisy one.
      assert [7] == for({"quiet", _, %{slot: slot}} <- starts, uniq: true, do: slot)
      assert [4, 5] == Enum.sort(for({"noisy", _, %{slot: slot}} <- starts, uniq: true, do: slot))
    end
  end

  describe "interruptible iteration" do
    @slice_ms 100
    @steps 40

    setup do
      start_fairway!("""
      queues:
        q:
          mode: interruptible
          concurrency: 2
          interruptible: {slice_ms: #{@slice_ms}}
      """)

      :ok
    end

    test "with every slot held by long iterable noisy jobs, a quiet job starts within 5 slices; " <>
           "interrupted jobs resume from their cursor and run every step exactly once" do
      # Four jobs of 40 steps x 10 ms: 400 ms each, four slices each, and twice
      # as many of them as there are slots.
      {:ok, noisy} = Fairway.enqueue_all(jobs("noisy", 4, Stepper, %{steps: @steps, step_ms: 10}))
      assert [{"noisy", _, _}, {"noisy", _, _}] = starts(2)

      enqueued_at = System.monotonic_time(:millisecond)
      {:ok, quiet} = Fairway.enqueue(hd(jobs("quiet", 1, Stepper, %{steps: 3, step_ms: 1})))

      # The next start is the quiet job's, although two noisy jobs that have
      # not run at all were enqueued before it.
      assert [{"quiet", %{at: started_at}, %{job: %{id: quiet_id}}}] = starts(1)
      assert quiet_id == quiet.id
      assert started_at - enqueued_at <= 5 * @slice_ms

      await_stops("quiet", :completed, 1)
      await_stops("noisy", :completed, 4, 10_000)

      steps = for {:step, id, step, attempt} <- drain(), do: {id, step, attempt}

      for job <- noisy do
        ran = for {id, step, attempt} <- steps, id == job.id, do: {step, attempt}

        # Every step exactly once, in order.
        assert Enum.map(ran, &elem(&1, 0)) == Enum.to_list(0..(@steps - 1))

        # It was interrupted: the steps are spread over several executions,
        # and each execution took up where the one before it stopped.
        executions = ran |> Enum.map(&elem(&1, 1)) |> Enum.uniq()
        assert length(executions) >= 3
        assert executions == Enum.sort(executions)

        assert {:ok,
                %Fairway.Job{state: :completed, failures: 0, attempt: attempt, cursor: cursor}} =
                 Fairway.fetch(job.id)

        # The cursor in the store is the one saved at the last interruption.
        assert attempt >= List.last(executions)
        assert %{"step" => saved} = cursor
        assert saved in 1..@steps
      end
    end
  end

  # Everything already in the mailbox.
  defp drain do
    receive do
      message -> [message | drain()]
    after
      0 -> []
    end
  end
end
