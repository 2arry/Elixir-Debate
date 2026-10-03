defmodule Fairway.FairnessTest do
  # The modes are pure functions, so these tests call them directly. What they
  # do inside a running queue is covered by Fairway.FairnessE2ETest.
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Fairway.Fairness
  alias Fairway.Fairness.{FIFO, Interruptible, RoundRobin, ShuffleShard, Throttle}

  doctest Fairness
  doctest ShuffleShard

  defp view(attrs) do
    Map.merge(%{ready: [], running: %{}, free: [], now: 0}, Map.new(attrs))
  end

  defp tenant, do: string(?a..?h, min_length: 1, max_length: 2)
  defp tenants, do: uniq_list_of(tenant(), min_length: 1, max_length: 8)

  describe "modes" do
    test "every mode names a module that implements the behaviour" do
      assert Enum.sort(Fairness.modes()) ==
               [:fifo, :interruptible, :per_tenant, :shuffle_shard, :throttle]

      for mode <- Fairness.modes() do
        behaviours = mode |> Fairness.module() |> then(& &1.module_info(:attributes)[:behaviour])
        assert Fairness in behaviours
      end
    end
  end

  describe "ring/2" do
    property "is the sorted tenants, rotated to start after the last one served" do
      check all(tenants <- tenants(), last <- one_of([constant(nil), tenant()])) do
        ring = Fairness.ring(tenants, last)
        {behind, ahead} = Enum.split_with(ring, &(last != nil and &1 <= last))

        assert Enum.sort(ring) == Enum.sort(tenants)
        assert ring == Enum.sort(ahead) ++ Enum.sort(behind)
      end
    end
  end

  describe "FIFO" do
    test "starts the tenant with the oldest ready job in the first free slot" do
      assert FIFO.select(FIFO.init(%{}), view(ready: ["b", "a"], free: [2, 0])) == {:run, "b", 2}
    end

    test "is idle with nothing ready or nothing free" do
      assert FIFO.select(nil, view(ready: [], free: [0])) == :idle
      assert FIFO.select(nil, view(ready: ["a"], free: [])) == :idle
    end

    test "keeps choosing the same tenant for as long as its jobs are oldest" do
      picks = serve(FIFO, FIFO.init(%{}), fn _n -> ["noisy", "quiet"] end, 5)
      assert picks == List.duplicate("noisy", 5)
    end
  end

  describe "RoundRobin" do
    test "serves waiting tenants in turn, whatever the age of their jobs" do
      picks = serve(RoundRobin, RoundRobin.init(%{}), fn _n -> ["noisy", "quiet", "other"] end, 6)
      assert picks == ["noisy", "other", "quiet", "noisy", "other", "quiet"]
    end

    test "skips a tenant with nothing ready and does not owe it a turn" do
      ready = fn
        n when n in [1, 2] -> ["a", "c"]
        _n -> ["a", "b", "c"]
      end

      assert serve(RoundRobin, RoundRobin.init(%{}), ready, 6) == ["a", "c", "a", "b", "c", "a"]
    end

    test "is idle with nothing ready or nothing free" do
      assert RoundRobin.select(nil, view(ready: [], free: [0])) == :idle
      assert RoundRobin.select(nil, view(ready: ["a"], free: [])) == :idle
    end

    property "every waiting tenant is served once in any run of as many starts as there are tenants" do
      check all(tenants <- tenants(), rounds <- integer(1..4), offset <- integer(0..7)) do
        count = length(tenants)

        picks =
          serve(RoundRobin, RoundRobin.init(%{}), fn _n -> tenants end, count * rounds + offset)

        for window <- Enum.chunk_every(Enum.drop(picks, offset), count, 1, :discard) do
          assert Enum.sort(window) == Enum.sort(tenants)
        end
      end
    end

    property "a tenant that joins late waits for at most one start by each other tenant" do
      check all(
              everyone <- uniq_list_of(tenant(), min_length: 2, max_length: 8),
              late <- member_of(everyone),
              before <- integer(0..20)
            ) do
        tenants = everyone -- [late]
        ready = fn n -> if n > before, do: everyone, else: tenants end
        picks = serve(RoundRobin, RoundRobin.init(%{}), ready, before + length(tenants) + 1)

        waited = picks |> Enum.drop(before) |> Enum.find_index(&(&1 == late))
        assert waited <= length(tenants)
      end
    end
  end

  describe "Interruptible" do
    test "assigns slots exactly as RoundRobin does" do
      ready = fn _n -> ["noisy", "quiet"] end

      assert serve(Interruptible, Interruptible.init(%{}), ready, 4) ==
               serve(RoundRobin, RoundRobin.init(%{}), ready, 4)
    end
  end

  describe "Throttle" do
    setup do
      %{state: Throttle.init(%{max_concurrency: 2, rate: 10, burst: 3})}
    end

    test "does not start a tenant that is at its concurrency cap", %{state: state} do
      busy = %{0 => "a", 1 => "a"}

      assert Throttle.select(state, view(ready: ["a"], running: busy, free: [2])) == :idle

      assert Throttle.select(state, view(ready: ["a", "b"], running: busy, free: [2])) ==
               {:run, "b", 2}

      assert Throttle.select(state, view(ready: ["a"], running: %{0 => "a"}, free: [2])) ==
               {:run, "a", 2}
    end

    test "lets a tenant start `burst` jobs at once and then makes it wait", %{state: state} do
      state = Enum.reduce(1..3, state, fn _n, state -> start!(state, "a", 1_000) end)

      assert Throttle.select(state, view(ready: ["a"], free: [0], now: 1_000)) == {:wait, 100}
    end

    test "the wait is exactly as long as the next token takes", %{state: state} do
      state = Enum.reduce(1..3, state, fn _n, state -> start!(state, "a", 1_000) end)

      assert Throttle.select(state, view(ready: ["a"], free: [0], now: 1_040)) == {:wait, 60}
      assert Throttle.select(state, view(ready: ["a"], free: [0], now: 1_099)) == {:wait, 1}
      assert Throttle.select(state, view(ready: ["a"], free: [0], now: 1_100)) == {:run, "a", 0}
    end

    test "one tenant's empty bucket does not hold back another", %{state: state} do
      state = Enum.reduce(1..3, state, fn _n, state -> start!(state, "a", 1_000) end)

      assert Throttle.select(state, view(ready: ["a", "b"], free: [0], now: 1_000)) ==
               {:run, "b", 0}
    end

    test "a bucket never holds more than `burst`, however long the tenant was idle", %{
      state: state
    } do
      state = start!(state, "a", 0)
      state = Enum.reduce(1..3, state, fn _n, state -> start!(state, "a", 3_600_000) end)

      assert Throttle.select(state, view(ready: ["a"], free: [0], now: 3_600_000)) == {:wait, 100}
    end

    test "forgets a tenant once its bucket is full again", %{state: state} do
      state = start!(state, "a", 0)
      assert Map.keys(state.buckets) == ["a"]

      state = start!(state, "b", 100)
      assert Map.keys(state.buckets) == ["b"]
    end

    test "takes turns among the tenants that are allowed to start" do
      generous = Throttle.init(%{max_concurrency: 10, rate: 1_000, burst: 1_000})

      assert serve(Throttle, generous, fn _n -> ["b", "a"] end, 4) == ["a", "b", "a", "b"]
    end

    test "honours fractional rates" do
      state = %{max_concurrency: 1, rate: 0.5, burst: 1} |> Throttle.init() |> start!("a", 0)

      assert Throttle.select(state, view(ready: ["a"], free: [0], now: 0)) == {:wait, 2_000}
      assert Throttle.select(state, view(ready: ["a"], free: [0], now: 2_000)) == {:run, "a", 0}
    end

    property "never starts more than burst + rate * w jobs of a tenant in any window of w seconds" do
      check all(
              rate <- integer(1..50),
              burst <- integer(1..10),
              # Times at which the scheduler asks, as gaps in milliseconds.
              gaps <- list_of(integer(0..40), min_length: 50, max_length: 400),
              window_ms <- member_of([100, 500, 1_000, 2_500])
            ) do
        state = Throttle.init(%{max_concurrency: 1_000, rate: rate, burst: burst})
        times = Enum.scan(gaps, &+/2)

        {starts, _state} =
          Enum.flat_map_reduce(times, state, fn now, state ->
            case Throttle.select(state, view(ready: ["a"], free: [0], now: now)) do
              {:run, "a", 0} -> {[now], Throttle.started(state, "a", 0, now)}
              {:wait, ms} when ms > 0 -> {[], state}
            end
          end)

        allowed = burst + rate * window_ms / 1_000

        for from <- starts do
          in_window = Enum.count(starts, &(&1 >= from and &1 < from + window_ms))
          assert in_window <= allowed
        end
      end
    end

    property "after the wait it asks for, the tenant may start" do
      check all(rate <- integer(1..50), burst <- integer(1..5), elapsed <- integer(0..200)) do
        state = Throttle.init(%{max_concurrency: 1_000, rate: rate, burst: burst})
        state = Enum.reduce(1..burst, state, fn _n, state -> start!(state, "a", 0) end)

        case Throttle.select(state, view(ready: ["a"], free: [0], now: elapsed)) do
          {:run, "a", 0} ->
            :ok

          {:wait, ms} ->
            assert Throttle.select(state, view(ready: ["a"], free: [0], now: elapsed + ms)) ==
                     {:run, "a", 0}

            assert {:wait, 1} =
                     Throttle.select(state, view(ready: ["a"], free: [0], now: elapsed + ms - 1))
        end
      end
    end
  end

  describe "ShuffleShard" do
    setup do
      %{state: ShuffleShard.init(%{slots: 8, shard_size: 2, seed: 0})}
    end

    # These values are part of the contract: if they change, every tenant of
    # every deployment moves to different slots on upgrade.
    test "assignments are pinned", %{state: state} do
      assert ShuffleShard.shard(state, "acme") == [1, 6]
      assert ShuffleShard.shard(state, "globex") == [1, 3]
      assert ShuffleShard.shard(state, "noisy") == [4, 5]
      assert ShuffleShard.shard(state, "quiet") == [5, 7]

      wide = ShuffleShard.init(%{slots: 20, shard_size: 4, seed: 7})
      assert ShuffleShard.shard(wide, "acme") == [0, 15, 17, 18]
    end

    test "the seed reshuffles every tenant", %{state: state} do
      reseeded = ShuffleShard.init(%{slots: 8, shard_size: 2, seed: 1})
      tenants = for n <- 1..50, do: "tenant-#{n}"

      moved =
        Enum.count(tenants, &(ShuffleShard.shard(state, &1) != ShuffleShard.shard(reseeded, &1)))

      assert moved > 40
    end

    test "starts a tenant only in a free slot of its shard", %{state: state} do
      assert ShuffleShard.select(state, view(ready: ["noisy"], free: [0, 1, 2, 3, 6, 7])) == :idle

      assert ShuffleShard.select(state, view(ready: ["noisy"], free: [0, 5])) ==
               {:run, "noisy", 5}

      assert ShuffleShard.select(state, view(ready: ["noisy"], free: [4, 5])) ==
               {:run, "noisy", 4}
    end

    test "a tenant whose shard is full does not block the tenants behind it", %{state: state} do
      assert ShuffleShard.select(state, view(ready: ["noisy", "quiet"], free: [7])) ==
               {:run, "quiet", 7}
    end

    test "in a slot two tenants share, the one with the older job goes first", %{state: state} do
      assert ShuffleShard.select(state, view(ready: ["noisy", "quiet"], free: [5])) ==
               {:run, "noisy", 5}

      assert ShuffleShard.select(state, view(ready: ["quiet", "noisy"], free: [5])) ==
               {:run, "quiet", 5}
    end

    test "spreads tenants evenly over the slots and over the possible shards", %{state: state} do
      shards = for n <- 1..2_800, do: ShuffleShard.shard(state, "tenant-#{n}")

      # 2800 tenants x 2 slots over 8 slots is 700 a slot, and 28 possible
      # shards is 100 tenants a shard. The inputs are fixed, so this is exact
      # arithmetic on a hash, not a statistical test that could flake.
      for {_slot, tenants} <- shards |> List.flatten() |> Enum.frequencies() do
        assert_in_delta tenants, 700, 100
      end

      by_shard = Enum.frequencies(shards)
      assert map_size(by_shard) == 28
      assert by_shard |> Map.values() |> Enum.max() < 140
    end

    property "a shard is `shard_size` distinct slots of the queue, the same every time" do
      check all(
              slots <- integer(1..40),
              shard_size <- integer(1..slots),
              seed <- integer(),
              tenant <- string(:printable, min_length: 1)
            ) do
        state = ShuffleShard.init(%{slots: slots, shard_size: shard_size, seed: seed})
        shard = ShuffleShard.shard(state, tenant)

        assert length(shard) == shard_size
        assert shard == Enum.uniq(shard)
        assert shard == Enum.sort(shard)
        assert Enum.all?(shard, &(&1 in 0..(slots - 1)))

        assert shard ==
                 ShuffleShard.shard(
                   ShuffleShard.init(%{slots: slots, shard_size: shard_size, seed: seed}),
                   tenant
                 )
      end
    end

    property "adding a slot moves a tenant onto the new slot or not at all" do
      check all(slots <- integer(2..40), shard_size <- integer(1..slots), tenant <- tenant()) do
        before =
          ShuffleShard.shard(
            ShuffleShard.init(%{slots: slots, shard_size: shard_size, seed: 0}),
            tenant
          )

        grown =
          ShuffleShard.shard(
            ShuffleShard.init(%{slots: slots + 1, shard_size: shard_size, seed: 0}),
            tenant
          )

        assert (grown -- before) in [[], [slots]]
      end
    end

    property "never starts a tenant outside its shard" do
      check all(ready <- tenants(), mask <- list_of(boolean(), length: 8)) do
        state = ShuffleShard.init(%{slots: 8, shard_size: 2, seed: 0})
        free = for {true, slot} <- Enum.with_index(mask), do: slot

        case ShuffleShard.select(state, view(ready: ready, free: free)) do
          {:run, tenant, slot} ->
            assert slot in free
            assert slot in ShuffleShard.shard(state, tenant)
            # Nobody ahead of the chosen tenant could have been started.
            ahead = Enum.take_while(ready, &(&1 != tenant))

            assert Enum.all?(
                     ahead,
                     &(ShuffleShard.shard(state, &1) -- free == ShuffleShard.shard(state, &1))
                   )

          :idle ->
            assert Enum.all?(
                     ready,
                     &(ShuffleShard.shard(state, &1) -- free == ShuffleShard.shard(state, &1))
                   )
        end
      end
    end
  end

  # Runs `count` starts against a mode with one slot that is always free.
  # `ready` is given the number of the start and answers who is waiting.
  defp serve(mode, state, ready, count) do
    {picks, _state} =
      Enum.map_reduce(1..count//1, state, fn n, state ->
        {:run, tenant, 0} = mode.select(state, view(ready: ready.(n), free: [0], now: n))
        {tenant, mode.started(state, tenant, 0, n)}
      end)

    picks
  end

  defp start!(state, tenant, now) do
    assert {:run, ^tenant, 0} = Throttle.select(state, view(ready: [tenant], free: [0], now: now))
    Throttle.started(state, tenant, 0, now)
  end
end
