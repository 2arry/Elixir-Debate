defmodule Fairway.ClusterTest do
  # Acceptance criterion C. Every test starts its own three nodes: separate OS
  # processes that discover each other through libcluster and share a store.
  # See Fairway.Test.Cluster for how they are started, watched and killed.
  use ExUnit.Case, async: false

  import Fairway.Test.Case, only: [eventually: 2]

  alias Fairway.Job
  alias Fairway.Test.Cluster

  @moduletag :tmp_dir
  @moduletag timeout: 180_000

  defp start_cluster(%{tmp_dir: tmp_dir}, opts \\ []) do
    cluster = Cluster.start!(tmp_dir, opts)
    on_exit(fn -> Cluster.stop(cluster) end)
    cluster
  end

  defp nodes(cluster), do: cluster.members |> Enum.map(& &1.node) |> Enum.sort()

  defp events(cluster, event),
    do: cluster |> Cluster.journal() |> Enum.filter(&(&1.event == event))

  defp fetch!(cluster, job) do
    {:ok, saved} = Cluster.call(hd(cluster.members), Fairway, :fetch, [job.id])
    saved
  end

  # Jobs that had started on `node` and not finished there, as the journal has it.
  defp in_flight_on(cluster, node) do
    journal = Cluster.journal(cluster)
    finished = for %{event: :finish, node: ^node, id: id} <- journal, do: id
    for %{event: :start, node: ^node, id: id} <- journal, id not in finished, uniq: true, do: id
  end

  test "jobs run on more than one node", context do
    cluster = start_cluster(context)

    jobs = Cluster.enqueue!(cluster, "q", [{"acme", 30, 50}])
    eventually(fn -> Cluster.count(cluster, "q", :completed) == 30 end, 30_000)

    ran_on = cluster |> events(:start) |> Enum.map(& &1.node) |> Enum.uniq() |> Enum.sort()
    assert length(ran_on) > 1

    # More than that: six slots over three nodes puts work on every one of them.
    assert ran_on == nodes(cluster)

    # And each job ran exactly once.
    assert cluster |> events(:start) |> Enum.map(& &1.id) |> Enum.sort() ==
             Enum.map(jobs, & &1.id)

    assert cluster |> events(:finish) |> Enum.map(& &1.id) |> Enum.sort() ==
             Enum.map(jobs, & &1.id)
  end

  test "there is one scheduler per queue, cluster-wide", context do
    cluster = start_cluster(context)

    assert_one_leader = fn queue ->
      assert Enum.sort(Cluster.roles(cluster, queue)) == [:leader, :standby, :standby]

      # Every node names the same leader, and it is the node that says it leads.
      assert %{node: leader} = Cluster.leader(cluster, queue)

      assert [{^leader, :leader}] =
               cluster |> nodes_with_roles(queue) |> Enum.filter(&(elem(&1, 1) == :leader))
    end

    Enum.each(["q", "other"], assert_one_leader)

    # It is still so after both queues have done work, and one scheduler means
    # no job was dispatched twice.
    jobs =
      Cluster.enqueue!(cluster, "q", [{"acme", 24, 20}]) ++
        Cluster.enqueue!(cluster, "other", [{"acme", 12, 20}])

    eventually(
      fn ->
        Cluster.count(cluster, "q", :completed) == 24 and
          Cluster.count(cluster, "other", :completed) == 12
      end,
      30_000
    )

    Enum.each(["q", "other"], assert_one_leader)

    assert cluster |> events(:start) |> Enum.map(& &1.id) |> Enum.sort() ==
             jobs |> Enum.map(& &1.id) |> Enum.sort()
  end

  defp nodes_with_roles(cluster, queue) do
    Enum.zip(Enum.map(cluster.members, & &1.node), Cluster.roles(cluster, queue))
  end

  test "killing the scheduler's node loses no jobs", context do
    context |> start_cluster() |> lose_the_scheduler()
  end

  @tag :postgres
  test "killing the scheduler's node loses no jobs, on PostgreSQL", context do
    url = System.fetch_env!("FAIRWAY_PG_URL")
    start_supervised!({Fairway.Store.Postgres, url: url, name: __MODULE__.Pool})
    Postgrex.query!(__MODULE__.Pool, "TRUNCATE fairway_jobs RESTART IDENTITY", [])

    context
    |> start_cluster(store: "store: {adapter: postgres, url: \"#{url}\"}")
    |> lose_the_scheduler()
  end

  defp lose_the_scheduler(cluster) do
    # Six long jobs fill the six slots, two on each node, so the node that is
    # killed is certainly running two of them. Then, behind them, a backlog
    # that only a scheduler can start.
    long = Cluster.enqueue!(cluster, "q", [{"acme", 3, 1_500}, {"zen", 3, 1_500}])
    eventually(fn -> length(events(cluster, :start)) == 6 end, 30_000)

    short = Cluster.enqueue!(cluster, "q", [{"acme", 27, 50}, {"zen", 27, 50}])
    ids = (long ++ short) |> Enum.map(& &1.id) |> Enum.sort()

    scheduler = Cluster.leader(cluster, "q")
    assert scheduler != nil

    survivors = Cluster.kill(cluster, scheduler)
    assert length(survivors.members) == 2

    interrupted = in_flight_on(cluster, scheduler.node)
    assert length(interrupted) == 2
    assert Enum.all?(interrupted, &(&1 in Enum.map(long, fn job -> job.id end)))

    # No job is lost: all sixty complete, so none is discarded or left behind.
    eventually(fn -> Cluster.count(survivors, "q", :completed) == 60 end, 60_000)

    assert Cluster.call(hd(survivors.members), Fairway, :counts, ["q"]) ==
             %{{"acme", :completed} => 30, {"zen", :completed} => 30}

    assert cluster |> events(:finish) |> Enum.map(& &1.id) |> Enum.uniq() |> Enum.sort() == ids

    # A surviving node took over the scheduling.
    eventually(fn -> Cluster.leader(survivors, "q") != nil end, 30_000)
    assert Cluster.leader(survivors, "q").node in nodes(survivors)
    assert Enum.sort(Cluster.roles(survivors, "q")) == [:leader, :standby]

    # The jobs the dead node was running were run again by a survivor.
    for id <- interrupted do
      reruns = for %{event: :finish, id: ^id, node: node} <- Cluster.journal(cluster), do: node
      assert reruns != []
      assert Enum.all?(reruns, &(&1 in nodes(survivors)))
    end

    # Work that was not on the dead node was not repeated.
    untouched = ids -- interrupted
    starts = cluster |> events(:start) |> Enum.map(& &1.id) |> Enum.frequencies()
    assert Enum.all?(untouched, &(starts[&1] == 1))
  end

  test "killing a worker node re-runs its in-flight jobs elsewhere", context do
    cluster = start_cluster(context)

    # Six slots, six jobs long enough to be running when the node dies: two on
    # each node.
    jobs = Cluster.enqueue!(cluster, "q", [{"acme", 6, 1_500}])
    eventually(fn -> length(events(cluster, :start)) == 6 end, 30_000)

    scheduler = Cluster.leader(cluster, "q")
    worker = Enum.find(cluster.members, &(&1 != scheduler))
    lost = in_flight_on(cluster, worker.node)
    assert length(lost) == 2

    survivors = Cluster.kill(cluster, worker)
    eventually(fn -> Cluster.count(survivors, "q", :completed) == 6 end, 60_000)

    for job <- jobs, job.id in lost do
      # Started on the node that died, started again on one that did not, and
      # finished there.
      assert [first, second] =
               for(
                 %{event: :start, id: id} = event <- Cluster.journal(cluster),
                 id == job.id,
                 do: event
               )

      assert {first.node, first.attempt} == {worker.node, 1}
      assert second.node in nodes(survivors)
      assert second.attempt == 2

      assert [%{node: finished_on, attempt: 2}] =
               for(
                 %{event: :finish, id: id} = event <- Cluster.journal(cluster),
                 id == job.id,
                 do: event
               )

      assert finished_on == second.node

      assert %Job{state: :completed, attempt: 2, failures: 1, last_error: "lost: " <> _why} =
               fetch!(survivors, job)
    end

    # The four jobs on the other nodes were left alone.
    for job <- jobs, job.id not in lost do
      assert %Job{state: :completed, attempt: 1, failures: 0} = fetch!(survivors, job)
    end

    # The scheduler did not move.
    assert Cluster.leader(survivors, "q") == scheduler
  end
end
