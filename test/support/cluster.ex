defmodule Fairway.Test.Cluster do
  @moduledoc false
  # Three real Erlang nodes, each a separate OS process running Fairway.
  #
  # The nodes are controlled over stdio (`connection: :standard_io`), so the
  # test node is not distributed and is not part of the cluster. The nodes
  # find each other the way production nodes would: through the `cluster`
  # section of the YAML file they are all started with. Tests observe them by
  # calling into them and by reading the journal that the jobs write.

  import Fairway.Test.Case, only: [eventually: 2]

  alias Fairway.Queue.{Runner, Scheduler}

  @call_timeout 30_000

  @type member :: %{node: node(), peer: pid(), os_pid: String.t()}
  @type t :: %{members: [member()], journal: Path.t()}

  @doc """
  Starts three nodes sharing `store` (a YAML `store:` section; by default a
  SQLite file in `dir`) and waits until they agree on who schedules what.
  """
  @spec start!(Path.t(), keyword()) :: t()
  def start!(dir, opts \\ []) do
    tag = "#{System.pid()}_#{System.unique_integer([:positive])}"
    names = for n <- 1..3, do: "fairway_#{tag}_#{n}"
    hosts = Enum.map_join(names, ", ", &~s("#{&1}@127.0.0.1"))
    # The path is quoted: test directories are named after tests, commas and all.
    store =
      opts[:store] || "store: {adapter: sqlite, path: #{inspect(Path.join(dir, "jobs.db"))}}"

    config = Path.join(dir, "fairway.yml")

    File.write!(config, """
    #{store}
    cluster:
      strategy: epmd
      hosts: [#{hosts}]
    queues:
      q:
        mode: per_tenant
        concurrency: 6
        poll_interval_ms: 100
        orphan_grace_ms: 2000
      other:
        mode: fifo
        concurrency: 3
        poll_interval_ms: 100
        orphan_grace_ms: 2000
    """)

    cluster = %{
      members: Enum.map(names, &start_node!(&1, config)),
      journal: Path.join(dir, "journal.log")
    }

    File.write!(cluster.journal, "")
    await_settled(cluster)
    cluster
  end

  defp start_node!(name, config) do
    {:ok, peer, node} =
      :peer.start(%{
        name: String.to_charlist(name),
        host: ~c"127.0.0.1",
        longnames: true,
        connection: :standard_io,
        # Quiet, so that three nodes starting and stopping do not bury the
        # test output. Errors still print.
        args: [~c"-setcookie", ~c"fairway_test", ~c"-kernel", ~c"logger_level", ~c"error"]
      })

    member = %{node: node, peer: peer, os_pid: nil}
    :ok = call(member, :code, :add_paths, [:code.get_path()])
    :ok = call(member, :application, :set_env, [:logger, :level, :error])
    :ok = call(member, :application, :set_env, [:fairway, :config_file, config])
    {:ok, _started} = call(member, :application, :ensure_all_started, [:fairway])

    %{member | os_pid: member |> call(:os, :getpid, []) |> List.to_string()}
  end

  @doc "Stops every node that is still up."
  @spec stop(t()) :: :ok
  def stop(cluster) do
    Enum.each(cluster.members, fn member ->
      try do
        :peer.stop(member.peer)
      catch
        :exit, _already_gone -> :ok
      end
    end)
  end

  @doc """
  Kills a node's OS process outright, and forgets it. Returns once the process
  is gone, so nothing it was doing can still reach the journal.
  """
  @spec kill(t(), member()) :: t()
  def kill(cluster, member) do
    control = Process.monitor(member.peer)
    {_output, 0} = System.cmd("kill", ["-9", member.os_pid])

    receive do
      {:DOWN, ^control, :process, _peer, _reason} -> :ok
    after
      @call_timeout -> raise "#{member.node} survived kill -9"
    end

    %{cluster | members: List.delete(cluster.members, member)}
  end

  @doc "Calls a function on a node."
  @spec call(member(), module(), atom(), [term()]) :: term()
  def call(member, module, function, args) do
    :peer.call(member.peer, module, function, args, @call_timeout)
  end

  @doc "The member whose scheduler leads `queue`, as every node sees it."
  @spec leader(t(), String.t()) :: member() | nil
  def leader(cluster, queue) do
    case cluster.members |> Enum.map(&call(&1, Fairway, :leader, [queue])) |> Enum.uniq() do
      [node] -> Enum.find(cluster.members, &(&1.node == node))
      _disagreement -> nil
    end
  end

  @doc "Each member's role for `queue`."
  @spec roles(t(), String.t()) :: [:leader | :standby | :unavailable]
  def roles(cluster, queue) do
    Enum.map(cluster.members, fn member ->
      try do
        call(member, Scheduler, :role, [queue])
      catch
        # The scheduler is between a step-down and its restart.
        _kind, _reason -> :unavailable
      end
    end)
  end

  @doc """
  Waits until the cluster is whole: every node sees every other, each queue
  has exactly one leader that all nodes agree on, and that leader's node sees
  every node's runner.
  """
  @spec await_settled(t()) :: true
  def await_settled(cluster) do
    size = length(cluster.members)

    eventually(
      fn ->
        Enum.all?(cluster.members, &(length(call(&1, Node, :list, [])) == size - 1)) and
          Enum.all?(["q", "other"], &settled?(cluster, &1, size))
      end,
      @call_timeout
    )
  end

  defp settled?(cluster, queue, size) do
    leader = leader(cluster, queue)

    leader != nil and
      Enum.count(roles(cluster, queue), &(&1 == :leader)) == 1 and
      Enum.count(roles(cluster, queue), &(&1 == :standby)) == size - 1 and
      Enum.all?(cluster.members, fn member ->
        length(call(member, :pg, :get_members, [Fairway.PG, Runner.group(queue)])) == size
      end)
  end

  @doc """
  Enqueues journal jobs on `queue` through the first node, in one batch, so
  that the scheduler sees all of them or none. `batch` is a list of
  `{tenant, count, sleep_ms}`.
  """
  @spec enqueue!(t(), String.t(), [{String.t(), pos_integer(), non_neg_integer()}]) :: [
          Fairway.Job.t()
        ]
  def enqueue!(cluster, queue, batch) do
    entries =
      Enum.flat_map(batch, fn {tenant, count, sleep_ms} ->
        args = %{journal: cluster.journal, sleep_ms: sleep_ms}
        entry = %{queue: queue, tenant: tenant, worker: Fairway.Test.Workers.Journal, args: args}
        List.duplicate(entry, count)
      end)

    {:ok, jobs} = call(hd(cluster.members), Fairway, :enqueue_all, [entries])
    jobs
  end

  @doc "How many jobs of `queue` are in `state`, summed over tenants."
  @spec count(t(), String.t(), Fairway.Job.state()) :: non_neg_integer()
  def count(cluster, queue, state) do
    counts = call(hd(cluster.members), Fairway, :counts, [queue])

    for {{_tenant, ^state}, count} <- counts, reduce: 0 do
      sum -> sum + count
    end
  end

  @doc "The journal so far, as `%{event, id, attempt, node}` in the order written."
  @spec journal(t()) :: [
          %{event: :start | :finish, id: pos_integer(), attempt: pos_integer(), node: node()}
        ]
  def journal(cluster) do
    cluster.journal
    |> File.read!()
    |> String.split("\n", trim: true)
    |> Enum.map(fn line ->
      [event, id, attempt, node] = String.split(line, " ")

      %{
        event: event(event),
        id: String.to_integer(id),
        attempt: String.to_integer(attempt),
        node: String.to_atom(node)
      }
    end)
  end

  defp event("start"), do: :start
  defp event("finish"), do: :finish
end
