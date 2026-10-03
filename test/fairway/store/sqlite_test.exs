defmodule Fairway.Store.SQLiteTest do
  use ExUnit.Case, async: true

  # Tags only reach tests defined after them, and the contract defines tests.
  @moduletag :tmp_dir

  use Fairway.StoreContract

  alias Fairway.Store.SQLite
  alias Fairway.StoreContract

  setup %{tmp_dir: tmp_dir} do
    path = Path.join(tmp_dir, "jobs.db")
    %{store: {SQLite, start_supervised!({SQLite, path: path, name: nil})}, path: path}
  end

  describe "a file shared by several connections" do
    # This is what nodes on one host do, and what the cluster tests rely on.
    test "never hands the same job to two of them", %{store: first, path: path} do
      others =
        for n <- 2..3 do
          spec = Supervisor.child_spec({SQLite, path: path, name: nil}, id: {SQLite, n})
          {SQLite, start_supervised!(spec)}
        end

      jobs = insert(first, List.duplicate([tenant: "a"], 90))

      claimed =
        [first | others]
        |> Enum.map(fn store -> Task.async(fn -> StoreContract.claim_all(store, "a", 0) end) end)
        |> Task.await_many(30_000)

      # Between them, every job exactly once.
      assert claimed |> List.flatten() |> Enum.sort() == Enum.map(jobs, & &1.id)
    end

    test "shows each of them what the others wrote", %{store: first, path: path} do
      spec = Supervisor.child_spec({SQLite, path: path, name: nil}, id: {SQLite, 2})
      second = {SQLite, start_supervised!(spec)}

      [job] = insert(first, [[tenant: "a"]])
      claimed = claim!(second, "a")

      assert Store.running(first, "q") == [claimed]
      assert Store.complete(first, job.id, claimed.attempt, 5) == :ok
      assert {:ok, %Job{state: :completed}} = Store.fetch(second, job.id)
    end
  end

  test "jobs outlive the process that wrote them", %{store: store, path: path} do
    [job] = insert(store, [[tenant: "a", args: %{"keep" => "me"}]])
    stop_supervised!(SQLite)

    reopened = {SQLite, start_supervised!({SQLite, path: path, name: nil})}

    assert Store.fetch(reopened, job.id) == {:ok, job}
    assert [later] = insert(reopened, [[tenant: "a"]])
    assert later.id > job.id
  end

  test "does not start on a path it cannot open", %{tmp_dir: tmp_dir} do
    path = Path.join([tmp_dir, "missing", "jobs.db"])
    spec = Supervisor.child_spec({SQLite, path: path, name: nil}, id: :unopenable)

    assert {:error, {{:sqlite, ^path, _reason}, _child}} = start_supervised(spec)
  end
end
