defmodule Fairway.Store.PostgresTest do
  # Runs when FAIRWAY_PG_URL is set; see test_helper.exs.
  use ExUnit.Case, async: false

  # Tags only reach tests defined after them, and the contract defines tests.
  @moduletag :postgres

  use Fairway.StoreContract

  alias Fairway.Store.Postgres

  setup do
    url = System.fetch_env!("FAIRWAY_PG_URL")
    start_supervised!({Postgres, url: url, name: __MODULE__.Pool, pool_size: 12})
    Postgrex.query!(__MODULE__.Pool, "TRUNCATE fairway_jobs RESTART IDENTITY", [])

    %{store: {Postgres, __MODULE__.Pool}, url: url}
  end

  test "creating the table again is harmless", %{store: store} do
    [job] = insert(store, [[tenant: "a"]])

    assert Postgres.setup(__MODULE__.Pool) == :ignore
    assert Store.fetch(store, job.id) == {:ok, job}
  end

  test "stores args and the cursor as jsonb", %{store: store} do
    [job] = insert(store, [[tenant: "a", args: %{"account" => %{"plan" => "pro"}}]])
    claimed = claim!(store, "a")
    :ok = Store.yield(store, job.id, claimed.attempt, %{"offset" => 7})

    assert %Postgrex.Result{rows: [["pro", 7]]} =
             Postgrex.query!(
               __MODULE__.Pool,
               "SELECT args #>> '{account,plan}', (progress ->> 'offset')::int FROM fairway_jobs WHERE id = $1",
               [job.id]
             )
  end

  test "reports a database it cannot reach instead of hanging" do
    opts = [
      url: "postgres://nobody:nothing@127.0.0.1:1/none",
      name: __MODULE__.Unreachable,
      setup_timeout: 500
    ]

    spec = Supervisor.child_spec({Postgres, opts}, id: :unreachable)

    assert {:error,
            {{:shutdown, {:failed_to_start_child, :setup, {:postgres_unavailable, message}}},
             _child}} =
             start_supervised(spec)

    assert is_binary(message)
  end
end
