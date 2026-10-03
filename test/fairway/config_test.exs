defmodule Fairway.ConfigTest do
  use ExUnit.Case, async: true

  alias Fairway.Config
  alias Fairway.Config.{Error, Queue}

  doctest Config

  # The smallest valid document, for tests that break one thing in it.
  @valid """
  queues:
    emails:
      mode: per_tenant
  """

  defp rejected(yaml) do
    assert {:error, %Error{} = error} = Config.parse(yaml)
    {error.path, error.reason}
  end

  describe "a valid document" do
    test "needs nothing but a queue and its mode" do
      assert {:ok, %Config{} = config} = Config.parse(@valid)

      assert config.store == {Fairway.Store.Memory, []}
      assert config.topologies == []

      assert config.queues == %{
               "emails" => %Queue{
                 name: "emails",
                 mode: :per_tenant,
                 concurrency: 4,
                 policy_opts: %{},
                 slice_ms: :infinity,
                 retry: %{max_attempts: 5, base_backoff_ms: 1_000, max_backoff_ms: 60_000},
                 poll_interval_ms: 1_000,
                 orphan_grace_ms: 10_000
               }
             }
    end

    test "selects each fairness mode per queue" do
      assert {:ok, config} =
               Config.parse("""
               queues:
                 control:
                   mode: fifo
                 emails:
                   mode: per_tenant
                   concurrency: 8
                 webhooks:
                   mode: throttle
                   concurrency: 16
                   throttle: {max_concurrency: 2, rate: 2.5, burst: 5}
                 imports:
                   mode: shuffle_shard
                   concurrency: 8
                   shuffle_shard: {shard_size: 2}
                 exports:
                   mode: interruptible
                   interruptible: {slice_ms: 500}
                   retry: {max_attempts: 3, base_backoff_ms: 500}
                   poll_interval_ms: 250
                   orphan_grace_ms: 0
               """)

      assert %{
               "control" => %Queue{mode: :fifo, policy_opts: %{}},
               "emails" => %Queue{mode: :per_tenant, concurrency: 8},
               "webhooks" => %Queue{
                 mode: :throttle,
                 policy_opts: %{max_concurrency: 2, rate: 2.5, burst: 5}
               },
               "imports" => %Queue{
                 mode: :shuffle_shard,
                 policy_opts: %{slots: 8, shard_size: 2, seed: 0}
               },
               "exports" => %Queue{
                 mode: :interruptible,
                 slice_ms: 500,
                 retry: %{max_attempts: 3, base_backoff_ms: 500, max_backoff_ms: 60_000},
                 poll_interval_ms: 250,
                 orphan_grace_ms: 0
               }
             } = config.queues
    end

    test "policy options are what the mode's init/1 accepts" do
      {:ok, config} =
        Config.parse("""
        queues:
          a: {mode: throttle, throttle: {max_concurrency: 1, rate: 1, burst: 1}}
          b: {mode: shuffle_shard, shuffle_shard: {shard_size: 4, seed: -3}}
          c: {mode: interruptible, interruptible: {slice_ms: 10}}
          d: {mode: fifo}
          e: {mode: per_tenant}
        """)

      for {_name, queue} <- config.queues do
        mode = Fairway.Fairness.module(queue.mode)
        state = mode.init(queue.policy_opts)

        assert mode.select(state, %{ready: [], running: %{}, free: [0], now: 0}) == :idle
      end
    end

    test "configures the SQLite store" do
      assert {:ok, config} =
               Config.parse(@valid <> "store: {adapter: sqlite, path: /tmp/jobs.db}")

      assert config.store == {Fairway.Store.SQLite, [path: "/tmp/jobs.db"]}
    end

    test "configures the PostgreSQL store" do
      url = "postgres://u:p@db/fairway"

      assert {:ok, config} = Config.parse(@valid <> "store: {adapter: postgres, url: #{url}}")
      assert config.store == {Fairway.Store.Postgres, [url: url, pool_size: 5]}

      assert {:ok, config} =
               Config.parse(@valid <> "store: {adapter: postgres, url: #{url}, pool_size: 20}")

      assert config.store == {Fairway.Store.Postgres, [url: url, pool_size: 20]}
    end

    test "configures clustering" do
      assert {:ok, config} =
               Config.parse(
                 @valid <> ~s(cluster: {strategy: epmd, hosts: ["a@10.0.0.1", "b@10.0.0.2"]})
               )

      assert config.topologies == [
               fairway: [
                 strategy: Cluster.Strategy.Epmd,
                 config: [hosts: [:"a@10.0.0.1", :"b@10.0.0.2"]]
               ]
             ]

      assert {:ok, config} = Config.parse(@valid <> "cluster: {strategy: gossip}")
      assert config.topologies == [fairway: [strategy: Cluster.Strategy.Gossip]]

      assert {:ok, %Config{topologies: []}} = Config.parse(@valid <> "cluster: {strategy: none}")
    end
  end

  describe "an invalid document is rejected with the key at fault:" do
    test "a misspelt key" do
      assert {"queues.emails.concurency", reason} =
               rejected("""
               queues:
                 emails:
                   mode: per_tenant
                   concurency: 8
               """)

      assert reason =~ "unknown key"
      assert reason =~ "concurrency"
    end

    test "an unknown key at any depth" do
      assert {"colour", _} = rejected(@valid <> "colour: blue")
      assert {"store.adaptor", _} = rejected(@valid <> "store: {adaptor: sqlite}")
      assert {"cluster.nodes", _} = rejected(@valid <> "cluster: {nodes: []}")

      assert {"queues.q.retry.attempts", _} =
               rejected("queues: {q: {mode: fifo, retry: {attempts: 3}}}")

      assert {"queues.q.throttle.limit", _} =
               rejected("queues: {q: {mode: throttle, throttle: {limit: 3}}}")
    end

    test "a missing required key" do
      assert {"queues", "is required"} = rejected("store: {adapter: memory}")
      assert {"queues.q.mode", "is required"} = rejected("queues: {q: {concurrency: 2}}")

      assert {"queues.q.throttle.burst", "is required"} =
               rejected("queues: {q: {mode: throttle, throttle: {max_concurrency: 1, rate: 1}}}")

      assert {"queues.q.shuffle_shard.shard_size", "is required"} =
               rejected("queues: {q: {mode: shuffle_shard, shuffle_shard: {seed: 1}}}")
    end

    test "an unknown mode" do
      assert {"queues.q.mode", reason} = rejected("queues: {q: {mode: fair}}")
      assert reason =~ "expected one of"
      assert reason =~ "shuffle_shard"
      assert reason =~ ~s("fair")
    end

    test "a value of the wrong type" do
      assert {"queues.q.concurrency", "expected a positive integer; got: 0"} =
               rejected("queues: {q: {mode: fifo, concurrency: 0}}")

      assert {"queues.q.concurrency", "expected a positive integer; got: \"four\""} =
               rejected("queues: {q: {mode: fifo, concurrency: four}}")

      assert {"queues.q.poll_interval_ms", _} =
               rejected("queues: {q: {mode: fifo, poll_interval_ms: 1.5}}")

      assert {"queues.q.orphan_grace_ms", "expected a non-negative integer; got: -1"} =
               rejected("queues: {q: {mode: fifo, orphan_grace_ms: -1}}")

      assert {"queues.q.throttle.rate", "expected a positive number; got: 0"} =
               rejected(
                 "queues: {q: {mode: throttle, throttle: {max_concurrency: 1, rate: 0, burst: 1}}}"
               )

      assert {"queues.q.retry", "expected a mapping; got: 3"} =
               rejected("queues: {q: {mode: fifo, retry: 3}}")

      assert {"queues.q", "expected a mapping; got: \"fifo\""} = rejected("queues: {q: fifo}")

      assert {"store.pool_size", _} =
               rejected(@valid <> "store: {adapter: postgres, url: x, pool_size: many}")

      assert {"cluster.hosts[1]", _} =
               rejected(@valid <> "cluster: {strategy: epmd, hosts: [a@b, 7]}")
    end

    test "no queues" do
      assert {"queues", "expected a non-empty mapping; got: %{}"} = rejected("queues: {}")
      assert {"queues", _} = rejected("queues: [a, b]")
    end

    test "a queue name that is not a string" do
      assert {"queues.7", "names must be non-empty strings"} =
               rejected("queues: {7: {mode: fifo}}")
    end

    test "a mode without its section" do
      assert {"queues.q.throttle", "is required when mode is throttle"} =
               rejected("queues: {q: {mode: throttle}}")

      assert {"queues.q.shuffle_shard", "is required when mode is shuffle_shard"} =
               rejected("queues: {q: {mode: shuffle_shard}}")

      assert {"queues.q.interruptible", "is required when mode is interruptible"} =
               rejected("queues: {q: {mode: interruptible}}")
    end

    test "a section for a mode the queue is not in" do
      assert {"queues.q.throttle", "only applies when mode is throttle"} =
               rejected(
                 "queues: {q: {mode: per_tenant, throttle: {max_concurrency: 1, rate: 1, burst: 1}}}"
               )

      assert {"queues.q.interruptible", "only applies when mode is interruptible"} =
               rejected("queues: {q: {mode: fifo, interruptible: {slice_ms: 5}}}")
    end

    test "a shard larger than the queue" do
      assert {"queues.q.shuffle_shard.shard_size", "must not exceed concurrency (4)"} =
               rejected("queues: {q: {mode: shuffle_shard, shuffle_shard: {shard_size: 5}}}")
    end

    test "a backoff floor above its ceiling" do
      assert {"queues.q.retry.base_backoff_ms", "must not exceed max_backoff_ms (100)"} =
               rejected(
                 "queues: {q: {mode: fifo, retry: {base_backoff_ms: 200, max_backoff_ms: 100}}}"
               )
    end

    test "store options that do not belong to the adapter" do
      assert {"store.adapter", _} = rejected(@valid <> "store: {adapter: redis}")
      assert {"store.path", "is required"} = rejected(@valid <> "store: {adapter: sqlite}")
      assert {"store.url", "is required"} = rejected(@valid <> "store: {adapter: postgres}")

      assert {"store.path", "only applies to the sqlite adapter"} =
               rejected(@valid <> "store: {adapter: memory, path: /tmp/x.db}")

      assert {"store.url", "only applies to the postgres adapter"} =
               rejected(@valid <> "store: {adapter: sqlite, path: /tmp/x.db, url: postgres://x}")
    end

    test "cluster options that do not belong to the strategy" do
      assert {"cluster.strategy", _} = rejected(@valid <> "cluster: {strategy: consul}")
      assert {"cluster.hosts", "is required"} = rejected(@valid <> "cluster: {strategy: epmd}")
      assert {"cluster.hosts", _} = rejected(@valid <> "cluster: {strategy: epmd, hosts: []}")

      assert {"cluster.hosts", "only applies to the epmd strategy"} =
               rejected(@valid <> "cluster: {strategy: gossip, hosts: [a@b]}")
    end

    test "the first error is reported, in a stable order" do
      yaml = "queues: {b: {mode: nope}, a: {mode: nope}}"
      assert {"queues.a.mode", _} = rejected(yaml)
      assert rejected(yaml) == rejected(yaml)
    end
  end

  describe "a document that is not a configuration at all" do
    test "is rejected without a key" do
      assert {nil, reason} = rejected("just a string")
      assert reason =~ "expected a mapping at the top level"

      assert {nil, reason} = rejected("queues: {a: [unclosed")
      assert reason =~ "not valid YAML"
    end

    test "from_map/1 accepts what a YAML parser would produce" do
      assert {:ok, %Config{}} = Config.from_map(%{"queues" => %{"q" => %{"mode" => "fifo"}}})
      assert {:error, %Error{path: "queues"}} = Config.from_map(%{queues: %{}})
    end
  end

  describe "load/1" do
    @describetag :tmp_dir

    test "reads a file", %{tmp_dir: tmp_dir} do
      path = Path.join(tmp_dir, "fairway.yml")
      File.write!(path, @valid)

      assert {:ok, %Config{queues: %{"emails" => %Queue{}}}} = Config.load(path)
    end

    test "the example in the repository is valid" do
      assert {:ok, config} = Config.load("examples/fairway.yml")

      assert {Fairway.Store.Postgres,
              [url: "postgres://fairway:secret@db.internal:5432/fairway", pool_size: 10]} =
               config.store

      assert [fairway: [strategy: Cluster.Strategy.Epmd, config: [hosts: [_one, _two, _three]]]] =
               config.topologies

      assert %{
               "emails" => %Queue{mode: :per_tenant},
               "webhooks" => %Queue{mode: :throttle},
               "imports" => %Queue{mode: :shuffle_shard},
               "exports" => %Queue{mode: :interruptible, slice_ms: 500}
             } = config.queues
    end

    test "names the file it could not read", %{tmp_dir: tmp_dir} do
      path = Path.join(tmp_dir, "absent.yml")

      assert {:error, %Error{path: nil, reason: reason}} = Config.load(path)
      assert reason =~ path
    end
  end

  describe "errors" do
    test "read as a sentence, with the key first" do
      {:error, error} = Config.parse("queues: {emails: {mode: per_tenant, concurency: 8}}")

      assert Exception.message(error) =~
               "invalid Fairway configuration at queues.emails.concurency: unknown key"

      assert_raise Error, ~r/queues\.emails\.concurency/, fn ->
        Config.parse!("queues: {emails: {mode: per_tenant, concurency: 8}}")
      end
    end
  end
end
