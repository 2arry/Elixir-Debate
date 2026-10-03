defmodule Fairway.StoreContract.Check do
  @moduledoc false

  # Defines a check, a public function named after its description, and
  # records it, so that none can be written and then forgotten.
  defmacro check(description, store, do: body) do
    name = String.to_atom(description)

    quote do
      @checks unquote(name)
      def unquote(name)(unquote(store)), do: unquote(body)
    end
  end
end

defmodule Fairway.StoreContract do
  @moduledoc false
  # What every store adapter must do, as checks that take a store.
  #
  # A test module starts its adapter in `setup`, puts `store: {adapter, server}`
  # in the context and says `use Fairway.StoreContract`, which turns every
  # check below into a test of that module. The checks are ordinary functions,
  # so a failure points at a line in this file.

  import ExUnit.Assertions
  import Fairway.StoreContract.Check

  alias Fairway.{Job, Store}

  Module.register_attribute(__MODULE__, :checks, accumulate: true)

  defmacro __using__(_opts) do
    quote unquote: false do
      import Fairway.StoreContract, only: [insert: 2, claim!: 2, claim!: 3]

      alias Fairway.{Job, Store}

      for check <- Fairway.StoreContract.checks() do
        test Atom.to_string(check), %{store: store} do
          apply(Fairway.StoreContract, unquote(check), [store])
        end
      end
    end
  end

  ## Helpers, also imported into the adapters' own tests

  @doc "An unsaved job with sensible defaults."
  @spec job(keyword()) :: Job.t()
  def job(attrs \\ []) do
    struct!(
      %Job{
        queue: "q",
        tenant: "t",
        worker: "W",
        args: %{},
        max_attempts: 3,
        run_at: 0,
        inserted_at: 0
      },
      attrs
    )
  end

  @spec insert(Store.t(), [keyword()]) :: [Job.t()]
  def insert(store, attrs_list) do
    {:ok, jobs} = Store.insert_all(store, Enum.map(attrs_list, &job/1))
    jobs
  end

  @spec claim!(Store.t(), String.t(), keyword()) :: Job.t()
  def claim!(store, tenant, opts \\ []) do
    claim = %{node: opts[:node] || "n1@host", slot: opts[:slot] || 0, now: opts[:now] || 100}
    {:ok, job} = Store.claim(store, opts[:queue] || "q", tenant, claim)
    job
  end

  @doc "Claims jobs of `tenant` until there are none, and returns their ids."
  @spec claim_all(Store.t(), String.t(), non_neg_integer()) :: [pos_integer()]
  def claim_all(store, tenant, slot) do
    claim = %{node: "n@host", slot: slot, now: 100}

    fn -> Store.claim(store, "q", tenant, claim) end
    |> Stream.repeatedly()
    |> Enum.take_while(&(&1 != :none))
    |> Enum.map(fn {:ok, job} -> job.id end)
  end

  ## insert_all/2 and fetch/2

  check "insert_all/2 returns the jobs with ascending ids in the order given", store do
    jobs = insert(store, [[tenant: "a"], [tenant: "b"], [tenant: "c"]])

    assert Enum.map(jobs, & &1.tenant) == ["a", "b", "c"]
    assert [first, second, third] = Enum.map(jobs, & &1.id)
    assert first < second and second < third
  end

  check "insert_all/2 saves new jobs as available and untried", store do
    [job] = insert(store, [[max_attempts: 7, run_at: 42, inserted_at: 41]])

    assert {:ok, saved} = Store.fetch(store, job.id)
    assert saved == job

    assert %Job{
             state: :available,
             attempt: 0,
             failures: 0,
             max_attempts: 7,
             run_at: 42,
             inserted_at: 41,
             cursor: nil,
             slot: nil,
             node: nil,
             last_error: nil,
             finished_at: nil
           } = saved
  end

  check "insert_all/2 keeps args as the JSON that went in", store do
    args = %{
      "text" => "naïve ☃",
      "int" => 9_007_199_254_740_993,
      "float" => 1.5,
      "flags" => [true, false, nil],
      "nested" => %{"list" => [1, %{"deep" => "yes"}]}
    }

    [job] = insert(store, [[args: args]])

    assert {:ok, %Job{args: ^args}} = Store.fetch(store, job.id)
  end

  check "insert_all/2 accepts an empty batch", store do
    assert {:ok, []} = Store.insert_all(store, [])
  end

  check "fetch/2 answers :error for an id that was never issued", store do
    assert Store.fetch(store, 123_456) == :error
  end

  ## ready_tenants/3

  check "ready_tenants/3 lists tenants by the age of their oldest ready job", store do
    insert(store, [[tenant: "b"], [tenant: "a"], [tenant: "b"], [tenant: "c"], [tenant: "a"]])

    assert Store.ready_tenants(store, "q", 100) == ["b", "a", "c"]
  end

  check "ready_tenants/3 leaves out jobs whose time has not come", store do
    insert(store, [[tenant: "later", run_at: 200], [tenant: "now", run_at: 100]])

    assert Store.ready_tenants(store, "q", 99) == []
    assert Store.ready_tenants(store, "q", 100) == ["now"]
    assert Store.ready_tenants(store, "q", 200) == ["later", "now"]
  end

  check "ready_tenants/3 orders by the oldest job that is ready, not the oldest job", store do
    insert(store, [[tenant: "a", run_at: 500], [tenant: "b"], [tenant: "a"]])

    assert Store.ready_tenants(store, "q", 100) == ["b", "a"]
  end

  check "ready_tenants/3 sees only its own queue and only waiting jobs", store do
    insert(store, [[tenant: "other", queue: "elsewhere"], [tenant: "busy"], [tenant: "idle"]])
    claim!(store, "busy")

    assert Store.ready_tenants(store, "q", 100) == ["idle"]
    assert Store.ready_tenants(store, "elsewhere", 100) == ["other"]
    assert Store.ready_tenants(store, "nowhere", 100) == []
  end

  ## claim/4

  check "claim/4 takes the tenant's oldest ready job and marks it running", store do
    [first, second] = insert(store, [[tenant: "a"], [tenant: "a"]])

    claim = %{node: "n2@host", slot: 3, now: 100}
    assert {:ok, claimed} = Store.claim(store, "q", "a", claim)

    assert claimed.id == first.id
    assert %Job{state: :running, attempt: 1, failures: 0, node: "n2@host", slot: 3} = claimed
    assert Store.fetch(store, first.id) == {:ok, claimed}

    assert {:ok, %Job{id: id}} = Store.claim(store, "q", "a", claim)
    assert id == second.id
    assert Store.claim(store, "q", "a", claim) == :none
  end

  check "claim/4 skips jobs that are not ready, and other tenants and queues", store do
    [_future, ready, _other_tenant, _other_queue] =
      insert(store, [
        [tenant: "a", run_at: 500],
        [tenant: "a"],
        [tenant: "b"],
        [tenant: "a", queue: "elsewhere"]
      ])

    assert claim!(store, "a").id == ready.id
    assert Store.claim(store, "q", "a", %{node: "n", slot: 0, now: 100}) == :none
    assert Store.claim(store, "q", "nobody", %{node: "n", slot: 0, now: 100}) == :none
  end

  check "claim/4 never hands the same job to two claimers", store do
    jobs = insert(store, List.duplicate([tenant: "a"], 60))

    claimed =
      1..12
      |> Enum.map(fn slot -> Task.async(fn -> claim_all(store, "a", slot) end) end)
      |> Task.await_many(30_000)
      |> List.flatten()

    assert Enum.sort(claimed) == Enum.map(jobs, & &1.id)
  end

  ## Acknowledgements

  check "complete/4 finishes a running job", store do
    [job] = insert(store, [[tenant: "a"]])
    claimed = claim!(store, "a", slot: 2)

    assert Store.complete(store, job.id, claimed.attempt, 777) == :ok

    assert {:ok, %Job{state: :completed, finished_at: 777, slot: nil, attempt: 1, failures: 0}} =
             Store.fetch(store, job.id)

    assert Store.running(store, "q") == []
  end

  check "retry/4 puts the job back for later, counting the failure", store do
    [job] = insert(store, [[tenant: "a"]])
    claimed = claim!(store, "a")
    change = %{run_at: 300, error: "boom", cursor: %{"step" => 4}}

    assert Store.retry(store, job.id, claimed.attempt, change) == :ok

    assert {:ok,
            %Job{
              state: :available,
              failures: 1,
              attempt: 1,
              run_at: 300,
              last_error: "boom",
              cursor: %{"step" => 4},
              slot: nil
            }} = Store.fetch(store, job.id)

    assert Store.ready_tenants(store, "q", 299) == []
    assert %Job{attempt: 2, failures: 1} = claim!(store, "a", now: 300)
  end

  check "discard/4 ends the job for good, counting the failure", store do
    [job] = insert(store, [[tenant: "a"]])
    claimed = claim!(store, "a")

    assert Store.discard(store, job.id, claimed.attempt, %{error: "gave up", now: 888}) == :ok

    assert {:ok, %Job{state: :discarded, failures: 1, last_error: "gave up", finished_at: 888}} =
             Store.fetch(store, job.id)

    assert Store.ready_tenants(store, "q", 10_000) == []
  end

  check "yield/4 puts the job back at once with its cursor and no failure", store do
    [job] = insert(store, [[tenant: "a"]])
    claimed = claim!(store, "a")
    cursor = %{"offset" => 1500, "seen" => ["x", nil, 2.5], "more" => %{"deep" => true}}

    assert Store.yield(store, job.id, claimed.attempt, cursor) == :ok

    assert {:ok, %Job{state: :available, failures: 0, cursor: ^cursor, slot: nil}} =
             Store.fetch(store, job.id)

    assert %Job{attempt: 2, failures: 0, cursor: ^cursor} = claim!(store, "a")
  end

  check "yield/4 keeps the job's place in its tenant's queue", store do
    [first, _second] = insert(store, [[tenant: "a"], [tenant: "a"]])
    claimed = claim!(store, "a")
    :ok = Store.yield(store, first.id, claimed.attempt, 1)

    assert claim!(store, "a").id == first.id
  end

  check "yield/4 stores scalar and list cursors too", store do
    [job] = insert(store, [[tenant: "a"]])

    for cursor <- [0, false, "text", [1, 2]] do
      claimed = claim!(store, "a")
      assert Store.yield(store, job.id, claimed.attempt, cursor) == :ok
      assert {:ok, %Job{cursor: ^cursor}} = Store.fetch(store, job.id)
    end
  end

  ## Fencing

  check "fencing: an acknowledgement with the wrong attempt changes nothing", store do
    [job] = insert(store, [[tenant: "a"]])
    claimed = claim!(store, "a")
    wrong = claimed.attempt + 1

    assert Store.complete(store, job.id, wrong, 1) == {:error, :stale}
    assert Store.yield(store, job.id, wrong, 1) == {:error, :stale}
    assert Store.discard(store, job.id, wrong, %{error: "e", now: 1}) == {:error, :stale}

    assert Store.retry(store, job.id, wrong, %{run_at: 1, error: "e", cursor: nil}) ==
             {:error, :stale}

    assert Store.fetch(store, job.id) == {:ok, claimed}
  end

  check "fencing: an execution that was given up on cannot report", store do
    [job] = insert(store, [[tenant: "a"]])
    zombie = claim!(store, "a", node: "old@host")
    :ok = Store.retry(store, job.id, zombie.attempt, %{run_at: 0, error: "lost", cursor: nil})
    current = claim!(store, "a", node: "new@host")

    assert Store.complete(store, job.id, zombie.attempt, 1) == {:error, :stale}
    assert Store.fetch(store, job.id) == {:ok, current}
    assert Store.complete(store, job.id, current.attempt, 2) == :ok
  end

  check "fencing: a job that is not running cannot be acknowledged", store do
    [job] = insert(store, [[tenant: "a"]])
    assert Store.complete(store, job.id, 0, 1) == {:error, :stale}

    claimed = claim!(store, "a")
    :ok = Store.complete(store, job.id, claimed.attempt, 1)

    assert Store.complete(store, job.id, claimed.attempt, 2) == {:error, :stale}
    assert Store.yield(store, job.id, claimed.attempt, 1) == {:error, :stale}
    assert {:ok, %Job{state: :completed, finished_at: 1}} = Store.fetch(store, job.id)
  end

  check "fencing: an unknown job cannot be acknowledged", store do
    assert Store.complete(store, 987_654, 1, 1) == {:error, :stale}
  end

  ## running/2 and counts/2

  check "running/2 lists the queue's running jobs, oldest first, with where they are", store do
    insert(store, [[tenant: "a"], [tenant: "b"], [tenant: "a"], [tenant: "z", queue: "elsewhere"]])

    second = claim!(store, "b", node: "n2@host", slot: 5)
    first = claim!(store, "a", node: "n1@host", slot: 1)
    claim!(store, "z", queue: "elsewhere")

    assert Store.running(store, "q") == [first, second]

    assert [%Job{node: "n1@host", slot: 1}, %Job{node: "n2@host", slot: 5}] =
             Store.running(store, "q")
  end

  check "counts/2 counts jobs by tenant and state", store do
    insert(store, [[tenant: "a"], [tenant: "a"], [tenant: "a"], [tenant: "b"], [tenant: "b"]])
    insert(store, [[tenant: "a", queue: "elsewhere"]])

    done = claim!(store, "a")
    :ok = Store.complete(store, done.id, done.attempt, 1)
    claim!(store, "a")
    dead = claim!(store, "b")
    :ok = Store.discard(store, dead.id, dead.attempt, %{error: "e", now: 1})

    assert Store.counts(store, "q") == %{
             {"a", :completed} => 1,
             {"a", :running} => 1,
             {"a", :available} => 1,
             {"b", :discarded} => 1,
             {"b", :available} => 1
           }

    assert Store.counts(store, "nowhere") == %{}
  end

  @doc "The names of every check, in the order they are written."
  @spec checks() :: [atom()]
  def checks, do: Enum.reverse(@checks)
end
