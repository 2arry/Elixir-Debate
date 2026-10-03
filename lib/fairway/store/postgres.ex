if Code.ensure_loaded?(Postgrex) do
  defmodule Fairway.Store.Postgres do
    @moduledoc """
    Jobs in PostgreSQL, through `postgrex`. The store for clusters that span
    hosts.

        store:
          adapter: postgres
          url: postgres://fairway:secret@db.internal:5432/fairway
          pool_size: 10

    Every node holds its own connection pool. A claim is one `UPDATE` whose
    subquery selects the oldest ready row `FOR UPDATE SKIP LOCKED`, so
    concurrent claims never wait on, or return, the same job.

    `args` and the cursor are `jsonb` columns, written and read as text and
    cast by the server, so no JSON library has to be configured for Postgrex.

    The table is created on start, under an advisory lock so that nodes booting
    together do not race. Needs the optional `:postgrex` dependency.
    """

    @behaviour Fairway.Store

    use Supervisor

    alias Fairway.Store.Row

    @columns Enum.map_join(Row.columns(), ", ", fn
               column when column in ["args", "progress"] -> column <> "::text"
               column -> column
             end)

    # Any constant will do; it only has to be the same on every node.
    @setup_lock 4_703_217_291

    @setup [
      """
      CREATE TABLE IF NOT EXISTS fairway_jobs (
        id BIGSERIAL PRIMARY KEY,
        queue TEXT NOT NULL,
        tenant TEXT NOT NULL,
        worker TEXT NOT NULL,
        args JSONB NOT NULL,
        state TEXT NOT NULL DEFAULT 'available',
        attempt INTEGER NOT NULL DEFAULT 0,
        failures INTEGER NOT NULL DEFAULT 0,
        max_attempts INTEGER NOT NULL,
        run_at BIGINT NOT NULL,
        progress JSONB,
        slot INTEGER,
        node TEXT,
        last_error TEXT,
        inserted_at BIGINT NOT NULL,
        finished_at BIGINT
      )
      """,
      """
      CREATE INDEX IF NOT EXISTS fairway_jobs_available
      ON fairway_jobs (queue, tenant, id) WHERE state = 'available'
      """,
      """
      CREATE INDEX IF NOT EXISTS fairway_jobs_running
      ON fairway_jobs (queue, id) WHERE state = 'running'
      """
    ]

    @insert """
    INSERT INTO fairway_jobs (queue, tenant, worker, args, max_attempts, run_at, inserted_at)
    SELECT queue, tenant, worker, args::jsonb, max_attempts, run_at, inserted_at
    FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::int[], $6::bigint[], $7::bigint[])
      WITH ORDINALITY
      AS new (queue, tenant, worker, args, max_attempts, run_at, inserted_at, position)
    ORDER BY position
    RETURNING #{@columns}
    """

    @ready_tenants """
    SELECT tenant FROM fairway_jobs
    WHERE queue = $1 AND state = 'available' AND run_at <= $2
    GROUP BY tenant
    ORDER BY MIN(id)
    """

    @claim """
    UPDATE fairway_jobs
    SET state = 'running', attempt = attempt + 1, node = $4, slot = $5
    WHERE id = (
      SELECT id FROM fairway_jobs
      WHERE queue = $1 AND tenant = $2 AND state = 'available' AND run_at <= $3
      ORDER BY id
      LIMIT 1
      FOR UPDATE SKIP LOCKED
    )
    RETURNING #{@columns}
    """

    @complete """
    UPDATE fairway_jobs
    SET state = 'completed', slot = NULL, finished_at = $3
    WHERE id = $1 AND attempt = $2 AND state = 'running'
    """

    @retry """
    UPDATE fairway_jobs
    SET state = 'available', slot = NULL, failures = failures + 1,
        run_at = $3, last_error = $4, progress = $5::text::jsonb
    WHERE id = $1 AND attempt = $2 AND state = 'running'
    """

    @discard """
    UPDATE fairway_jobs
    SET state = 'discarded', slot = NULL, failures = failures + 1,
        last_error = $3, finished_at = $4
    WHERE id = $1 AND attempt = $2 AND state = 'running'
    """

    @yield """
    UPDATE fairway_jobs
    SET state = 'available', slot = NULL, progress = $3::text::jsonb
    WHERE id = $1 AND attempt = $2 AND state = 'running'
    """

    @running """
    SELECT #{@columns} FROM fairway_jobs WHERE queue = $1 AND state = 'running' ORDER BY id
    """

    @fetch "SELECT #{@columns} FROM fairway_jobs WHERE id = $1"

    @counts """
    SELECT tenant, state, COUNT(*) FROM fairway_jobs WHERE queue = $1 GROUP BY tenant, state
    """

    @spec start_link(keyword()) :: Supervisor.on_start()
    def start_link(opts), do: Supervisor.start_link(__MODULE__, opts)

    @impl Fairway.Store
    def insert_all(pool, jobs) do
      columns =
        for field <- [:queue, :tenant, :worker, :args, :max_attempts, :run_at, :inserted_at] do
          Enum.map(jobs, &column(&1, field))
        end

      {:ok, pool |> rows(@insert, columns) |> Enum.map(&Row.to_job/1) |> Enum.sort_by(& &1.id)}
    end

    @impl Fairway.Store
    def ready_tenants(pool, queue, now) do
      pool |> rows(@ready_tenants, [queue, now]) |> Enum.map(fn [tenant] -> tenant end)
    end

    @impl Fairway.Store
    def claim(pool, queue, tenant, %{node: node, slot: slot, now: now}) do
      case rows(pool, @claim, [queue, tenant, now, node, slot]) do
        [row] -> {:ok, Row.to_job(row)}
        [] -> :none
      end
    end

    @impl Fairway.Store
    def complete(pool, id, attempt, now), do: ack(pool, @complete, [id, attempt, now])

    @impl Fairway.Store
    def retry(pool, id, attempt, %{run_at: run_at, error: error, cursor: cursor}) do
      ack(pool, @retry, [id, attempt, run_at, error, Row.encode(cursor)])
    end

    @impl Fairway.Store
    def discard(pool, id, attempt, %{error: error, now: now}) do
      ack(pool, @discard, [id, attempt, error, now])
    end

    @impl Fairway.Store
    def yield(pool, id, attempt, cursor), do: ack(pool, @yield, [id, attempt, Row.encode(cursor)])

    @impl Fairway.Store
    def running(pool, queue), do: pool |> rows(@running, [queue]) |> Enum.map(&Row.to_job/1)

    @impl Fairway.Store
    def fetch(pool, id) do
      case rows(pool, @fetch, [id]) do
        [row] -> {:ok, Row.to_job(row)}
        [] -> :error
      end
    end

    @impl Fairway.Store
    def counts(pool, queue), do: pool |> rows(@counts, [queue]) |> Row.counts()

    defp column(job, :args), do: Row.encode(job.args)
    defp column(job, field), do: Map.fetch!(job, field)

    defp rows(pool, sql, params), do: Postgrex.query!(pool, sql, params).rows

    defp ack(pool, sql, params) do
      case Postgrex.query!(pool, sql, params) do
        %Postgrex.Result{num_rows: 1} -> :ok
        %Postgrex.Result{num_rows: 0} -> {:error, :stale}
      end
    end

    @impl Supervisor
    def init(opts) do
      name = Keyword.get(opts, :name, __MODULE__)

      pool =
        opts
        |> Keyword.fetch!(:url)
        |> connection()
        |> Keyword.merge(name: name, pool_size: Keyword.get(opts, :pool_size, 5))

      setup_timeout = Keyword.get(opts, :setup_timeout, 10_000)

      children = [
        {Postgrex, pool},
        %{id: :setup, start: {__MODULE__, :setup, [name, setup_timeout]}, restart: :temporary}
      ]

      Supervisor.init(children, strategy: :rest_for_one)
    end

    @doc false
    # Runs inside the supervisor's start sequence, so nothing that needs the
    # table is started before it exists. Not a process: it answers `:ignore`.
    #
    # The pool connects in the background, so the database gets `timeout`
    # milliseconds to answer before Fairway refuses to start.
    @spec setup(GenServer.server(), non_neg_integer()) :: :ignore | {:error, term()}
    def setup(pool, timeout \\ 10_000) do
      with :ok <- await(pool, System.monotonic_time(:millisecond) + timeout),
           {:ok, _created} <- Postgrex.transaction(pool, &create_table/1) do
        :ignore
      end
    end

    defp await(pool, deadline) do
      case Postgrex.query(pool, "SELECT 1", []) do
        {:ok, _result} -> :ok
        {:error, error} -> retry_await(pool, deadline, error)
      end
    end

    defp retry_await(pool, deadline, error) do
      if System.monotonic_time(:millisecond) < deadline do
        Process.sleep(100)
        await(pool, deadline)
      else
        {:error, {:postgres_unavailable, Exception.message(error)}}
      end
    end

    # The lock is held until the transaction ends, so a second node waits here
    # and then finds the table already there.
    defp create_table(conn) do
      Enum.each(
        [{"SELECT pg_advisory_xact_lock($1)", [@setup_lock]} | Enum.map(@setup, &{&1, []})],
        fn {sql, params} -> Postgrex.query!(conn, sql, params) end
      )
    end

    defp connection(url) do
      uri = URI.parse(url)
      {username, password} = credentials(uri.userinfo)

      Enum.reject(
        [
          hostname: uri.host,
          port: uri.port || 5432,
          username: username,
          password: password,
          database: String.trim_leading(uri.path || "", "/")
        ],
        fn {_option, value} -> is_nil(value) end
      )
    end

    defp credentials(nil), do: {nil, nil}

    defp credentials(userinfo) do
      case String.split(userinfo, ":", parts: 2) do
        [username, password] -> {URI.decode(username), URI.decode(password)}
        [username] -> {URI.decode(username), nil}
      end
    end
  end
end
