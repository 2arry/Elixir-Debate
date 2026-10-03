if Code.ensure_loaded?(Exqlite.Sqlite3) do
  defmodule Fairway.Store.SQLite do
    @moduledoc """
    Jobs in a SQLite file, through `exqlite`.

        store:
          adapter: sqlite
          path: /var/lib/fairway/jobs.db

    One process owns one connection, and every statement goes through it. Each
    change is a single statement, so SQLite's own locking makes a claim atomic,
    also between nodes: several nodes **on the same host** can open the same
    file (WAL mode, with a five second busy timeout). That is how the cluster
    tests run without an external service. It does not extend across hosts;
    use PostgreSQL there.

    The table is created on start. Needs the optional `:exqlite` dependency.
    """

    @behaviour Fairway.Store

    use GenServer

    alias Exqlite.Sqlite3
    alias Fairway.Store.Row

    @columns Enum.join(Row.columns(), ", ")
    @call_timeout 15_000

    @setup [
      "PRAGMA busy_timeout = 5000",
      "PRAGMA journal_mode = WAL",
      "PRAGMA synchronous = NORMAL",
      """
      CREATE TABLE IF NOT EXISTS fairway_jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        queue TEXT NOT NULL,
        tenant TEXT NOT NULL,
        worker TEXT NOT NULL,
        args TEXT NOT NULL,
        state TEXT NOT NULL DEFAULT 'available',
        attempt INTEGER NOT NULL DEFAULT 0,
        failures INTEGER NOT NULL DEFAULT 0,
        max_attempts INTEGER NOT NULL,
        run_at INTEGER NOT NULL,
        progress TEXT,
        slot INTEGER,
        node TEXT,
        last_error TEXT,
        inserted_at INTEGER NOT NULL,
        finished_at INTEGER
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
    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
    RETURNING #{@columns}
    """

    @ready_tenants """
    SELECT tenant FROM fairway_jobs
    WHERE queue = ?1 AND state = 'available' AND run_at <= ?2
    GROUP BY tenant
    ORDER BY MIN(id)
    """

    @claim """
    UPDATE fairway_jobs
    SET state = 'running', attempt = attempt + 1, node = ?4, slot = ?5
    WHERE id = (
      SELECT id FROM fairway_jobs
      WHERE queue = ?1 AND tenant = ?2 AND state = 'available' AND run_at <= ?3
      ORDER BY id
      LIMIT 1
    )
    RETURNING #{@columns}
    """

    @complete """
    UPDATE fairway_jobs
    SET state = 'completed', slot = NULL, finished_at = ?3
    WHERE id = ?1 AND attempt = ?2 AND state = 'running'
    RETURNING id
    """

    @retry """
    UPDATE fairway_jobs
    SET state = 'available', slot = NULL, failures = failures + 1,
        run_at = ?3, last_error = ?4, progress = ?5
    WHERE id = ?1 AND attempt = ?2 AND state = 'running'
    RETURNING id
    """

    @discard """
    UPDATE fairway_jobs
    SET state = 'discarded', slot = NULL, failures = failures + 1,
        last_error = ?3, finished_at = ?4
    WHERE id = ?1 AND attempt = ?2 AND state = 'running'
    RETURNING id
    """

    @yield """
    UPDATE fairway_jobs
    SET state = 'available', slot = NULL, progress = ?3
    WHERE id = ?1 AND attempt = ?2 AND state = 'running'
    RETURNING id
    """

    @running """
    SELECT #{@columns} FROM fairway_jobs WHERE queue = ?1 AND state = 'running' ORDER BY id
    """

    @fetch "SELECT #{@columns} FROM fairway_jobs WHERE id = ?1"

    @counts """
    SELECT tenant, state, COUNT(*) FROM fairway_jobs WHERE queue = ?1 GROUP BY tenant, state
    """

    @spec start_link(keyword()) :: GenServer.on_start()
    def start_link(opts) do
      path = Keyword.fetch!(opts, :path)
      GenServer.start_link(__MODULE__, path, name: Keyword.get(opts, :name, __MODULE__))
    end

    @impl Fairway.Store
    def insert_all(server, jobs) do
      rows =
        Enum.map(jobs, fn job ->
          [
            job.queue,
            job.tenant,
            job.worker,
            Row.encode(job.args),
            job.max_attempts,
            job.run_at,
            job.inserted_at
          ]
        end)

      {:ok, server |> call({:insert_all, rows}) |> Enum.map(&Row.to_job/1)}
    end

    @impl Fairway.Store
    def ready_tenants(server, queue, now) do
      server |> query(@ready_tenants, [queue, now]) |> Enum.map(fn [tenant] -> tenant end)
    end

    @impl Fairway.Store
    def claim(server, queue, tenant, %{node: node, slot: slot, now: now}) do
      case query(server, @claim, [queue, tenant, now, node, slot]) do
        [row] -> {:ok, Row.to_job(row)}
        [] -> :none
      end
    end

    @impl Fairway.Store
    def complete(server, id, attempt, now), do: ack(server, @complete, [id, attempt, now])

    @impl Fairway.Store
    def retry(server, id, attempt, %{run_at: run_at, error: error, cursor: cursor}) do
      ack(server, @retry, [id, attempt, run_at, error, Row.encode(cursor)])
    end

    @impl Fairway.Store
    def discard(server, id, attempt, %{error: error, now: now}) do
      ack(server, @discard, [id, attempt, error, now])
    end

    @impl Fairway.Store
    def yield(server, id, attempt, cursor) do
      ack(server, @yield, [id, attempt, Row.encode(cursor)])
    end

    @impl Fairway.Store
    def running(server, queue) do
      server |> query(@running, [queue]) |> Enum.map(&Row.to_job/1)
    end

    @impl Fairway.Store
    def fetch(server, id) do
      case query(server, @fetch, [id]) do
        [row] -> {:ok, Row.to_job(row)}
        [] -> :error
      end
    end

    @impl Fairway.Store
    def counts(server, queue), do: server |> query(@counts, [queue]) |> Row.counts()

    defp ack(server, sql, params) do
      case query(server, sql, params) do
        [[_id]] -> :ok
        [] -> {:error, :stale}
      end
    end

    defp query(server, sql, params), do: call(server, {:query, sql, params})
    defp call(server, request), do: GenServer.call(server, request, @call_timeout)

    @impl GenServer
    def init(path) do
      with {:ok, conn} <- Sqlite3.open(path),
           :ok <- setup(conn) do
        {:ok, conn}
      else
        {:error, reason} -> {:stop, {:sqlite, path, reason}}
      end
    end

    @impl GenServer
    def handle_call({:query, sql, params}, _from, conn) do
      {:reply, run(conn, sql, params), conn}
    end

    # One transaction, so a batch is saved whole or not at all. If a statement
    # fails this process exits, the connection closes and SQLite rolls back.
    def handle_call({:insert_all, rows}, _from, conn) do
      :ok = Sqlite3.execute(conn, "BEGIN IMMEDIATE")
      saved = Enum.flat_map(rows, &run(conn, @insert, &1))
      :ok = Sqlite3.execute(conn, "COMMIT")
      {:reply, saved, conn}
    end

    defp setup(conn) do
      Enum.reduce_while(@setup, :ok, fn sql, :ok ->
        case Sqlite3.execute(conn, sql) do
          :ok -> {:cont, :ok}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)
    end

    defp run(conn, sql, params) do
      {:ok, statement} = Sqlite3.prepare(conn, sql)

      try do
        :ok = Sqlite3.bind(statement, params)
        {:ok, rows} = Sqlite3.fetch_all(conn, statement)
        rows
      after
        :ok = Sqlite3.release(conn, statement)
      end
    end
  end
end
