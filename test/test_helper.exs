# Postgres tests need a database. They run when FAIRWAY_PG_URL points at one,
# as it does in CI, and are skipped otherwise.
exclude = if System.get_env("FAIRWAY_PG_URL"), do: [], else: [:postgres]

ExUnit.start(exclude: exclude, capture_log: true)
