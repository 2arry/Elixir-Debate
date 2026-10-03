defmodule Fairway.Fairness do
  @moduledoc """
  The contract between a queue's scheduler and its fairness mode.

  A mode is a module of pure functions. The scheduler shows it a snapshot of
  the queue and asks one question: given what is waiting and what is running,
  which tenant should start a job next, and in which slot?

  The snapshot (`t:view/0`) has:

    * `:ready` - tenants that have a job ready to run, ordered by their oldest
      such job, oldest first.
    * `:running` - the tenant occupying each busy slot.
    * `:free` - the free slots, in the order the scheduler would use them.
    * `:now` - the scheduler's monotonic clock, in milliseconds.

  `c:select/2` answers `{:run, tenant, slot}`, `{:wait, ms}` when something is
  ready but not yet allowed (the scheduler asks again after `ms`), or `:idle`.
  It must not change anything: the scheduler calls `c:started/4` only once the
  store has confirmed the claim, and that is where a mode records the start.

  Because a mode does no I/O and owns no process, it can be tested by calling
  it, and the scheduler can rebuild it from nothing after a failover.

  | Mode             | Module                           |
  |------------------|----------------------------------|
  | `:fifo`          | `Fairway.Fairness.FIFO`          |
  | `:per_tenant`    | `Fairway.Fairness.RoundRobin`    |
  | `:throttle`      | `Fairway.Fairness.Throttle`      |
  | `:shuffle_shard` | `Fairway.Fairness.ShuffleShard`  |
  | `:interruptible` | `Fairway.Fairness.Interruptible` |
  """

  alias Fairway.Fairness.{FIFO, Interruptible, RoundRobin, ShuffleShard, Throttle}

  @type tenant :: String.t()
  @type slot :: non_neg_integer()
  @type mode :: :fifo | :per_tenant | :throttle | :shuffle_shard | :interruptible
  @type state :: term()

  @type view :: %{
          ready: [tenant()],
          running: %{optional(slot()) => tenant()},
          free: [slot()],
          now: integer()
        }

  @type decision :: {:run, tenant(), slot()} | {:wait, pos_integer()} | :idle

  @doc "Builds the mode's state from the options in the queue's configuration."
  @callback init(opts :: map()) :: state()

  @doc "Chooses the next job to start. Must not have side effects."
  @callback select(state(), view()) :: decision()

  @doc "Records that a job of `tenant` was started in `slot` at `now`."
  @callback started(state(), tenant(), slot(), now :: integer()) :: state()

  @modes %{
    fifo: FIFO,
    per_tenant: RoundRobin,
    throttle: Throttle,
    shuffle_shard: ShuffleShard,
    interruptible: Interruptible
  }

  @doc "The modes a queue can be configured with."
  @spec modes() :: [mode()]
  def modes, do: Map.keys(@modes)

  @doc "The module that implements `mode`."
  @spec module(mode()) :: module()
  def module(mode), do: Map.fetch!(@modes, mode)

  @doc """
  Orders `tenants` as a ring, starting just after `last`.

  This is round-robin without per-tenant bookkeeping: the tenant ids, sorted,
  form the ring, and the only state is the tenant served last. A tenant cannot
  improve its position by emptying and refilling its queue, and there is no
  table of tenants to grow or prune.

      iex> Fairway.Fairness.ring(["c", "a", "b"], "a")
      ["b", "c", "a"]

      iex> Fairway.Fairness.ring(["c", "a", "b"], nil)
      ["a", "b", "c"]
  """
  @spec ring([tenant()], tenant() | nil) :: [tenant()]
  def ring(tenants, nil), do: Enum.sort(tenants)

  def ring(tenants, last) do
    {upto, after_last} = tenants |> Enum.sort() |> Enum.split_while(&(&1 <= last))
    after_last ++ upto
  end
end
