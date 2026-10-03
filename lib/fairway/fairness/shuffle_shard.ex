defmodule Fairway.Fairness.ShuffleShard do
  @moduledoc """
  Confines each tenant to a small, fixed subset of the queue's slots.
  Configured as `mode: shuffle_shard`.

      concurrency: 8
      shuffle_shard:
        shard_size: 2   # slots each tenant may use
        seed: 0         # change to reshuffle every tenant

  A tenant's shard is `shard_size` of the `concurrency` slots, chosen by
  rendezvous hashing: every slot is scored with a hash of the seed, the slot
  and the tenant, and the lowest scores win. The hash is SHA-256, which gives
  the same answer on every node and every release, so the assignment needs no
  coordination and survives restarts. Raising `concurrency` moves a tenant
  only onto slots that are new.

  The hash has to mix well, because the scores of different slots are compared
  with each other. `:erlang.phash2/1` over a tuple does not: it ranks the slots
  nearly the same way for every tenant, which piles tenants onto a few slots.

  Slots are spread over the nodes of the cluster, so a shard is also a small
  set of nodes: a tenant whose jobs exhaust memory or crash the VM takes down
  only the nodes behind its own slots.

  ## Guarantee

    * A tenant's jobs only ever run in its shard, so it can occupy at most
      `shard_size` slots however much it enqueues.
    * A tenant that shares fewer than `shard_size` slots with a noisy tenant
      keeps at least one slot the noisy tenant cannot touch.

  ## Limits

    * Isolation is a matter of odds. Two tenants draw the same shard with
      probability `1 / C(concurrency, shard_size)`: 1 in 28 for 2 of 8, 1 in
      4,845 for 4 of 20. When they do, they share everything.
    * Inside a shared slot the oldest job goes first, so a noisy tenant with a
      long backlog does win the slots it shares.
    * A tenant never uses more than `shard_size` slots, even when the rest of
      the queue is idle. That is the price of the isolation.
  """

  @behaviour Fairway.Fairness

  alias Fairway.Fairness

  @enforce_keys [:slots, :shard_size, :seed]
  defstruct [:slots, :shard_size, :seed]

  @type t :: %__MODULE__{slots: pos_integer(), shard_size: pos_integer(), seed: integer()}

  @impl true
  def init(%{slots: slots, shard_size: shard_size, seed: seed}) do
    %__MODULE__{slots: slots, shard_size: shard_size, seed: seed}
  end

  @impl true
  def select(%__MODULE__{} = state, %{ready: ready, free: free}) do
    free = MapSet.new(free)

    Enum.find_value(ready, :idle, fn tenant ->
      case Enum.find(shard(state, tenant), &MapSet.member?(free, &1)) do
        nil -> nil
        slot -> {:run, tenant, slot}
      end
    end)
  end

  @impl true
  def started(state, _tenant, _slot, _now), do: state

  @doc """
  The slots `tenant` may use, in ascending order.

      iex> shards = Fairway.Fairness.ShuffleShard.init(%{slots: 8, shard_size: 2, seed: 0})
      iex> shard = Fairway.Fairness.ShuffleShard.shard(shards, "acme")
      iex> shard == Fairway.Fairness.ShuffleShard.shard(shards, "acme")
      true
      iex> length(shard)
      2
  """
  @spec shard(t(), Fairness.tenant()) :: [Fairness.slot()]
  def shard(%__MODULE__{slots: slots, shard_size: shard_size, seed: seed}, tenant) do
    0..(slots - 1)
    |> Enum.sort_by(&score(seed, &1, tenant))
    |> Enum.take(shard_size)
    |> Enum.sort()
  end

  defp score(seed, slot, tenant) do
    <<score::64, _rest::binary>> =
      :crypto.hash(:sha256, <<seed::signed-64, slot::32, tenant::binary>>)

    score
  end
end
