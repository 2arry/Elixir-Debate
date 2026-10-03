defmodule Fairway.Fairness.Throttle do
  @moduledoc """
  Per-tenant limits on concurrency and on start rate. Configured as
  `mode: throttle`.

      throttle:
        max_concurrency: 2   # jobs of one tenant running at once
        rate: 10             # job starts per second, per tenant
        burst: 5             # starts a tenant may make at once after being idle

  Each tenant has a token bucket holding at most `burst` tokens and refilled at
  `rate` per second. Starting a job takes a token. Tenants that are under both
  limits are served round-robin.

  ## Guarantee

  For every tenant, at every moment:

    * at most `max_concurrency` of its jobs are running, and
    * in any window of `w` seconds it has started at most `burst + rate * w`
      jobs.

  ## Limits

    * The bucket meters the scheduler's decision to start a job. The job's
      `perform/1` is entered a message hop later.
    * Buckets live in the leader's memory. After a failover every bucket is
      full again, so a tenant can get one extra burst.
    * The limits are the same for every tenant.
    * Bucket arithmetic is in whole millionths of a token per millisecond, so a
      `rate` is honoured to three decimal places.

  A bucket that has refilled completely is forgotten, so the state holds only
  tenants that have started a job recently.
  """

  @behaviour Fairway.Fairness

  alias Fairway.Fairness

  # Token arithmetic is done in integers so that the limits hold exactly.
  @token 1_000_000

  @enforce_keys [:max_concurrency, :refill_per_ms, :capacity]
  defstruct [:max_concurrency, :refill_per_ms, :capacity, :last, buckets: %{}]

  @type t :: %__MODULE__{
          max_concurrency: pos_integer(),
          refill_per_ms: pos_integer(),
          capacity: pos_integer(),
          last: Fairness.tenant() | nil,
          buckets: %{optional(Fairness.tenant()) => {tokens :: integer(), at :: integer()}}
        }

  @impl true
  def init(%{max_concurrency: max_concurrency, rate: rate, burst: burst}) do
    %__MODULE__{
      max_concurrency: max_concurrency,
      refill_per_ms: max(1, round(rate * @token / 1000)),
      capacity: burst * @token
    }
  end

  @impl true
  def select(_state, %{ready: []}), do: :idle
  def select(_state, %{free: []}), do: :idle

  def select(%__MODULE__{} = state, %{ready: ready, running: running, free: [slot | _], now: now}) do
    busy = running |> Map.values() |> Enum.frequencies()

    under_cap =
      ready
      |> Fairness.ring(state.last)
      |> Enum.filter(&(Map.get(busy, &1, 0) < state.max_concurrency))

    case Enum.find(under_cap, &(tokens(state, &1, now) >= @token)) do
      nil -> wait(state, under_cap, now)
      tenant -> {:run, tenant, slot}
    end
  end

  @impl true
  def started(%__MODULE__{} = state, tenant, _slot, now) do
    buckets =
      state.buckets
      |> Map.put(tenant, {tokens(state, tenant, now) - @token, now})
      |> Map.reject(fn {_tenant, {tokens, at}} ->
        refilled(state, tokens, at, now) == state.capacity
      end)

    %{state | buckets: buckets, last: tenant}
  end

  # Every remaining tenant is at its concurrency cap. A finishing job will
  # prompt the scheduler to ask again, so there is nothing to wait for.
  defp wait(_state, [], _now), do: :idle

  defp wait(state, tenants, now) do
    tenants
    |> Enum.map(&ms_until_token(state, &1, now))
    |> Enum.min()
    |> then(&{:wait, &1})
  end

  defp ms_until_token(state, tenant, now) do
    missing = @token - tokens(state, tenant, now)
    max(1, div(missing + state.refill_per_ms - 1, state.refill_per_ms))
  end

  defp tokens(state, tenant, now) do
    case state.buckets do
      %{^tenant => {tokens, at}} -> refilled(state, tokens, at, now)
      _full -> state.capacity
    end
  end

  defp refilled(state, tokens, at, now) do
    min(state.capacity, tokens + max(0, now - at) * state.refill_per_ms)
  end
end
