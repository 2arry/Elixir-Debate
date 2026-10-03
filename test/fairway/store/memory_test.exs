defmodule Fairway.Store.MemoryTest do
  use ExUnit.Case, async: true
  use Fairway.StoreContract

  alias Fairway.Store.Memory

  setup do
    %{store: {Memory, start_supervised!({Memory, name: nil})}}
  end
end
