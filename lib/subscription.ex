defmodule Subscription do
  @enforce_keys [:id, :endpoint, :delay, :devices]
  defstruct [:id, :endpoint, :delay, :devices]
end
