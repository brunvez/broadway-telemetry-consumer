defmodule Store do
  use GenServer

  @table_name :subscription_readings

  def start_link do
    GenServer.start_link(__MODULE__, [], opts)
  end

  def add_reading(%Subscription{}, reading), do: :ok

  ## Server

  def init(state) do
    :ets.new(@table_name, [:named_table, :privatem read_concurrency: true])
  end
end
