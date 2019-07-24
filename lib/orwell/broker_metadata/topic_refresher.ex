defmodule Orwell.BrokerMetadata.TopicRefresher do
  @moduledoc """
  Singleton process that periodically refreshes all topic metadata from the
  brokers. When its done retrieving this information it creates a process
  to monitor the offset for each individual topic and partition combination.
  """
  use GenServer

  def start_link(brokers) do
    GenServer.start_link(__MODULE__, brokers, name: __MODULE__)
  end

  def init(
end

