defmodule Orwell.BrokerMetadata.Topic do
  @moduledoc """
  Periodically refreshes the count of partitions and each partition offset for
  a given topic.
  """
  use GenServer

  import Access, only: [at: 1, all: 0]

  require Logger

  @offset_refresh_timeout 10_000

  def start_link(topic) do
    GenServer.start_link(__MODULE__, topic)
  end

  def init(topic) do
    data = %{
      topic: topic,
      partitions: %{},
      brokers: [],
    }

    {:ok, data}
  end

  defp kafka_endpoints do
    [{'localhost', 9092}]
  end
end

