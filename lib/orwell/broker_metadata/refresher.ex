defmodule Orwell.BrokerMetadata.Refresher do
  @moduledoc """
  Singleton process that periodically refreshes all topic metadata from the
  brokers. When its done retrieving this information it creates a process
  to monitor the offset for each individual topic and partition combination.
  The refresher maintains a list of partitions for a given topic so if partitions
  are added or removed we can produce a delta and update the partition monitors
  accordingly.
  """
  use GenServer

  alias Orwell.BrokerMetadata

  require Logger

  @refresh_interval 60_000

  def start_link(endpoints) do
    GenServer.start_link(__MODULE__, endpoints, name: __MODULE__)
  end

  def init(endpoints) do
    Process.send_after(self(), :fetch_metadata, 0)

    data = %{
      endpoints: endpoints,
      partitions: %{},
      brokers: [],
    }

    {:ok, data}
  end

  def handle_info(:fetch_metadata, data) do
    Process.send_after(self(), :fetch_metadata, @refresh_interval)

    case :brod.get_metadata(data.endpoints) do
      {:ok, metadata} ->
        data = update_metadata(data, metadata)
        for {topic_partition, _} <- data.partitions do
          BrokerMetadata.watch_offset(topic_partition)
        end

        {:noreply, data}

      {:error, error} ->
        Logger.warn(fn ->
          "Could not fetch metadata for topic: #{data.topic}: #{inspect error}"
        end)
        {:noreply, data}
    end
  end

  defp update_metadata(data, metadata) do
    brokers = metadata.brokers

    partitions =
      for t <- metadata.topic_metadata,
          p <- t.partition_metadata,
      do: {{t.topic, p.partition}, %{leader: p.leader, replicas: p.replicas}},
      into: %{}

    %{data | brokers: brokers, partitions: partitions}
  end
end
