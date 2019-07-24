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

  def start_link(brokers) do
    GenServer.start_link(__MODULE__, brokers, name: __MODULE__)
  end

  def init(brokers) do
    Process.send_after(self(), :fetch_metadata, 0)

    {:ok, %{brokers: brokers}}
  end

  def handle_info(:fetch_metadata, data) do
    case :brod.get_metadata(data.brokers) do
      {:ok, metadata} ->
        data = update_metadata(data, metadata)
        update_offsets(data)
        Process.send_after(self(), :fetch_offsets, @offset_refresh_timeout)
        {:noreply, data}

      {:error, error} ->
        Logger.warn(fn -> "Could not fetch metadata for topic: #{data.topic}" end)
        {:noreply, data}
    end
  end

  defp update_metadata(data, metadata) do
    brokers = get_in(metadata, [:brokers])
    partitions =
      metadata
      |> get_in([:topic_metadata, :partition_metadata])
      |> Enum.map(fn meta -> {meta.partition, %{leader: meta.leader, replicas: meta.replicas, offset: nil}} end)
      |> Enum.into(%{})

    %{data | brokers: brokers, partitions: partitions}
  end
end

