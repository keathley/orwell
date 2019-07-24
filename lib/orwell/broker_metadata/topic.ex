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

    {:ok, data, {:continue, :fetch_topic_data}}
  end

  def handle_info(:fetch_offsets, data) do
    update_offsets(data)
    {:noreply, data}
  end

  def handle_continue(:fetch_topic_data, data) do
    Logger.debug(fn -> "Fetching topic metadata: #{data.topic}" end)

    case :brod.get_metadata(kafka_endpoints(), [data.topic]) do
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

  defp update_offsets(data) do
    IO.inspect(data, label: "Updating offsets")
    data.partitions
    |> Enum.map(fn {id, _partition} -> Task.async(fn -> {id, :brod.resolve_offset(kafka_endpoints, data.topic, id)} end) end)
    |> Enum.map(fn task -> Task.await(task) end)
    |> Enum.filter(fn {result, _} -> result == :ok  end)
    |> Enum.map(fn {:ok, {id, offset}} -> {id, offset} end)
    |> Enum.each(fn {id, offset} -> BrokerMetadata.set_offset(data.topic, id, offset) end)
  end

  defp kafka_endpoints do
    [{'localhost', 9092}]
  end
end

