defmodule Orwell.BrokerMetadata.PartitionMonitor do
  use GenServer, restart: :transient

  alias Orwell.BrokerMetadata
  alias Orwell.BrokerMetadata.PartitionRegistry

  require Logger

  @offset_refresh_interval 10_000

  def start_link(brokers, {topic, partition}) do
    data = %{
      brokers: brokers,
      topic: topic,
      partition: partition
    }

    GenServer.start_link(__MODULE__, data, name: via_tuple(topic, partition))
  end

  def init(data) do
    Logger.debug("Starting offset monitor for #{data.topic}, #{data.partition}")

    Process.send_after(self(), :fetch_offset, 0)

    {:ok, data}
  end

  def handle_info(:fetch_offset, data) do
    case :brod.resolve_offset(data.brokers, data.topic, data.partition) do
      {:ok, offset} ->
        BrokerMetadata.set_offset(data.topic, data.partition, offset)

      {:error, reason} ->
        Logger.warn(fn ->
          "Fetching offset for #{data.topic}, #{data.partition} failed: #{inspect reason}"
        end)
    end

    Process.send_after(self(), :fetch_offset, @offset_refresh_interval)
    {:noreply, data}
  end

  defp via_tuple(topic, partition) do
    {:via, Registry, {PartitionRegistry, {topic, partition}}}
  end
end
