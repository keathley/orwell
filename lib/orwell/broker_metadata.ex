defmodule Orwell.BrokerMetadata do
  @moduledoc """
  This module provides an interface for polling the broker metadata. This
  information is used to keep track of maximum offsets, which brokers own which
  partitions, etc. All of the relevant information is stored in ETS for fast
  lookups in other parts of the system. Polling for offsets is done at a set interval.
  """
  use Supervisor

  alias Orwell.BrokerMetadata.Refresher

  def start_link(brokers) do
    Supervisor.start_link(__MODULE__, brokers, name: __MODULE__)
  end

  def init(brokers) do
    :topic_offsets = :ets.new(:topic_offsets, [:set, :public, :named_table])

    children = [
      {DynamicSupervisor, name: PartitionSupervisor, strategy: :one_for_one},
      {Registry, [keys: :unique, name: PartitionRegistry]},
      {Refresher, brokers},
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def set_offset(topic, partition, offset) do
    true = :ets.insert(:topic_offsets, {key(topic, partition), offset})
  end

  def watch_offset(topic, partition) do
    DynamicSupervisor.start_child(PartitionSupervisor,)
  end

  @doc """
  Returns the maximum offset that we've seen for a given topic and partition.
  If no offset is available then 0 is returned.
  """
  def offset(topic, partition) when is_binary(topic) and is_integer(partition) do
    case :ets.lookup(:topic_offsets, key(topic, partition)) do
      [] -> nil
      [offset] -> offset
    end
  end

  defp key(topic, partition), do: {topic, partition}
end

