defmodule Orwell.Producer do
  @moduledoc """
  Producer used for testing.
  """

  def start(topic) do
    :brod.start_producer(Orwell.KafkaClient, topic, [])
  end

  def produce(topic, key, value, partition \\ 0) do
    :brod.produce_sync_offset(Orwell.KafkaClient, topic, partition, key, value)
  end
end
