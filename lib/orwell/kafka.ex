defmodule Orwell.Kafka do
  @moduledoc """
  Provides convenience functions for interacting with kafka.
  """

  @kafka_client Orwell.KafkaClient

  def client_name, do: @kafka_client

  def kafka_brokers do
    brokers = System.get_env("ORWELL_KAFKA_BROKERS")

    if brokers do
      brokers
      |> String.split(",")
      |> Enum.map(fn broker ->
        [host, port] = String.split(broker, ":")
        {host, String.to_integer(port)}
      end)
    else
      [{'localhost', 9092}]
    end
  end
end
