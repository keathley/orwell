defmodule Orwell.Application do
  @moduledoc false

  use Application

  @kafka_client Orwell.KafkaClient

  def start(_type, _args) do
    children = [
      {Orwell.BrokerMetadata, kafka_endpoints()},
      Orwell.GroupMonitor,
      {Orwell.OffsetConsumer, @kafka_client},
    ]

    :ok = :brod.start_client(kafka_endpoints(), @kafka_client)

    opts = [strategy: :one_for_one, name: Orwell.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp kafka_endpoints do
    [{'localhost', 9092}]
  end
end
