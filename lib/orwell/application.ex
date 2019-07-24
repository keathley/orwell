defmodule Orwell.Application do
  @moduledoc false

  use Application

  alias Orwell.Parser

  @kafka_client Orwell.KafkaClient

  def start(_type, _args) do
    children = [
      {Orwell.BrokerInformation, kafka_endpoints()},
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
