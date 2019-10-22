defmodule Orwell.Application do
  @moduledoc false

  use Application

  alias Orwell.Kafka


  def start(_type, _args) do
    children = [
      {Orwell.BrokerMetadata, Kafka.kafka_brokers()},
      Orwell.GroupMonitor,
      {Orwell.OffsetConsumer, Kafka.client_name()},
    ]

    telemetry = telemetry_adapter()
    notifier = notification_adapter()
    logger = Orwell.Telemetry.Logger

    telemetry.configure()
    telemetry.start()

    :ok = :brod.start_client(Kafka.kafka_brokers(), Kafka.client_name())

    :ok = :telemetry.attach(
      "orwell-consumer-telemetry",
      [:orwell, :consumer_group, :lag],
      &telemetry.handle_event/4,
      :unused
    )

    :ok = :telemetry.attach(
      "orwell-notifier",
      [:orwell, :consumer_group, :status_change],
      &notifier.handle_event/4,
      :unused
    )

    :ok = :telemetry.attach_many(
      "orwell-logger",
      [
        [:orwell, :consumer_group, :status_change],
        [:orwell, :consumer_group, :lag],
      ],
      &logger.handle_event/4,
      :unused
    )

    opts = [strategy: :one_for_one, name: Orwell.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp telemetry_adapter do
    Orwell.Telemetry.DataDog
  end

  defp notification_adapter do
    Orwell.Telemetry.DataDog
  end
end
