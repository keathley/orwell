defmodule Orwell.Telemetry.Logger do
  require Logger

  def handle_event([:orwell, :consumer_group, :lag], measurements, meta, _) do
    lag = measurements.lag

    tags = [
      "topic:#{meta.topic}",
      "partition:#{meta.partition}",
      "group_id:#{meta.group_id}"
    ]

    Logger.debug(fn ->
      "group_id: #{meta.group_id}, topic: #{meta.topic}, partition: #{meta.partition}, lag: #{measurements.lag}"
    end)
  end

  def handle_event([:orwell, :consumer_group, :status_change], measurements, meta, _) do
    case meta.status do
      :stopped ->
        Logger.error(fn ->
          "consumer group: #{meta.group_id} has stopped"
        end)

      :ok ->
        Logger.info(fn ->
          "consumer group: #{meta.group_id} has recovered"
        end)

      :stalled ->
        Logger.error(fn ->
          "consumer group: #{meta.group_id} has stalled on topic: #{meta.topic}, partition: #{meta.partition}"
        end)

      :lagging ->
        Logger.warn(fn ->
          "consumer group: #{meta.group_id} is lagging on topic: #{meta.topic}, partition: #{meta.partition}"
        end)
    end
  end
end
