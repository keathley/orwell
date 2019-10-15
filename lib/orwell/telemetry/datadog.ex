defmodule Orwell.Telemetry.DataDog do
  use Statix, runtime_config: true

  def start do
    connect()
  end

  def configure do
    if Application.get_env(:statix, :host) == :none do
      Application.put_env(:statix, :host, container_addr())
    end
  end

  def handle_event([:orwell, :consumer_group, :lag], measurements, meta, _) do
    lag = measurements.lag

    tags = [
      "topic:#{meta.topic}",
      "partition:#{meta.partition}",
      "group_id:#{meta.group_id}"
    ]

    gauge("orwell.consumer_group.lag", lag, tags: tags)
  end

  def handle_event([:orwell, :consumer_group, :status_change], measurements, meta, _) do
    case meta.status do
      :stopped ->
        msg = "consumer group: #{meta.group_id} has stopped"
        event("orwell.consumer_group.status", msg, alert_type: :error)

      :ok ->
        msg = "consumer group: #{meta.group_id} has recovered"
        event("orwell.consumer_group.status", msg, alert_type: :info)

      :stalled ->
        msg = "consumer group: #{meta.group_id} has stalled on topic: #{meta.topic}, partition: #{meta.partition}"
        event("orwell.consumer_group.status", msg, alert_type: :error)

      :lagging ->
        msg = "consumer group: #{meta.group_id} is lagging on topic: #{meta.topic}, partition: #{meta.partition}"
        event("orwell.consumer_group.status", msg, alert_type: :warning)
    end
  end

  defp container_addr do
    'ip route | awk \'/default/{print $3}\''
    |> :os.cmd()
    # "Deletes the trailing \n from the char list
    |> :string.strip(:right, ?\n)
  end
end
