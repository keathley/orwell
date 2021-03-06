defmodule Orwell.OffsetConsumer do
  @moduledoc """
  Consumes messages from the __consumer_offsets topic and redirects messages
  to offset and metadata storage for reporting.
  """

  alias Orwell.OffsetConsumer.Parser
  alias Orwell.OffsetConsumer.Parser.{
    Tombstone,
    GroupMetadata,
    OffsetCommit,
  }

  require Logger

  @topic "__consumer_offsets"

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent,
      shutdown: 500
    }
  end

  def start_link(client_id) do
    group_config = [
      offset_commit_policy: :commit_to_kafka_v2,
      offset_commit_interval_seconds: 5,
    ]

    group_id = "orwell-offset-consumer-group"

    consumer_config = [
      begin_offset: :earliest
    ]

    :brod.start_link_group_subscriber(
      client_id,
      group_id,
      [@topic],
      group_config,
      consumer_config,
      __MODULE__,
      []
    )
  end

  def init(_topic, _args) do
    {:ok, []}
  end

  def handle_message(_topic, _partition, message, state) do
    {:kafka_message, _offset, key, value, _ts_type, _ts, _headers} = message

    with {:ok, msg} <- Parser.parse(key, value) do
      case msg do
        %Tombstone{} ->
          Logger.debug("Skipping Tombstone")

        %OffsetCommit{}=oc ->
          Logger.debug("Offset Commit")
          Orwell.GroupMonitor.store_offset_commit(oc)

        %GroupMetadata{}=gm ->
          Logger.debug("Group Metadata")
          Orwell.GroupMonitor.store_memberships(gm.group, gm.members)

        _ ->
          Logger.error("Unknown log message type")
      end
    else
      {:error, error, rest} ->
        Logger.error(fn -> "Error decoding: #{error}, #{rest}" end)

      error ->
        Logger.error("Unhandled error in decoder: #{inspect error}")
    end

    {:ok, :ack, state}
  end
end
