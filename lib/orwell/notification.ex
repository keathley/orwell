defmodule Orwell.Notification do
  @moduledoc """
  This module provides an api for sending notifications to external systems.
  It is a stateful part of the system and understands trigger recovery
  messages.
  """
  import Norm

  alias Orwell.Window

  require Logger

  def send(status, group_id, topic, partition) do
    status = conform!(status, Window.statuses())

    case status do
      :lagging ->
        Logger.warn(fn -> "#{group_id} on topic: #{topic} partition: #{partition} is lagging" end)

      :ok ->
        Logger.info(fn -> "#{group_id} on topic: #{topic} partition: #{partition} is Ok" end)

      status ->
        Logger.error(fn -> "#{group_id} on topic: #{topic} partition: #{partition} is #{status}" end)
    end
  end
end
