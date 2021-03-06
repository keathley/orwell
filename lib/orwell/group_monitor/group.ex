defmodule Orwell.GroupMonitor.Group do
  @moduledoc """
  Monitors a single consumer group. A circular buffer is maintained for each
  topic partition combination. Evaluation of a consumer's status is done after
  receiving each offset commit message. If a consumer's status changes this
  information is broadcast to the notifier which can be used to trigger or heal
  alerts.
  """
  use GenServer

  alias Orwell.BrokerMetadata
  alias Orwell.GroupMonitor.GroupRegistry
  alias Orwell.Window
  alias Orwell.Notification

  require Logger

  def start_link(group_id) do
    GenServer.start_link(__MODULE__, group_id, name: via_tuple(group_id))
  end

  def store_offset_commit(group, offset_commit) do
    GenServer.cast(group, {:store, offset_commit})
  end

  def get_window(group) do
    GenServer.call(via_tuple(group), :get_window)
  end

  def details(group) do
    GenServer.call(group, :get_details)
  end

  def store_memberships(group, members) do
    GenServer.cast(group, {:store_members, members})
  end

  def init(group_id) do
    schedule_check()

    data = %{
      group_id: group_id,
      assignments: [],
      windows: %{},
      intervals: 10,
    }

    {:ok, data}
  end

  def handle_call(:get_details, _from, data) do
    partitions =
      data.windows
      |> Enum.map(fn {{topic, partition}, window} ->
        offset = Window.current_offset(window)

        %{
          topic: topic,
          partition: partition,
          offset: offset,
          lag: head_offset(topic, partition) - offset,
          status: Window.current_status(window)
        }
      end)

    assignments =
      data.assignments
      |> Enum.map(fn a ->
        a
        |> Map.take([:client_host, :client_id])
        |> Map.put(:topics, a.assignment.topics)
      end)

    details = %{
      id: data.group_id,
      assignments: assignments,
      partitions: partitions
    }

    {:reply, details, data}
  end

  def handle_call(:get_window, _from, data) do
    {:reply, data.windows, data}
  end

  def handle_cast({:store, %{offset: offset, timestamp: ts}=oc}, data) do
    # Get the existing window for our topic+partition
    window = Map.get(data.windows, key(oc), Window.new(data.intervals))

    window =
      window
      |> Window.insert(offset, ts, head_offset(key(oc)))

    new_data = put_in(data, [:windows, key(oc)], window)

    measurements = %{
      lag: Window.current_lag(window)
    }

    metadata = %{
      group_id: data.group_id,
      topic: oc.topic,
      partition: oc.partition,
    }

    :telemetry.execute([:orwell, :consumer_group, :lag], measurements, metadata)

    {:noreply, new_data}
  end

  def handle_cast({:store_members, members}, data) do
    new_data = %{data | assignments: members}

    {:noreply, new_data}
  end

  def handle_info(:check_status, data) do
    windows =
      data.windows
      |> Enum.map(fn window -> check_status(window, data.group_id) end)
      |> Enum.into(%{})

    schedule_check()

    {:noreply, %{data | windows: windows}}
  end

  def handle_info(_msg, data) do
    {:noreply, data}
  end

  defp check_status({{topic, partition}, window}, group_id) do
    case Window.update_status(window, now(), head_offset(topic, partition)) do
      # If we haven't changed status do nothing
      {status, status, window} ->
        Logger.debug(fn -> "Status has not changed: #{group_id}, status: #{status}" end)
        {{topic, partition}, window}

      {_old_status, new_status, window} ->
        measurements = %{
          lag: Window.current_lag(window)
        }

        metadata = %{
          group_id: group_id,
          status: new_status,
          topic: topic,
          partition: partition,
        }

        :telemetry.execute([:orwell, :consumer_group, :status_change], measurements, metadata)
        {{topic, partition}, window}
    end
  end

  defp via_tuple(group_id) do
    {:via, Registry, {GroupRegistry, group_id}}
  end

  defp key(%{topic: t, partition: p}), do: {t, p}

  defp head_offset({topic, partition}), do: head_offset(topic, partition)

  defp head_offset(topic, partition), do: BrokerMetadata.offset(topic, partition)

  defp schedule_check do
    Process.send_after(self(), :check_status, 5_000)
  end

  defp now, do: DateTime.utc_now() |> DateTime.to_unix(:millisecond)
end
