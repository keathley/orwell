defmodule Orwell.GroupMonitor.Group do
  @moduledoc """
  Monitors a single consumer group. A circular buffer is maintained for each
  topic partition combination. Evaluation of a consumer's status is done after
  receiving each offset commit message. If a consumer's status changes this
  information is broadcast to the notifier which can be used to trigger or heal
  alerts.
  """
  use GenServer

  alias Cbuf.Map, as: Buf
  alias Orwell.BrokerMetadata
  alias Orwell.GroupMonitor.GroupRegistry
  alias Orwell.GroupMonitor.GroupState
  alias Orwell.Window
  alias Orwell.Notification

  def start_link(group_id) do
    GenServer.start_link(__MODULE__, group_id, name: via_tuple(group_id))
  end

  def store_offset_commit(group, offset_commit) do
    GenServer.cast(group, {:store, offset_commit})
  end

  def get_window(group) do
    GenServer.call(via_tuple(group), :get_window)
  end

  def init(group_id) do
    schedule_check()

    data = %{
      group_id: group_id,
      windows: %{},
      intervals: 10,
    }

    {:ok, data}
  end

  def handle_call(:get_window, _from, data) do
    {:reply, data.windows, data}
  end

  def handle_cast({:store, offset_commit}, data) do
    # Get the existing window for our topic+partition
    window = Map.get(data.windows, key(offset_commit), Window.new(data.intervals))

    # Create a new interval with the consumer's offset, timestamp, and head offset
    interval = %{
      offset: offset_commit.offset,
      timestamp: offset_commit.timestamp,
      head: head_offset(key(offset_commit))
    }

    new_window = Window.update(window, interval)
    new_data = put_in(data, [:windows, key(offset_commit)], new_window)

    {:noreply, new_data}
  end

  def handle_info(:check_status, data) do
    data.windows
    |> Enum.each(fn window -> check_status(window, data.group_id) end)

    schedule_check()

    {:noreply, data}
  end

  def handle_info(_msg, data) do
    {:noreply, data}
  end

  defp check_status({{topic, partition}, window}, group_id) do
    status = Window.status(window, now(), head_offset(topic, partition))
    Notification.send(group_id, topic, partition, status)
  end

  defp via_tuple(group_id) do
    {:via, Registry, {GroupRegistry, group_id}}
  end

  defp key(%{topic: t, partition: p}), do: {t, p}

  defp head_offset({topic, partition}), do: head_offset(topic, partition)

  defp head_offset(topic, partition), do: BrokerMetadata.offset(topic, partition)

  defp schedule_check() do
    Process.send_after(self(), :check_status, 5_000)
  end

  defp now(), do: DateTime.utc_now() |> DateTime.to_unix()
end
