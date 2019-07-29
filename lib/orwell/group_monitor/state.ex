defmodule Orwell.GroupMonitor.GroupState do
  alias Orwell.Window

  def new(intervals) do
    %{intervals: intervals, topic_partitions: %{}}
  end

  def update(state, offset) do
    topic_partitions = update_offset(state, offset)

    %{state | topic_partitions: topic_partitions}
  end

  defp key(offset) do
    {offset.topic, offset.partition}
  end

  defp update_offset(state, offset) do
    case Map.get(state.topic_partitions, key(offset)) do
      nil ->
        new_window =
          state.intervals
          |> Window.new()
          |> Window.update(offset)

        Map.put(state.topic_partitions, key(offset), new_window)

      window ->
        Map.put(state.topic_partitions, key(offset), Window.update(window, offset))
    end
  end
end
