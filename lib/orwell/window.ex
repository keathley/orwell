defmodule Orwell.Window do
  @moduledoc """
  Tracks a group of topics and partitions over a given window.
  """
  import Norm

  alias CircularBuffer, as: Buffer

  def statuses, do: one_of([
    :ok,
    :stopped,
    :stalled,
    :lagging
  ])

  def new(intervals \\ 10) do
    %{buffer: Buffer.new(intervals)}
  end

  @doc """
  insert adds a new interval to the window. It takes the current offset,
  the timestamp, and the head pointer (the farthest offset we know about).
  """
  def insert(window, offset, timestamp, head) do
    interval = {offset, timestamp, calculate_lag(head, offset)}
    update_in(window, [:buffer], &Buffer.insert(&1, interval))
  end

  def update(window, %{offset: offset, timestamp: ts, head: head}) do
    interval = {offset, ts, calculate_lag(head, offset)}

    update_in(window, [:buffer], &Buffer.insert(&1, interval))
  end

  def status(window, time_now, head) do
    list = Buffer.to_list(window.buffer)

    cond do
      # Check to see if we've been waiting for a longer period of time than
      # the difference between the most recent offset and the oldest offset.
      stopped?(window.buffer, time_now, head) ->
        :stopped

      # Bail out if there are any periods with 0 lag.
      Enum.any?(list, fn {_, _, lag} -> lag == 0 end) ->
        :ok

      # If we're here then we already know that we have lag since the above
      # check ensures that we had at least 1 period without lag
      offset_fixed?(list) ->
        :stalled

      # Same as above we know that there must be _some_ lag if we get to this
      # point. We need to see if the offsets are still increasing which would
      # indicate that the consumer can't keep up but isn't stalled out.
      offset_increasing?(list) and lag_increasing?(list) ->
        :lagging

      # If we got all the way here there's an error in our logic so return an
      # error.
      true ->
        :ok
    end
  end

  defp lag_increasing?(list) do
    list
    |> Enum.map(fn {_, _, lag} -> lag end)
    |> increasing?
  end

  defp offset_increasing?(list) do
    list
    |> Enum.map(fn {offset, _, _} -> offset end)
    |> increasing?
  end

  defp increasing?(list) do
    case list do
      [a, b | rest] when a > b ->
        false

      [_a | rest] ->
        increasing?(rest)

      [] ->
        true
    end
  end

  defp offset_fixed?(list) do
    offsets =
      list
      |> Enum.map(fn {offset, _, _} -> offset end)
      |> Enum.uniq()

    Enum.count(offsets) == 1
  end

  defp calculate_lag(head, offset) do
    max(head - offset, 0)
  end

  defp stopped?(buffer, time_now, head) do
    {newest_offset, newest_time, _} = Buffer.newest(buffer)
    {_, oldest_time, _} = Buffer.oldest(buffer)
    newest_offset < head && (time_now - newest_time) > (newest_time - oldest_time)
  end
end
