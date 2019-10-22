defmodule Orwell.GroupMonitor do
  @moduledoc """
  Monitors different consumer groups by topic and partition. Consumer processes
  are started dynamically.
  """
  use Supervisor

  alias Orwell.GroupMonitor.{
    Group,
    GroupRegistry,
    GroupSup,
  }

  require Logger

  def start_link(init_args) do
    Supervisor.start_link(__MODULE__, init_args, name: __MODULE__)
  end

  def store_offset_commit(offset_commit) do
    offset_commit.group
    |> get_group_pid
    |> Group.store_offset_commit(offset_commit)
  end

  def window_for(group_id) do
    Group.get_window(group_id)
  end

  def groups do
    # Use match specs to get all keys in the registry
    Registry.select(GroupRegistry, [{{:"$1", :_, :_}, [], [:"$1"]}])
  end

  def group_details(group_id) do
    with {:ok, pid} <- lookup_group(group_id) do
      details = Group.details(pid)
      {:ok, details}
    end
  end

  def init(_init_args) do
    children = [
      {DynamicSupervisor, name: GroupSup, strategy: :one_for_one},
      {Registry, [keys: :unique, name: GroupRegistry]},
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp get_group_pid(group_id) do
    case lookup_group(group_id) do
      :none ->
        case start_group(group_id) do
          {:ok, pid} ->
            Logger.debug("Starting new group monitor")
            pid

          {:error, {:already_started, pid}} ->
            Logger.debug("Looking up existing consumer group")
            pid
        end

      {:ok, pid} ->
        Logger.debug("Found pid in registry")
        pid
    end
  end

  def lookup_group(group_id) do
    case Registry.lookup(GroupRegistry, group_id) do
      [] ->
        :none

      [{pid, _}] ->
        {:ok, pid}
    end
  end

  def start_group(group_id) do
    DynamicSupervisor.start_child(GroupSup, {Group, group_id})
  end
end
