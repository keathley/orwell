defmodule Orwell.BrokerMetadata do
  @moduledoc """
  This module provides an interface for polling the broker metadata. This
  information is used to keep track of maximum offsets, which brokers own which
  partitions, etc. All of the relevant information is stored in ETS for fast
  lookups in other parts of the system. Polling for offsets is done at a set interval.
  """
  use Supervisor

  def start_link(init_arg) do
    Supervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  def init(_init_arg) do
    children = [
      Storage,
      OffsetPoller,
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

