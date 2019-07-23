defmodule Orwell.Storage do
  @moduledoc """
  Stores information about topics, partitions, offsets, and consumers in order to
  make decisions over a given time period.
  """
  use GenServer
end

