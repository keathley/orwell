defmodule OrwellTest do
  use ExUnit.Case
  doctest Orwell

  test "greets the world" do
    assert Orwell.hello() == :world
  end
end
