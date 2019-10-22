defmodule Orwell.WindowTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Orwell.Window

  def offsets do
    bind(list_of(integer(), length: 10), fn list ->
      constant(Enum.scan(list, 0, fn a, b -> a + b end))
    end)
  end

  def good_window do
    bind(offsets(), fn _list ->
      constant(true)
    end)
  end

  describe "status/1" do
    test "status is :ok if lag is 0 at any point during the window" do
      window =
        Window.new()
        |> Window.insert(0, 0, 10)
        |> Window.insert(10, 1, 10)
        |> Window.insert(10, 2, 15)
        |> Window.insert(10, 3, 20)

      assert Window.status(window, 3, 20) == :ok
    end

    test "is :stalled when consumer offsets aren't increasing but lag is" do
      window =
        Window.new()
        |> Window.insert(10, 0, 11)
        |> Window.insert(10, 1, 15)
        |> Window.insert(10, 2, 20)
        |> Window.insert(10, 3, 25)

      assert Window.status(window, 3, 25) == :stalled
    end

    test "is :lagging if offsets are increasing but lag is fixed or increasing" do
      window =
        Window.new()
        |> Window.insert(0,  0, 10)
        |> Window.insert(5,  1, 15)
        |> Window.insert(10, 2, 25)
        |> Window.insert(10, 3, 35)

      assert Window.status(window, 3, 35) == :lagging
    end

    test "is :stopped when the consumer hasn't comitted an offset recently" do
      window =
        Window.new()
        |> Window.insert(0, 0, 10)
        |> Window.insert(10, 1, 10)
        |> Window.insert(10, 2, 15)
        |> Window.insert(10, 3, 20)

      assert Window.status(window, 5, 20) == :ok
      assert Window.status(window, 7, 20) == :stopped

      window =
        Window.new()
        |> Window.insert(0, 60, 10)
        |> Window.insert(5, 120, 10)
        |> Window.insert(6, 180, 12)
        |> Window.insert(7, 240, 20)
        |> Window.insert(8, 300, 20)

      assert Window.status(window, 320, 20) == :ok
      assert Window.status(window, 1200, 20) == :stopped

      window =
        Window.new()
        |> Window.insert(0, 60, 10)
        |> Window.insert(5, 120, 10)
        |> Window.insert(6, 180, 12)
        |> Window.insert(7, 240, 20)
        |> Window.insert(20, 300, 20)

      # This is ok even after a long duration because the consumer has caught up to whatever the head offset is.
      assert Window.status(window, 320, 20) == :ok
      assert Window.status(window, 1200, 20) == :ok
    end
  end
end
