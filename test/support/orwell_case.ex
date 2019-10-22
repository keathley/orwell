defmodule Orwell.Case do
  @moduledoc """
  Default test case for all orwell tests
  """
  use ExUnit.CaseTemplate

  using do
    quote do
      import Orwell.TestHelpers
    end
  end
end
