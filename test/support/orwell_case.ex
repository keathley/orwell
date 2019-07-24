defmodule Orwell.Case do
  use ExUnit.CaseTemplate

  using do
    quote do
      import Orwell.TestHelpers
    end
  end
end
