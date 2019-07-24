defmodule Orwell.TestHelpers do
  def eventually(f, retries \\ 0) do
    if retries > 10 do
      false
    else
      if f.() do
        true
      else
        :timer.sleep(400)
        eventually(f, retries + 1)
      end
    end
  rescue
    err ->
      if retries == 10 do
        reraise err, __STACKTRACE__
      else
        :timer.sleep(700)
        eventually(f, retries + 1)
      end
  end
end
