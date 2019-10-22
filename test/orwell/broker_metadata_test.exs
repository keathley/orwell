defmodule Orwell.BrokerMetadataTest do
  use Orwell.Case, async: false

  alias Orwell.{
    BrokerMetadata,
    Producer,
  }

  test "can get the latest offset for a topic partition" do
    Producer.start("orwell_test")

    {:ok, offset} = Producer.produce("orwell_test", "key", "offset-test")

    eventually(fn ->
      assert BrokerMetadata.offset("orwell_test", 0) == offset
    end)
  end
end
