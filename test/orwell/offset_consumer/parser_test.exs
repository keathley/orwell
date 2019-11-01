defmodule Orwell.OffsetConsumer.ParserTest do
  use ExUnit.Case, async: true

  alias Orwell.OffsetConsumer.Parser

  # TODO - Write real tests here

  test "parses tombstones" do
    key = <<0, 2, 0, 22, 116, 101, 115, 116, 95, 99, 111, 109, 109, 101, 110, 116, 115, 45, 99, 111, 110, 115, 117, 109, 101, 114>>
    value = ""

    {<<0, 1, 0, 29, 100, 101, 118, 101, 108, 111, 112, 109, 101, 110, 116, 95, 99,
   111, 109, 109, 101, 110, 116, 115, 45, 99, 111, 110, 115, 117, 109, 101, 114,
   0, 13, 99, 111, 109, 109, 101, 110, 116, 115, 95, 116, 101, 115, 116, 0, 0,
   0, 0>>, ""}

    {<<0, 2, 0, 29, 100, 101, 118, 101, 108, 111, 112, 109, 101, 110, 116, 95, 99,
   111, 109, 109, 101, 110, 116, 115, 45, 99, 111, 110, 115, 117, 109, 101,
   114>>, ""}

    assert Parser.parse(key, value)
  end

  test "parses version 0 and 1 keys" do
    key = <<0, 1, 0, 33, 116, 101, 115, 116, 95, 114, 101, 97, 99, 116, 105, 111, 110, 115, 45, 97, 103, 103, 114, 101, 103, 97, 116, 101, 45, 99, 111, 110, 115, 117, 109, 101, 114, 0, 24, 114, 101, 97, 99, 116, 105, 111, 110, 115, 95, 97, 103, 103, 114, 101, 103, 97, 116, 101, 95, 116, 101, 115, 116, 0, 0, 0, 0>>

    value = <<0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 101, 144, 93, 235, 160, 0, 0, 1, 101, 149, 132, 71, 160>>

    assert Parser.parse(key, value)

    key = <<0, 2, 0, 33, 116, 101, 115, 116, 95, 114, 101, 97, 99, 116, 105, 111, 110, 115, 45, 97, 103, 103, 114, 101, 103, 97, 116, 101, 45, 99, 111, 110, 115, 117, 109, 101, 114>>
    value = <<0, 1, 0, 8, 99, 111, 110, 115, 117, 109, 101, 114, 0, 0, 0, 4, 255, 255, 255, 255, 0, 0, 0, 0>>

    assert Parser.parse(key, value)
  end

  test "parses version 2 keys" do
    key = <<0, 2, 0, 33, 116, 101, 115, 116, 95, 114, 101, 97, 99, 116, 105, 111, 110, 115, 45, 97, 103, 103, 114, 101, 103, 97, 116, 101, 45, 99, 111, 110, 115, 117, 109, 101, 114>>
    value = <<0, 1, 0, 8, 99, 111, 110, 115, 117, 109, 101, 114, 0, 0, 0, 4, 255, 255, 255, 255, 0, 0, 0, 0>>

    assert Parser.parse(key, value)
  end

  test "parses group metadata" do
    value = <<0, 2, 0, 8, 99, 111, 110, 115, 117, 109, 101, 114, 0, 0, 0, 30, 255, 255, 255, 255, 0, 0, 1, 104, 89, 219, 2, 156, 0, 0, 0, 0>>

    assert %{} = Parser.parse_group_metadata_value(value)
  end

  test "returns errors" do
    # value = <<
    key = <<0, 2, 0, 22, 115, 116, 97, 103, 45, 99, 111, 110, 110, 101, 99, 116, 45, 119, 111, 114, 107, 101, 114, 45, 115, 56>>
    value = <<0, 1, 0, 7, 99, 111, 110, 110, 101, 99, 116, 0, 0, 0, 6, 0, 7, 100, 101, 102, 97, 117, 108, 116, 0, 46, 99, 111, 110, 110, 101, 99, 116, 45, 49, 45, 50, 102, 100, 100, 50, 52, 52, 48, 45, 100, 48, 56, 98, 45, 52, 48, 100, 56, 45, 98, 49, 49, 55, 45, 101, 99, 52, 49, 52, 53, 97, 57, 50, 56, 57, 52, 0, 67>>
    assert %{} = Parser.parse(key, value)
  end
end
