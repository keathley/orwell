defmodule Orwell.Parser do
  def parse(_, ""), do: %{type: :tombstone}
  def parse(key, value) do

    case version(key) do
      {v, rest} when v in [0, 1] ->
        %{key: parse_offset_commit_key(rest), value: parse_offset_commit_value(value)}

      {2, rest} ->
        %{key: parse_group_metadata_key(rest), value: parse_group_metadata_value(value)}
    end
  end

  defp version(<< version :: integer-big-size(16), rest :: binary() >>), do: {version, rest}

  def parse_offset_commit_key(rest) do
    rest
    |> a(:group, string())
    |> a(:topic, string())
    |> a(:partition, int(32))
    |> collect()
  end

  def parse_group_metadata_key(rest) do
    rest
    |> a(:group, string())
    |> collect()
  end

  def parse_offset_commit_value(value) do
    case version(value) do
      {0, rest} ->
        rest
        |> a(:offset, int(64))
        |> a(:metadata, string())
        |> a(:timestamp, int(64))
        |> collect()

      {1, rest} ->
        rest
        |> a(:offset, int(64))
        |> a(:metadata, string())
        |> a(:commit_timestamp, int(64))
        |> a(:expire_timestamp, int(64))
        |> collect()

      {2, rest} ->
        rest
        |> a(:offset, int(64))
        |> a(:metadata, string())
        |> a(:commit_timestamp, int(64))
        |> collect()

      {3, rest} ->
        rest
        |> a(:offset, int(64))
        |> a(:leader_epoch, int(32))
        |> a(:metadata, string())
        |> a(:commit_timestamp, int(64))
        |> collect()
    end
  end

  def parse_group_metadata_value(value) do
    case version(value) do
      {0, rest} ->
        rest
        |> a(:protocol_type, string())
        |> a(:generation, int(32))
        |> a(:protocol, nullable_string())
        |> a(:leader, nullable_string())
        |> a(:members, list_of(parse_member_metadata(0)))
        |> collect()

      {1, rest} ->
        rest
        |> a(:protocol_type, string())
        |> a(:generation, int(32))
        |> a(:protocol, nullable_string())
        |> a(:leader, nullable_string())
        |> a(:members, list_of(parse_member_metadata(1)))
        |> collect()

      {2, rest} ->
        rest
        |> a(:protocol_type, string())
        |> a(:generation, int(32))
        |> a(:protocol, nullable_string())
        |> a(:leader, nullable_string())
        |> a(:current_state_timestamp, int(64))
        |> a(:members, list_of(parse_member_metadata(2)))
        |> collect()

      {3, rest} ->
        rest
        |> a(:protocol_type, string())
        |> a(:generation, int(32))
        |> a(:protocol, nullable_string())
        |> a(:leader, nullable_string())
        |> a(:current_state_timestamp, int(64))
        |> a(:members, list_of(parse_member_metadata(3)))
        |> collect()
    end
  end

  defp parse_member_metadata(version) do
    fn rest ->
      case version do
        0 ->
          rest
          |> a(:member_id, string())
          |> a(:client_id, string())
          |> a(:client_host, string())
          |> a(:session_timeout, int(32))
          |> a(:subscription, bytes())
          |> a(:assignment, bytes())
          |> collect()

        1 ->
          rest
          |> a(:member_id, string())
          |> a(:client_id, string())
          |> a(:client_host, string())
          |> a(:rebalance_timeout, int(32))
          |> a(:session_timeout, int(32))
          |> a(:subscription, bytes())
          |> a(:assignment, bytes())
          |> collect()

        2 ->
          rest
          |> a(:member_id, string())
          |> a(:client_id, string())
          |> a(:client_host, string())
          |> a(:rebalance_timeout, int(32))
          |> a(:session_timeout, int(32))
          |> a(:subscription, bytes())
          |> a(:assignment, bytes())
          |> collect()

        3 ->
          rest
          |> a(:member_id, string())
          |> a(:group_instance_id, nullable_string())
          |> a(:client_id, string())
          |> a(:client_host, string())
          |> a(:rebalance_timeout, int(32))
          |> a(:session_timeout, int(32))
          |> a(:subscription, bytes())
          |> a(:assignment, bytes())
          |> collect()
      end
    end
  end

  defp a(next, key, parser)
  defp a(next, key, parser) when is_binary(next), do: a({next, %{}}, key, parser)
  defp a({:error, e}, _key, _parser), do: {:error, e}
  defp a({next, built}, key, parser) do
    case parser.(next) do
      {:ok, val, rest} ->
        {rest, Map.put(built, key, val)}

      e ->
        e
    end
  end

  defp collect({:error, e}), do: {:error, e}
  defp collect({_, result}), do: result

  defp list_of(parser) do
    fn << length :: integer-size(32), next :: binary >> ->
      parse_list(length, next, [], parser)
    end
  end

  defp parse_list(0, rest, acc, _), do: {:ok, acc, rest}
  defp parse_list(i, rest, parsed, parser) do
    case parser.(rest) do
      {:ok, val, rest} ->
        parse_list(i-1, rest, parsed ++ [val], parser)

      e ->
        e
    end
  end

  defp bytes() do
    fn
      << sz :: integer-size(32), str :: bytes-size(sz), rest :: binary >> ->
        {:ok, str, rest}

      rest ->
        {:error, "Bytes: #{rest}"}
    end
  end

  defp string() do
    fn
      << str_size :: integer-size(16), str :: binary-size(str_size), rest :: binary >> ->
        {:ok, str, rest}

      rest ->
        {:error, rest}
    end
  end

  defp nullable_string() do
    fn
      # If the short value is -1 we just return nil and move on.
      << 255, 255, rest ::binary >> ->
        {:ok, nil, rest}

      << str_size :: integer-size(16), str :: binary-size(str_size), rest :: binary >> ->
        {:ok, str, rest}

      rest ->
        {:error, "Nullable string: #{rest}"}
    end
  end

  def int(size) do
    fn
      << i :: integer-size(size), rest :: binary >> ->
        {:ok, i, rest}

      rest ->
        {:error, "Could not parse as integer: #{rest}"}
    end
  end
end

