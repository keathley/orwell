defmodule Orwell.OffsetConsumer.Parser do
  @moduledoc """
  Parses internal kafka messages into offset commit messages and groupmetadata
  messages.
  """

  defmodule Member do
    @moduledoc false
    defstruct ~w|
      member_id
      client_id
      client_host
      assignment
    |a
  end

  defmodule GroupMetadata do
    @moduledoc false
    defstruct ~w|
      group
      members
    |a
  end

  defmodule OffsetCommit do
    @moduledoc false
    import Norm

    defstruct ~w|
      group
      topic
      partition
      offset
      timestamp
    |a

    def s do
      schema(%__MODULE__{
        group: spec(is_binary()),
        topic: spec(is_binary()),
        partition: spec(is_integer() and &(&1 >= 0)),
        offset: spec(is_integer() and &(&1 >= 0)),
        timestamp: spec(is_integer() and &(&1 >= 0))
      })
    end
  end

  defmodule Tombstone do
    @moduledoc false
    defstruct []
  end

  def parse(_, ""), do: %Tombstone{}
  def parse(key, value) do
    case version(key) do
      {v, rest} when v in [0, 1] ->
        build_offset_msg(rest, value)

      {2, rest} ->
        build_group_metadata_msg(rest, value)
    end
  end

  defp build_offset_msg(rest, value) do
    key = parse_offset_commit_key(rest)
    value = parse_offset_commit_value(value)

    %OffsetCommit{
      group: key.group,
      topic: key.topic,
      partition: key.partition,
      offset: value.offset,
      timestamp: value.commit_timestamp,
    }
  end

  defp build_group_metadata_msg(rest, value) do
    key = parse_group_metadata_key(rest)
    value = parse_group_metadata_value(value)

    members =
      value.members
      |> Enum.map(fn mem -> struct(Member, mem) end)

    %GroupMetadata{
      group: key.group,
      members: members
    }
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
          |> a(:session_timeout, int(32), :skip)
          |> a(:subscription, bytes(), :skip)
          |> a(:assignment, assignment())

        1 ->
          rest
          |> a(:member_id, string())
          |> a(:client_id, string())
          |> a(:client_host, string())
          |> a(:rebalance_timeout, int(32), :skip)
          |> a(:session_timeout, int(32), :skip)
          |> a(:subscription, bytes(), :skip)
          |> a(:assignment, assignment())

        2 ->
          rest
          |> a(:member_id, string())
          |> a(:client_id, string())
          |> a(:client_host, string())
          |> a(:rebalance_timeout, int(32), :skip)
          |> a(:session_timeout, int(32), :skip)
          |> a(:subscription, bytes(), :skip)
          |> a(:assignment, assignment())

        3 ->
          rest
          |> a(:member_id, string())
          |> a(:group_instance_id, nullable_string(), :skip)
          |> a(:client_id, string())
          |> a(:client_host, string())
          |> a(:rebalance_timeout, int(32), :skip)
          |> a(:session_timeout, int(32), :skip)
          |> a(:subscription, bytes(), :skip)
          |> a(:assignment, assignment())
      end
    end
  end

  defp a(next, key, parser, skip_or_keep \\ :keep)
  defp a(next, key, parser, opt) when is_binary(next), do: a({:ok, %{}, next}, key, parser, opt)
  defp a({:error, e}, _key, _parser, _opt), do: {:error, e}
  defp a({:ok, built, next}, key, parser, skip_or_keep) do
    with {:ok, val, rest} <- parser.(next) do
      case skip_or_keep do
        :skip ->
          {:ok, built, rest}

        :keep ->
          {:ok, Map.put(built, key, val), rest}
      end
    end
  end

  defp collect({:error, e}), do: {:error, e}
  defp collect({:ok, result, _}), do: result

  defp list_of(parser) do
    fn << length :: integer-size(32), next :: binary >> ->
      result = parse_list(length, next, [], parser)
      result
    end
  end

  defp parse_list(0, rest, acc, _), do: {:ok, acc, rest}
  defp parse_list(i, rest, parsed, parser) do
    case parser.(rest) do
      {:error, e} ->
        {:error, e}

      {:ok, val, rest} ->
        parse_list(i-1, rest, parsed ++ [val], parser)
    end
  end

  defp assignment do
    fn next ->
      << sz :: integer-size(32), bts :: bytes-size(sz), other :: binary >> = next

      result =
        bts
        |> a(:version, int(16), :skip)
        |> a(:topics, list_of(topic()))
        |> a(:user_data, bytes(), :skip)

      with {:ok, assignment, _rest} <- result do
        assignment = %{
          topics: Enum.into(assignment.topics, %{}),
        }

        {:ok, assignment, other}
      end
    end
  end

  defp topic do
    fn next ->
      result =
        next
        |> a(:name, string())
        |> a(:partitions, list_of(partition()))

      with {:ok, topic, rest} <- result do
        topic = {topic.name, topic.partitions}
        {:ok, topic, rest}
      end
    end
  end

  defp partition do
    fn next ->
      result =
        next
        |> a(:id, int(32))

      with {:ok, partition, rest} <- result do
        {:ok, partition.id, rest}
      end
    end
  end

  defp bytes do
    fn
      << sz :: signed-integer-size(32), rest :: binary >> ->
        if sz > 0 do
          << bts :: bytes-size(sz), rest :: binary>> = rest
          {:ok, bts, rest}
        else
          {:ok, nil, rest}
        end

      rest ->
        {:error, "Bytes: #{rest}"}
    end
  end

  defp string do
    fn
      << str_size :: integer-size(16), str :: binary-size(str_size), rest :: binary >> ->
        {:ok, str, rest}

      rest ->
        {:error, rest}
    end
  end

  defp nullable_string do
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
