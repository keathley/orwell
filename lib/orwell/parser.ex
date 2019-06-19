defmodule Orwell.Parser do
  def parse(_, ""), do: %{type: :tombstone}
  def parse(key, value) do
    case version(key) do
      0 ->
        %{key: parse_key(key), offset: parse_value(value)}

      1 ->
        %{key: parse_key(key), offset: parse_value(value)}

      2 ->
        parse_group_metadata(key, value)
        %{key: %{version: 2}, offset: %{}}
    end
  end

  defp parse_key(key) do
    <<
      version :: size(16),
      group_size :: size(16),
      group :: binary-size(group_size),
      topic_size :: size(16),
      topic :: binary-size(topic_size),
      partition :: size(32)
    >> = key

    %{version: version, group: group, topic: topic, partition: partition}
  end

  defp parse_value(value) do
    case version(value) do
      0 ->
        parse_value_v0(value)

      1 ->
        parse_value_v0(value)

      3 ->
        %{version: 3}
    end
  end

  defp parse_value_v0(value) do
    <<
      version :: size(16),
      offset :: integer-size(64),
      metadata_size :: integer-size(16),
      metadata :: binary-size(metadata_size),
      ts :: integer-size(64),
      _rest :: binary()
    >> = value

    %{version: version, offset: offset, metadata: metadata, ts: ts}
  end

  defp parse_group_metadata(key, value) do
    << _version :: size(16), g_length :: size(16), group :: binary-size(g_length), _rest :: binary() >> = key

    case version(value) do
      v in [0, 1] ->
        <<
          version :: size(16),
          p_size :: integer-size(16),
          protocol_type :: binary-size(p_size),
          generation :: integer-size(32),
          protocol_size :: integer-size(16),
          protocol :: binary-size(protocol_size),
          leader_size :: integer-size(16),
          leader :: binary-size(leader_size),
          _rest :: binary()
        >> = value

      2 ->
        <<
          version :: size(16),
          p_size :: integer-size(16),
          protocol_type :: binary-size(p_size),
          generation :: size(32),
          protocol_size :: integer-size(16),
          protocol :: binary-size(protocol_size),
          leader_size :: integer-size(16),
          leader :: binary-size(leader_size),
          ts :: integer-size(64),
          _rest :: binary()
        >> = value
    end
  end

  def version(<< version :: integer-big-size(16), _rest :: binary() >>), do: version
end
