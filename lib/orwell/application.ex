defmodule Orwell.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  alias Orwell.Parser

  def start(_type, _args) do
    # List all child processes to be supervised
    children = [
      # Starts a worker by calling: Orwell.Worker.start_link(arg)
      # {Orwell.Worker, arg}
    ]

    kafka_endpoints = [{'localhost', 9092}]
    topic = "__consumer_offsets"

    {:ok, _} = Application.ensure_all_started(:brod)
    :ok = :brod.start_client(kafka_endpoints, :brod_client)
    result = :brod_topic_subscriber.start_link(:brod_client, topic, :all, [{:begin_offset, :earliest}], __MODULE__, [])
    IO.inspect(result, label: "Result from starting subscriber")

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Orwell.Supervisor]
    Supervisor.start_link(children, opts)
  end

  def init(_topic, _args) do
    {:ok, [], %{}}
  end

  def handle_message(partition, message, state) do
    {:kafka_message, _offset, key, value, _ts_type, _ts, _headers} = message
    msg = Parser.parse(key, value)
    IO.inspect(msg, label: "Parsed message")
    {:ok, :ack, state}
  end

  def parse_message(_, ""), do: %{type: :tombstone}
  def parse_message(key, value) do
  end
end
