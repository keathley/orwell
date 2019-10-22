defmodule Orwell.Router do
  use Plug.Router

  alias Orwell.GroupMonitor

  plug :match
  plug :dispatch

  get "/up" do
    conn
    |> send_resp(200, "hello")
  end

  get "/consumer_groups" do
    groups = GroupMonitor.groups()

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, Jason.encode!(%{groups: groups}))
  end

  get "/consumer_groups/:group_id" do
    case GroupMonitor.group_details(group_id) do
      :none ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(404, ~s|{"group": {}}|)

      {:ok, details} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(200, Jason.encode!(details))
    end
  end

  match _ do
    send_resp(conn, 404, "oops")
  end
end
