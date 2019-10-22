defmodule Orwell.MixProject do
  use Mix.Project

  def project do
    [
      app: :orwell,
      version: "0.1.0",
      elixir: "~> 1.7",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {Orwell.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:brod, "~> 3.8"},
      {:cbuf, "~> 0.7"},
      {:norm, "~> 0.6.0"},
      {:telemetry, "~> 0.4"},
      {:statix, github: "rodrigues/statix", branch: "events_checks"},

      {:propcheck, "~> 1.1", only: [:test, :dev]},
      {:stream_data, "~> 0.4.3", only: [:test, :dev]},
      {:credo, "~> 1.1"}
    ]
  end
end
