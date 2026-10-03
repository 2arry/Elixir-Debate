defmodule Fairway.MixProject do
  use Mix.Project

  def project do
    [
      app: :fairway,
      version: "0.1.0",
      elixir: "~> 1.18",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      dialyzer: [flags: [:error_handling, :extra_return, :missing_return, :unmatched_returns]]
    ]
  end

  def application do
    [
      extra_applications: [:logger, :crypto],
      mod: {Fairway.Application, []}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]

  # The store drivers and libcluster are optional: a host application only
  # needs the ones its configuration names. `Fairway.Config` rejects a
  # configuration that names one that is not loaded.
  defp deps do
    [
      {:telemetry, "~> 1.3"},
      {:yaml_elixir, "~> 2.11"},
      {:libcluster, "~> 3.5", optional: true},
      {:exqlite, "~> 0.30", optional: true},
      {:postgrex, "~> 0.20", optional: true},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:stream_data, "~> 1.1", only: [:dev, :test]}
    ]
  end
end
