defmodule EctoSync.MixProject do
  use Mix.Project

  @source "https://github.com/Zurga/EctoSync"
  def project do
    [
      name: "EctoSync",
      app: :ecto_sync,
      description: "Subscribe to events emitted by EctoWatch, sync variables with cached values.",
      homepage_url: @source,
      version: "0.3.0",
      elixir: "~> 1.16",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      aliases: aliases(),
      deps: deps(),
      package: [
        exclude_patterns: ["priv", ".formatter.exs"],
        maintainers: ["Jim Lemmers"],
        licenses: ["MIT"],
        links: %{
          GitHub: @source
        }
      ],
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.github": :test,
        "coveralls.html": :test,
        "coveralls.json": :test
      ],

      # Docs
      name: "EctoSync",
      source_url: @source,
      home_page: @source,
      docs: [
        main: "readme",
        source_url: @source,
        extras: ["README.md"]
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp elixirc_paths(env) when env in ~w/test dev/a, do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:cachex, "~> 4.0.3"},
      {:libgraph, "~> 0.16.0"},
      {:ex_doc, "~> 0.37.2", only: :dev, runtime: false},
      {:credo, "~> 1.6", runtime: false, only: [:dev, :test]},
      {:dialyxir, "~> 1.2", runtime: false, only: [:dev, :test]},
      {:excoveralls, "~> 0.18.0", runtime: false, only: [:test]},
      {:nimble_options, "~> 1.1"},
      {:postgrex, ">= 0.0.0"},
      {:phoenix_pubsub, ">= 1.0.0"},
      {:jason, ">= 1.0.0"},
      {:ecto_graph, "~> 0.1.0"},
      {:ecto_sql, ">= 3.0.0"},
      {:mix_test_watch, "~> 1.0", only: [:dev, :test]},
      {:mox, "~> 1.2", only: [:dev, :test]}
    ]
  end

  defp aliases do
    [test: ["ecto.create --quiet -r TestRepo", "ecto.migrate --quiet -r TestRepo", "test"]]
  end
end
