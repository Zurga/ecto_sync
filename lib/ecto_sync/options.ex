# Original code copied and maybe modified from EctoWatch
defmodule EctoSync.Options do
  @moduledoc false

  alias EctoSync.Options.WatcherOptions

  @cache_name :ecto_sync
  defstruct ~w/adapter cache_name schemas repo_mod pub_sub_mod watchers debug?/a

  def new(opts) do
    watchers = opts[:watchers]

    schemas =
      watchers
      |> Enum.map(fn
        {%{table_name: table}, _, _} ->
          table

        tuple ->
          elem(tuple, 0)
      end)
      |> Enum.uniq()
      |> EctoGraph.new()

    %__MODULE__{
      adapter: opts[:adapter],
      repo_mod: opts[:repo],
      pub_sub_mod: opts[:pub_sub],
      cache_name: opts[:cache_name] || @cache_name,
      debug?: opts[:debug?],
      schemas: schemas,
      watchers:
        Enum.map(watchers, fn watcher_opts ->
          WatcherOptions.new(watcher_opts, opts[:debug?])
        end)
    }
  end

  def validate(opts) do
    schema = [
      repo: [
        type: {:custom, __MODULE__, :check_valid_repo_module, []},
        required: true
      ],
      watchers: [
        type: {:custom, WatcherOptions, :validate_list, []},
        required: true
      ],
      adapter: [type: :atom, required: true],
      debug?: [
        type: :boolean,
        required: false,
        default: false
      ]
    ]

    NimbleOptions.validate(opts, schema)
  end

  def check_valid_repo_module(repo_mod) when is_atom(repo_mod) do
    if repo_mod in Ecto.Repo.all_running() do
      {:ok, repo_mod}
    else
      {:error, "#{inspect(repo_mod)} was not a currently running ecto repo"}
    end
  end

  def check_valid_repo_module(repo), do: {:error, "#{inspect(repo)} was not an atom"}

  def check_valid_pubsub_module(pubsub_mod) when is_atom(pubsub_mod) do
    Phoenix.PubSub.node_name(pubsub_mod)

    {:ok, pubsub_mod}
  rescue
    _ -> {:error, "#{inspect(pubsub_mod)} was not a currently running Phoenix PubSub module"}
  end

  def check_valid_pubsub_module(repo), do: {:error, "#{inspect(repo)} was not an atom"}
end
