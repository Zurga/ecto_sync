defmodule EctoSync.Adapters.Postgres.Wal do
  use Supervisor

  def start_link(options) do
    Supervisor.start_link(__MODULE__, options, name: __MODULE__)
  end

  def init(options) do
    repo_mod = options.repo_mod

    replication_options =
      [publications: :ecto_sync, slot: :ecto_sync] ++
        Keyword.take(repo_mod.config(), ~w/host database username password/a)

    join_modules = Map.keys(options.schemas.join_modules)

    options.watchers
    |> Enum.filter(&(&1.schema_definition.label in join_modules))
    |> Enum.each(&setup_indexes(&1, repo_mod))

    children = [{__MODULE__.Replication, replication_options}]
    Supervisor.init(children, strategy: :one_for_one)
  end

  defp setup_indexes(%{extra_columns: columns} = watcher, repo_mod) do
    table = watcher.schema_definition.table_name

    index_name = "#{table}_#{Enum.join(columns, "_")}_index"

    Ecto.Adapters.SQL.query!(
      repo_mod,
      "CREATE INDEX IF NOT EXISTS #{index_name} ON #{table} (#{Enum.join(columns, ", ")}); ",
      []
    )

    Ecto.Adapters.SQL.query!(
      repo_mod,
      "ALTER TABLE #{table} REPLICA IDENTITY USING INDEX #{index_name}; ",
      []
    )
  end
end
