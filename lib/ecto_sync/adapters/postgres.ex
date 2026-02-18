defmodule EctoSync.Adapters.Postgres do
  use Supervisor

  def start_link(options) do
    Supervisor.start_link(__MODULE__, options, name: __MODULE__)
  end

  def init(options) do
    children = [{__MODULE__.Replication, options}]

    Enum.each(options[:state].watchers, fn %{extra_columns: columns} = watcher ->
      table = watcher.schema_definition.table_name

      if watcher.schema_definition.label in Map.keys(options[:state].schemas.join_modules) do
        index_name = "#{table}_#{Enum.join(columns, "_")}_index"

        Ecto.Adapters.SQL.query!(
          options[:state].repo,
          "CREATE UNIQUE INDEX IF NOT EXISTS #{index_name} ON #{table} (#{Enum.join(columns, ", ")}); ",
          []
        )

        Ecto.Adapters.SQL.query!(
          options[:state].repo,
          "ALTER TABLE #{table} REPLICA IDENTITY USING INDEX #{index_name}; ",
          []
        )
      end
    end)

    Supervisor.init(children, strategy: :one_for_one)
  end
end
