defmodule EctoSync.Publisher do
  import EctoSync.Helpers, only: [get_encoded_label: 1, ecto_schema_mod?: 1]
  use GenServer

  def start_link(opts) do
    watchers = opts[:watchers]
    GenServer.start_link(__MODULE__, watchers, name: __MODULE__)
  end

  def init(watchers) do
    {:ok, %{watchers: watchers}}
  end

  def publish(schema_or_table, event, values) do
    table =
      if ecto_schema_mod?(schema_or_table) do
        schema_or_table.__schema__(:source)
      else
        schema_or_table
      end

    GenServer.cast(__MODULE__, {:publish, table, event, values})
  end

  def handle_cast({:publish, table, event, values}, state) do
    %{
      schema_definition: %{primary_key: primary_key} = schema_definition,
      extra_columns: extra_columns
    } =
      Enum.find(state.watchers, &(&1.schema_definition.table_name == table))

    schema =
      if is_binary(schema_definition.label) do
        schema_definition.table_name
      else
        schema_definition.label
      end

    identifiers =
      Map.take(values, ([primary_key] ++ extra_columns) |> Enum.map(&to_string/1))
      |> Map.new(fn {k, v} -> {String.to_existing_atom(k), v} end)

    id = Map.get(values, primary_key)

    ref =
      case get_encoded_label({schema, event}) do
        {_mod, :inserted} = watcher -> watcher
        {mod, _} -> {mod, {primary_key, id}}
        label -> {label, {primary_key, id}}
      end
      |> EctoSync.increment_row_ref()

    if event == :inserted do
      [nil]
    else
      [primary_key]
    end
    |> Enum.concat(extra_columns)
    |> Enum.map(fn field ->
      identifier =
        (field && {field, Map.get(identifiers, field)}) ||
          nil

      EctoSync.Subscriber.subscriptions({schema, event}, identifier)
      |> Enum.map(fn {pid, opts} ->
        IO.inspect({pid, opts, identifiers})

        case opts[:parent] do
          {key, id} ->
            # The has_many assoc has moved away from this subscription
            if identifiers[key] != id do
              # send(pid, {:ecto_sync, {schema, :inserted, {identifier, ref}}})
              publish(table, :inserted, values)
            end

          _ ->
            nil
        end

        send(pid, {:ecto_sync, {schema, event, {identifiers, ref}}})
      end)
    end)

    {:noreply, state}
  end
end
