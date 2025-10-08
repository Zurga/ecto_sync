defmodule EctoSync.Config do
  @moduledoc false

  @derive {Inspect, only: ~w/id ref schema event/a}
  alias EctoSync.Helpers
  import Ecto.Query

  @type t :: %__MODULE__{}
  defstruct assocs: nil,
            cache_name: nil,
            event: nil,
            get_fun: nil,
            schemas: nil,
            id: nil,
            preloads: [],
            pub_sub: nil,
            ref: nil,
            repo: nil,
            schema: nil

  def new({label, {identifiers, ref}}, opts) when is_atom(label) do
    {config, state} = init(identifiers, ref, opts)

    {%{table_name: table, primary_key: primary_key, columns: columns}, event, _} =
      state.watchers
      |> Enum.find(fn
        {_, _, opts} ->
          Keyword.get(opts, :label) == label

        _ ->
          false
      end)

    table = to_string(table)

    keys = [primary_key | columns]

    %{
      config
      | schema: table,
        event: event,
        get_fun: fn table, id ->
          filters = [{primary_key, id}]

          from(table)
          |> select([t], ^keys)
          |> where(^filters)
          |> config.repo.one
        end
    }
  end

  def new({schema, event, {identifiers, ref}}, opts) do
    {config, _} = init(identifiers, ref, opts)
    %{config | schema: schema, event: event, get_fun: &config.repo.get(&1, &2)}
  end

  defp init(%{id: id} = identifiers, ref, opts) do
    assocs = Map.drop(identifiers, [:id])

    state = :persistent_term.get(EctoSync)

    global = Map.take(state, ~w/cache_name schemas pub_sub repo/a)

    {%__MODULE__{
       id: id,
       ref: ref,
       assocs: assocs,
       preloads: (opts[:preloads] || %{}) |> Helpers.normalize_to_preloads()
     }
     |> Map.merge(global), state}
  end

  def maybe_put_get_fun(config, nil), do: config
  def maybe_put_get_fun(config, get_fun), do: Map.put(config, :get_fun, get_fun)
end
