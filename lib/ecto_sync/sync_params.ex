defmodule EctoSync.SyncParams do
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
            repo_mod: nil,
            schema: nil

  def new({label, {identifiers, ref}}, opts) when is_atom(label) do
    {sync_params, options} = init(identifiers, ref, opts)

    {%{table_name: table, primary_key: primary_key, columns: columns}, event, _} =
      options.watchers
      |> Enum.find(fn
        {_, _, opts} ->
          Keyword.get(opts, :label) == label

        _ ->
          false
      end)

    table = to_string(table)

    keys = [primary_key | columns]

    %{
      sync_params
      | schema: table,
        event: event,
        get_fun: fn table, id ->
          filters = [{primary_key, id}]

          from(table)
          |> select([t], ^keys)
          |> where(^filters)
          |> sync_params.repo_mod.one
        end
    }
  end

  def new({schema, event, {identifiers, ref}}, opts) do
    {sync_params, _} = init(identifiers, ref, opts)
    %{sync_params | schema: schema, event: event, get_fun: &sync_params.repo_mod.get(&1, &2)}
  end

  defp init(%{id: id} = identifiers, ref, opts) do
    assocs = Map.drop(identifiers, [:id])

    options = :persistent_term.get(EctoSync)

    options =
      Map.take(options, ~w/cache_name schemas pub_sub repo_mod/a)

    {%__MODULE__{
       id: id,
       ref: ref,
       assocs: assocs,
       preloads: (opts[:preloads] || %{}) |> Helpers.normalize_to_preloads()
     }
     |> Map.merge(options), options}
  end

  def maybe_put_get_fun(sync_params, nil), do: sync_params
  def maybe_put_get_fun(sync_params, get_fun), do: Map.put(sync_params, :get_fun, get_fun)
end
