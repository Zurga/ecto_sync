defmodule EctoSync.SyncConfig do
  @moduledoc false

  import Ecto.Query

  @type t :: %__MODULE__{}
  defstruct id: nil,
            schema: nil,
            event: nil,
            repo: nil,
            cache_name: nil,
            ref: nil,
            get_fun: nil,
            assocs: nil,
            preloads: [],
            graph: nil,
            join_modules: nil

  def new({label, {identifiers, ref}}) when is_atom(label) do
    {config, state} = init(identifiers, ref)

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

          from(r in table, select: ^keys, where: ^filters)
          |> config.repo.one
        end
    }
  end

  def new({{schema, event}, {identifiers, ref}}) do
    {config, _} = init(identifiers, ref)
    %{config | schema: schema, event: event, get_fun: &config.repo.get(&1, &2)}
  end

  defp init(%{id: id} = identifiers, ref) do
    assocs = Map.drop(identifiers, [:id])

    %{repo: repo, cache_name: cache_name, graph: graph, join_modules: join_modules} =
      state = :persistent_term.get(__MODULE__)

    {%__MODULE__{
       id: id,
       cache_name: cache_name,
       ref: ref,
       repo: repo,
       assocs: assocs,
       graph: graph,
       join_modules: join_modules
     }, state}
  end

  def maybe_put_get_fun(config, nil), do: config
  def maybe_put_get_fun(config, get_fun), do: Map.put(config, :get_fun, get_fun)
end
