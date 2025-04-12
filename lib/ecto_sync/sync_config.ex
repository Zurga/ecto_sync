defmodule EctoSync.SyncConfig do
  import Ecto.Query
  @moduledoc false
  @type t :: %__MODULE__{}
  defstruct id: nil,
            schema: nil,
            event: nil,
            repo: nil,
            cache_name: nil,
            ref: nil,
            get_fun: nil,
            preloads: []

  def new(id, label) when is_atom(label) do
    {config, state} = init(id)

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
      | id: id,
        schema: table,
        event: event,
        get_fun: fn table, id ->
          filters = [{primary_key, id}]

          from(r in table, select: ^keys, where: ^filters)
          |> config.repo.one
        end
    }
  end

  def new(id, {schema, event}) do
    {config, _} = init(id)
    %{config | id: id, schema: schema, event: event, get_fun: &config.repo.get(&1, &2)}
  end

  defp init(id) do
    ref = :erlang.make_ref()
    %{repo: repo, cache_name: cache_name} = state = :persistent_term.get(__MODULE__)

    {%__MODULE__{
       id: id,
       cache_name: cache_name,
       ref: ref,
       repo: repo
     }, state}
  end

  def maybe_put_get_fun(config, nil), do: config
  def maybe_put_get_fun(config, get_fun), do: Map.put(config, :get_fun, get_fun)
end
