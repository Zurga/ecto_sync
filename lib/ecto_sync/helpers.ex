defmodule EctoSync.Helpers do
  @moduledoc false
  alias EctoSync.SyncConfig

  def ecto_schema_mod?(schema_mod) do
    schema_mod.__schema__(:fields)

    true
  rescue
    UndefinedFunctionError -> false
  end

  def get_schema([value | _]), do: get_schema(value)
  def get_schema(value) when is_struct(value), do: value.__struct__
  def get_schema(_), do: nil

  def find_preloads([value | _]), do: find_preloads(value)

  def find_preloads(value) when is_struct(value) do
    reduce_preloaded_assocs(value, [], fn {key, _}, acc ->
      case Map.get(value, key) do
        nil ->
          [{key, []} | acc]

        [] ->
          [{key, []} | acc]

        [assoc | _] ->
          [{key, find_preloads(assoc)} | acc]

        assoc ->
          [{key, find_preloads(assoc)} | acc]
      end
    end)
  end

  def get_from_cache(key \\ [], %SyncConfig{
        ref: ref,
        cache_name: cache_name,
        id: id,
        schema: schema,
        get_fun: get_fun
      }) do
    key =
      List.to_tuple([schema, id] ++ key ++ [ref])

    {_, value} =
      Cachex.fetch(cache_name, key, fn _key ->
        {:commit, get_fun.(schema, id)}
      end)

    value
  end

  def primary_key(%{__struct__: schema_mod} = value) when is_struct(value) do
    primary_key(schema_mod)
    |> then(&Map.get(value, &1))
  end

  def primary_key(schema_mod) when is_atom(schema_mod) do
    :primary_key
    |> schema_mod.__schema__()
    |> hd()
  end

  def reduce_assocs(schema_mod, acc \\ nil, function) when is_function(function) do
    schema_mod.__schema__(:associations)
    |> Enum.reduce(acc, fn key, acc ->
      assoc_info = schema_mod.__schema__(:association, key)
      function.({key, assoc_info}, acc)
    end)
  end

  def reduce_preloaded_assocs(%{__struct__: schema_mod} = value, acc \\ nil, function)
      when is_function(function) do
    reduce_assocs(schema_mod, (is_nil(acc) && value) || acc, fn {key, assoc_info}, acc ->
      case Map.get(value, key) do
        struct when not is_struct(struct, Ecto.Association.NotLoaded) ->
          maybe_call_with_struct(function, {key, assoc_info}, struct, acc)

        _ ->
          acc
      end
    end)
  end

  def update_cache(%SyncConfig{schema: schema, event: :deleted, id: id, cache_name: cache_name}) do
    Cachex.del(cache_name, {schema, id})
    {:ok, {schema, id}}
  end

  def update_cache(%SyncConfig{
        schema: schema,
        event: _event,
        id: id,
        cache_name: cache_name,
        get_fun: get_fun
      }) do
    key = {schema, id}

    record =
      get_fun.(schema, id)

    {:ok, true} = Cachex.put(cache_name, key, record)
    {:ok, key}
  end

  defp maybe_call_with_struct(function, key, struct, acc) do
    if is_function(function, 3) do
      function.(key, struct, acc)
    else
      function.(key, acc)
    end
  end
end
