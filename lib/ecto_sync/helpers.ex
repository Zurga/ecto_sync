defmodule EctoSync.Helpers do
  require Logger
  @moduledoc false
  alias EctoSync.Config

  def ecto_schema_mod?(schema_mod) do
    schema_mod.__schema__(:fields)

    true
  rescue
    ArgumentError -> false
    UndefinedFunctionError -> false
  end

  def encode_watcher_identifier({schema, event}) do
    try do
      schema.__info__(:module)

      :crypto.hash(:sha256, to_string(schema))
      |> Base.encode32(case: :lower, padding: false)
      |> binary_part(0, 8)
    rescue
      _ ->
        schema
    end
    |> then(&:"#{&1}_#{event}")
  end

  def get_encoded_label(watcher_identifier),
    do: :persistent_term.get({EctoSync, watcher_identifier}, watcher_identifier)

  def get_watcher_identifier(label),
    do: :persistent_term.get({EctoSync, label}, label)

  def get_schema([value | _]), do: get_schema(value)
  def get_schema(value) when is_struct(value), do: value.__struct__
  def get_schema(_), do: nil

  def find_preloads([value | _]) when is_struct(value), do: find_preloads(value)

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

  def find_preloads(preloads), do: preloads

  def get_from_cache(%Config{
        repo: repo,
        ref: ref,
        cache_name: cache_name,
        id: id,
        schema: schema,
        get_fun: get_fun,
        preloads: preloads
      }) do
    preloads =
      Map.get(preloads || %{}, schema, [])
      |> normalize_to_preloads()
      |> nested_sort()

    key =
      List.to_tuple([schema, id] ++ [ref] ++ [preloads])

    fetched =
      Cachex.fetch(cache_name, key, fn _key ->
        {:commit, get_fun.(schema, id) |> repo.preload(preloads, force: true)}
      end)

    case fetched do
      {ok, value} when ok in ~w/ok commit/a ->
        value

      {:error, error} ->
        error
    end
  end

  def kw_deep_merge([{k1, v1} | list1], [{k1, v1} | list2]) do
    [{k1, v1} | kw_deep_merge(list1, list2)]
  end

  def kw_deep_merge([{k1, v1} | list1], [{k1, v2} | list2]) do
    [{k1, kw_deep_merge(v1, v2)} | kw_deep_merge(list1, list2)]
  end

  def kw_deep_merge([{k1, v1} | list1], [{k2, v2} | list2]) do
    [{k1, v1}, {k2, v2} | kw_deep_merge(list1, list2)]
  end

  def kw_deep_merge([{k1, v1} | list1], [k1 | list2]) when is_atom(k1) do
    [{k1, v1} | kw_deep_merge(list1, list2)]
  end

  def kw_deep_merge([{k1, v1} | list1], [k2 | list2]) when is_atom(k2) do
    [k2, {k1, v1} | kw_deep_merge(list1, list2)]
  end

  def kw_deep_merge([k1 | list2], [{k1, v1} | list1]) when is_atom(k1) do
    [{k1, v1} | kw_deep_merge(list1, list2)]
  end

  def kw_deep_merge([k2 | list2], [{k1, v1} | list1]) when is_atom(k2) do
    [k2, {k1, v1} | kw_deep_merge(list1, list2)]
  end

  def kw_deep_merge([], list), do: list
  def kw_deep_merge(list, []), do: list
  def kw_deep_merge(list, list), do: list

  def primary_key(%{__struct__: schema_mod} = value) when is_struct(value) do
    primary_key(schema_mod)
    |> then(&Map.get(value, &1))
  end

  def primary_key(schema_mod) when is_atom(schema_mod) do
    :primary_key
    |> schema_mod.__schema__()
    |> hd()
  end

  def reduce_assocs(schema_mod, acc \\ nil, function)

  def reduce_assocs(%Ecto.Association.NotLoaded{} = value, _acc, _function), do: value

  def reduce_assocs(%{__struct__: schema_mod} = value, _acc, function)
      when is_function(function) do
    reduce_assocs(schema_mod, value, function)
  end

  def reduce_assocs(schema_mod, acc, function) when is_function(function) do
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

  def resolve_through(schema, []), do: schema

  def resolve_through(schema, [key | rest]) do
    case schema.__schema__(:association, key) do
      %{related: related} ->
        resolve_through(related, rest)

      %Ecto.Association.HasThrough{through: through} ->
        resolve_through(schema, through)
    end
  end

  def walk_preloaded_assocs(value, acc \\ nil, function)
  def walk_preloaded_assocs(empty, acc, _function) when is_nil(empty) or empty == [], do: acc

  def walk_preloaded_assocs(list, acc, function) when is_list(list),
    do: Enum.reduce(list, acc, &walk_preloaded_assocs(&1, &2, function))

  def walk_preloaded_assocs(value, acc, function) when is_function(function) do
    reduce_preloaded_assocs(value, acc, fn {key, assoc_info}, struct, acc ->
      acc = function.(key, assoc_info, struct, acc)
      walk_preloaded_assocs(struct, acc, function)
    end)
  end

  def normalize_to_preloads([]), do: []

  def normalize_to_preloads(k) when is_atom(k), do: [k]

  def normalize_to_preloads(map) when is_map(map) do
    Enum.reduce(map, %{}, fn
      {k, v}, acc ->
        Map.put(acc, k, normalize_to_preloads(v))
    end)
  end

  def normalize_to_preloads([k | rest]) when is_atom(k),
    do: [{k, []} | normalize_to_preloads(rest)]

  def normalize_to_preloads([{k, v} | rest]) when is_list(v),
    do: [{k, v} | normalize_to_preloads(rest)]

  def nested_sort([]), do: []
  def nested_sort([{k, v} | rest]), do: [{k, nested_sort(v)} | nested_sort(rest)]
  def nested_sort(list), do: Enum.sort(list)
  # def update_cache(%Config{schema: schema, event: :deleted, id: id, cache_name: cache_name}) do
  #   Cachex.del(cache_name, {schema, id})
  #   {:ok, {schema, id}}
  # end

  # def update_cache(%Config{
  #       schema: schema,
  #       event: _event,
  #       id: id,
  #       cache_name: cache_name,
  #       get_fun: get_fun
  #     }) do
  #   key = {schema, id}

  #   record =
  #     get_fun.(schema, id)

  #   {:ok, true} = Cachex.put(cache_name, key, record)
  #   {:ok, key}
  # end

  defp maybe_call_with_struct(function, key, struct, acc) do
    if is_function(function, 3) do
      function.(key, struct, acc)
    else
      function.(key, acc)
    end
  end

  def label(schema_mod_or_label) do
    if ecto_schema_mod?(schema_mod_or_label) do
      module_to_label(schema_mod_or_label)
    else
      schema_mod_or_label
    end
  end

  def module_to_label(module) do
    module
    |> Module.split()
    |> Enum.join("_")
    |> String.downcase()
  end

  def validate_list(list, func) when is_list(list) do
    result =
      list
      |> Enum.map(func)

    first_error =
      result
      |> Enum.find(&match?({:error, _}, &1))

    first_error || {:ok, Enum.map(result, fn {:ok, value} -> value end)}
  end

  def validate_list(_, _) do
    {:error, "should be a list"}
  end

  def debug_log(watcher_identifier, message) do
    Logger.debug("EctoSync | #{inspect(watcher_identifier)} | #{inspect(self())} | #{message}")
  end
end
