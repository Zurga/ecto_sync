defmodule EctoSync.Syncer do
  @moduledoc false
  alias EctoSync.{SyncParams, Subscriber}
  alias Ecto.Association.{BelongsTo, Has, HasThrough, ManyToMany, NotLoaded}
  import EctoSync.Helpers

  def sync(from_cache_or_value, params)

  def sync(:cached, %{event: :deleted} = params) do
    do_unsubscribe(params)

    if is_binary(params.schema) do
      {params.schema, params.id}
    else
      struct(params.schema, %{id: params.id})
    end
  end

  def sync(:cached, %{event: :inserted} = params) do
    value = get_from_cache(params)

    EctoSync.subscribe(value)
    value
  end

  def sync(:cached, params), do: get_from_cache(params)

  def sync(value_or_values, %{event: :deleted} = params) do
    do_unsubscribe(params)
    do_sync(value_or_values, params.id, params)
  end

  def sync(value_or_values, %{schema: schema, event: :inserted} = params) do
    preloads =
      for id <- params.assocs,
          {_, opts} <- Subscriber.subscriptions({schema, :inserted}, id),
          assoc <- opts[:assocs] do
        assoc
      end
      |> Enum.concat(params.preloads[schema] || [])
      |> List.flatten()

    params =
      %{
        params
        | preloads: Map.update(params.preloads, schema, preloads, &kw_deep_merge(&1, preloads))
      }

    if is_binary(schema) do
      case Map.get(params.schemas.join_modules, schema) do
        associated_schemas ->
          associated_schemas
          |> Enum.reduce(value_or_values, fn {_parent, {key, child}}, acc ->
            id = params.assocs[key]

            record =
              get_preloaded(child, id, preloads, params)

            Subscriber.subscribe(record, assocs: preloads)
            do_sync(acc, record, params)
          end)
      end
    else
      new = get_preloaded(params.schema, params.id, preloads, params)
      Subscriber.subscribe(new, assocs: preloads)

      do_sync(value_or_values, new, params)
      |> then(fn
        values when is_list(values) ->
          Enum.map(values, &maybe_update_has_through(&1, new, params))

        value ->
          maybe_update_has_through(value, new, params)
      end)
    end
  end

  def sync(value_or_values, params) do
    new = get_from_cache(params)

    do_sync(value_or_values, new, params)
  end

  defp do_sync(nil, new, %{event: :inserted}), do: new

  defp do_sync([], new, %{event: event}) when event in ~w/inserted updated/a do
    [new]
  end

  defp do_sync(
         [%schema{} | _] = values,
         new,
         %{event: :inserted, schema: schema, strict: true} = params
       ) do
    Enum.map(values, &do_sync(&1, new, params)) ++ [new]
  end

  defp do_sync([%_schema{} | _] = values, new, %{event: :inserted, strict: false} = params) do
    Enum.map(values, &do_sync(&1, new, params)) ++ [new]
  end

  defp do_sync([%schema{} | _] = values, id, %{event: :deleted, schema: schema} = params) do
    Enum.reject(values, &same_record?(&1, {schema, id}))
    |> Enum.map(&do_sync(&1, id, params))
  end

  defp do_sync(values, new, params) when is_list(values),
    do: Enum.map(values, &do_sync(&1, new, params))

  defp do_sync(%value_schema{} = value, deleted_id, %{event: :deleted, schema: schema} = params) do
    case Map.get(params.schemas.join_modules, schema) do
      nil ->
        params.schemas
        |> EctoGraph.paths(value_schema, schema)
        |> EctoGraph.prewalk(value, &assoc_update(&1, &2, &3, deleted_id, params))

      associated_schemas ->
        associated_schemas
        |> Enum.reduce(value, fn {parent, {key, child}}, acc ->
          id = params.assocs[key]

          params.schemas
          |> EctoGraph.paths(value_schema, parent)
          |> EctoGraph.prewalk(acc, fn _acc, assoc, _assoc_info ->
            params.schemas
            |> EctoGraph.paths(parent, child)
            |> EctoGraph.prewalk(assoc, &assoc_update(&1, &2, &3, id, params))
          end)
        end)
    end
  end

  defp do_sync(%value_schema{} = value, %new_schema{} = new, params) when is_struct(value) do
    if same_record?(value, new) do
      preloads = find_preloads(params.preloads[new_schema] || value)

      get_preloaded(value_schema, params.id, preloads, params)
    else
      params.schemas
      |> EctoGraph.paths(value_schema, new_schema)
      |> EctoGraph.prewalk(value, &assoc_update(&1, &2, &3, new, params))
    end
  end

  defp do_sync(%value_schema{} = value, new, %{schema: schema} = params) do
    case Map.get(params.schemas.join_modules, schema) do
      nil ->
        params.schemas
        |> EctoGraph.paths(value_schema, schema)
        |> EctoGraph.prewalk(value, &assoc_update(&1, &2, &3, new, params))

      associated_schemas ->
        associated_schemas
        |> Enum.reduce(value, fn {_parent, {key, child}}, acc ->
          id = params.assocs[key]
          record = get_preloaded(child, id, [], params)
          do_sync(acc, record, params)
        end)
    end
  end

  defp do_sync(value, _new, _params) do
    value
  end

  defp assoc_update(
         _parent,
         assoc,
         %ManyToMany{
           related: related_schema,
           join_through: schema,
           join_keys: [_, {child_key, _}]
         },
         _,
         %{schema: schema, event: :deleted} = params
       ) do
    id = Map.get(params.assocs || %{}, child_key)

    case find_by_primary_key(assoc, {related_schema, id}) do
      nil -> assoc
      index -> List.delete_at(assoc, index)
    end
  end

  defp assoc_update(_parent, assoc, _assoc_info, id, %{schema: schema, event: :deleted})
       when is_list(assoc) do
    case find_by_primary_key(assoc, {schema, id}) do
      nil -> assoc
      index -> List.delete_at(assoc, index)
    end
  end

  defp assoc_update(_parent, assoc, _assoc_info, id, %{schema: schema, event: :deleted}) do
    if same_record?(assoc, {schema, id}) do
      nil
    else
      assoc
    end
  end

  defp assoc_update(_value, assocs, %HasThrough{}, new, %{event: :inserted})
       when is_list(assocs) do
    if is_nil(find_by_primary_key(assocs, new)) do
      assocs ++ [new]
    else
      assocs
    end
  end

  defp assoc_update(_value, _assoc, %HasThrough{}, new, %{event: :inserted}),
    do: new

  defp assoc_update(_value, assocs, %HasThrough{}, new, %{event: :updated}) do
    if is_list(assocs) do
      possible_index = find_by_primary_key(assocs, new)
      List.replace_at(assocs, possible_index, new)
    else
      new
    end
  end

  defp assoc_update(value, assocs, %Has{} = assoc_info, new, %{schema: schema} = params) do
    possible_index = find_by_primary_key(assocs, new)
    related_id = Map.get(new, assoc_info.related_key)
    owner_id = Map.get(value, assoc_info.owner_key)

    cond do
      # Maybe we are removed as assoc
      not is_nil(possible_index) and related_id != owner_id and
          assoc_info.related == schema ->
        # Broadcast an insert to the new owner
        # TODO Unsubscribe from the assoc.

        do_unsubscribe(params)

        List.delete_at(assocs, possible_index)

      # Maybe we are assigned as assoc
      is_nil(possible_index) and related_id == owner_id and
          assoc_info.related == schema ->
        do_insert(assocs, new, assoc_info, params)

      true ->
        maybe_update(assocs, new, params)
    end
  end

  defp assoc_update(value, assoc, assoc_info, new, params) do
    {related?, resolved} = resolve_assoc(assoc_info, value, new, params)

    if related? and params.event == :inserted do
      do_insert(assoc, resolved, assoc_info, params)
    else
      maybe_update(assoc, new, params)
    end
  end

  defp maybe_update(values, new, params) when is_list(values),
    do: Enum.map(values, &maybe_update(&1, new, params)) |> Enum.reject(&is_nil/1)

  defp maybe_update(%schema{} = value, new, params) do
    if same_record?(value, new) do
      if value != new do
        new
      end

      preloads = find_preloads(params.preloads[new.__struct__] || value)

      get_preloaded(schema, new.id, preloads, params)
    else
      value
    end
  end

  defp maybe_update_has_through(%value_schema{} = value, %new_schema{} = new, params) do
    # For each preloaded assoc, check if there is another schema that has it as a HasThrough.
    # If so, update that association based on its path for the assoc.
    reduce_preloaded_assocs(value, fn
      {key, %HasThrough{through: through} = assoc_info}, acc ->
        related_schema = resolve_through(value_schema, through)

        params.schemas
        |> EctoGraph.paths(new_schema, related_schema)
        |> Enum.map(&EctoGraph.get(new, &1))
        |> Enum.reduce(acc, fn values, acc ->
          Enum.reduce(values, acc, fn
            %NotLoaded{}, acc ->
              acc

            value, acc ->
              Map.update!(acc, key, &do_insert(&1, value, assoc_info, params))
          end)
        end)

      _, acc ->
        acc
    end)
  end

  defp do_insert(assocs, {_new, {schema, id}}, assoc_info, params) when is_list(assocs) do
    preloads =
      case assocs do
        [] -> params.preloads[schema] || []
        _ -> find_preloads(params.preloads[schema] || assocs || [])
      end

    inserted = get_preloaded(schema, id, preloads, params)
    in_where = match_where?(inserted, assoc_info)

    if in_where do
      EctoSync.subscribe(inserted)
      assocs ++ [inserted]
    else
      assocs
    end
  end

  defp do_insert(assocs, %schema{} = new, assoc_info, params) when is_list(assocs) do
    preloads =
      case assocs do
        [] -> params.preloads[schema] || []
        _ -> find_preloads(params.preloads[schema] || assocs || [])
      end

    inserted = get_preloaded(schema, new.id, preloads, params)

    in_where = match_where?(inserted, assoc_info)

    if in_where do
      EctoSync.subscribe(inserted)

      Enum.map(assocs, &maybe_update(&1, new, params)) ++ [inserted]
    else
      assocs
    end
  end

  defp do_insert(assoc, %schema{} = new, assoc_info, params) do
    preloads = find_preloads(params.preloads[schema] || assoc)

    new = get_preloaded(schema, new.id, preloads, params)

    (match_where?(new, assoc_info) && new) || assoc
  end

  defp get_preloaded(schema, id, preloads, params) do
    repo = params.repo_mod

    params =
      SyncParams.maybe_put_get_fun(params, fn schema, id ->
        repo.get(schema, id) |> repo.preload(preloads, force: true)
      end)

    get_from_cache(%{params | schema: schema, id: id, preloads: %{schema => preloads}})
  end

  defp resolve_assoc(%ManyToMany{join_through: schema} = assoc, value, new, %{schema: schema})
       when is_map(new) do
    [{related_key, parent_key}, {id_key, _}] = assoc.join_keys

    parent_id = Map.get(value, parent_key, false)

    child_id = Map.get(new, related_key)

    {parent_id == child_id, {new, {assoc.related, Map.get(new, id_key)}}}
  end

  defp resolve_assoc(assoc_info, value, new, %{schema: schema}) do
    case assoc_info do
      %ManyToMany{} ->
        {false, new}

      %Has{related: ^schema} ->
        parent_id = Map.get(value, assoc_info.owner_key, false)
        child_id = Map.get(new, assoc_info.related_key)
        {parent_id == child_id, new}

      %BelongsTo{related: ^schema} ->
        {false, new}

      _ ->
        {false, new}
    end
  end

  def find_by_primary_key([], _needle), do: nil

  def find_by_primary_key([%schema{} = value | _] = values, needle) when is_struct(value) do
    primary_key = primary_key(schema)

    Enum.find_index(values, &same_record?(&1, needle, primary_key))
  end

  defp same_record?(v1, v2, primary_key \\ nil)

  defp same_record?(%NotLoaded{}, _, _), do: false

  defp same_record?(%schema_mod{} = v1, %schema_mod{} = v2, primary_key) do
    primary_key =
      if primary_key == nil do
        primary_key(schema_mod)
      else
        primary_key
      end

    Map.get(v1, primary_key) == Map.get(v2, primary_key)
  end

  defp same_record?(%schema_mod{} = v1, {schema_mod, id}, primary_key) do
    primary_key =
      if primary_key == nil do
        primary_key(schema_mod)
      else
        primary_key
      end

    Map.get(v1, primary_key) == id
  end

  defp same_record?(_v1, _v2, _primary_key), do: false

  defp do_unsubscribe(%{schema: schema, id: id}) do
    if ecto_schema_mod?(schema) do
      struct(schema, id: id)
    else
      schema
    end
    |> EctoSync.unsubscribe([])

    {schema, id}
  end

  defp match_where?(_struct, []) do
    true
  end

  defp match_where?(struct, %{where: [{field, condition} | conditions]}) do
    value = Map.get(struct, field)

    truthy? =
      case condition do
        nil -> is_nil(value)
        {:not, comparer} -> value != comparer
        {:in, comparer} -> value in comparer
        comparer -> value == comparer
      end

    (truthy? && match_where?(struct, conditions)) || false
  end

  defp match_where?(_struct, %{}), do: true
end
