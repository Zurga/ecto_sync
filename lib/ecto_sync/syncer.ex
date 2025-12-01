defmodule EctoSync.Syncer do
  @moduledoc false
  alias EctoSync.{Config, Subscriber}
  alias Ecto.Association.{BelongsTo, Has, HasThrough, ManyToMany}
  import EctoSync.Helpers
  import Ecto.Query

  def sync(from_cache_or_value, config)

  def sync(:cached, %{event: :deleted} = config) do
    do_unsubscribe(config)
  end

  def sync(:cached, %{event: :inserted} = config) do
    value = get_from_cache(config)

    EctoSync.subscribe(value)
    value
  end

  def sync(:cached, config), do: get_from_cache(config)

  def sync(value_or_values, %{event: :deleted} = config) do
    do_unsubscribe(config)
    do_sync(value_or_values, config.id, config)
  end

  def sync(value_or_values, %{schema: schema, event: :inserted} = config) do
    preloads =
      for id <- config.assocs,
          {_, [assocs: assoc]} <- Subscriber.subscriptions({schema, :inserted}, id) do
        assoc
      end
      |> Enum.concat(config.preloads[schema] || [])
      |> List.flatten()

    config =
      %{
        config
        | preloads: Map.update(config.preloads, schema, preloads, &kw_deep_merge(&1, preloads))
      }

    if is_binary(schema) do
      case Map.get(config.schemas.join_modules, schema) do
        associated_schemas ->
          associated_schemas
          |> Enum.reduce(value_or_values, fn {_parent, {key, child}}, acc ->
            id = config.assocs[key]

            record =
              get_preloaded(child, id, preloads, config)

            Subscriber.subscribe(record, assocs: preloads)
            do_sync(acc, record, config)
          end)
      end
    else
      new = get_preloaded(config.schema, config.id, preloads, config)
      Subscriber.subscribe(new, assocs: preloads)

      do_sync(value_or_values, new, config)
      |> maybe_update_has_through(new, config)
    end
  end

  def sync(value_or_values, config) do
    new = get_from_cache(config)

    do_sync(value_or_values, new, config)
  end

  defp do_sync(nil, new, %{event: :inserted}), do: new

  defp do_sync([], new, %{event: :inserted}) do
    [new]
  end

  defp do_sync([%schema{} | _] = values, new, %{event: :inserted, schema: schema} = config) do
    Enum.map(values, &do_sync(&1, new, config)) ++ [new]
  end

  defp do_sync([%schema{} | _] = values, id, %{event: :deleted, schema: schema} = config) do
    Enum.reject(values, &same_record?(&1, {schema, id}))
    |> Enum.map(&do_sync(&1, id, config))
  end

  defp do_sync(values, new, config) when is_list(values),
    do: Enum.map(values, &do_sync(&1, new, config))

  defp do_sync(%value_schema{} = value, %new_schema{} = new, config) when is_struct(value) do
    if same_record?(value, new) do
      preloads = find_preloads(config.preloads[new_schema] || value)

      get_preloaded(value_schema, config.id, preloads, config)
    else
      config.schemas
      |> EctoGraph.paths(value_schema, new_schema)
      |> EctoGraph.prewalk(value, &assoc_update(&1, &2, &3, new, config))
    end
  end

  defp do_sync(%value_schema{} = value, new, %{schema: schema} = config) do
    case Map.get(config.schemas.join_modules, schema) do
      nil ->
        config.schemas
        |> EctoGraph.paths(value_schema, schema)
        |> EctoGraph.prewalk(value, &assoc_update(&1, &2, &3, new, config))

      associated_schemas ->
        associated_schemas
        |> Enum.reduce(value, fn {_parent, {key, child}}, acc ->
          id = config.assocs[key]
          record = get_preloaded(child, id, [], config)
          do_sync(acc, record, config)
        end)
    end
  end

  defp do_sync(value, _new, _config) do
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
         %{schema: schema, event: :deleted} = config
       ) do
    id = Map.get(config.assocs || %{}, child_key)

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

  defp assoc_update(value, assocs, %Has{} = assoc_info, new, %{schema: schema} = config) do
    possible_index = find_by_primary_key(assocs, new)
    related_id = Map.get(new, assoc_info.related_key)
    owner_id = Map.get(value, assoc_info.owner_key)

    cond do
      # Maybe we are removed as assoc
      not is_nil(possible_index) and related_id != owner_id and
          assoc_info.related == schema ->
        # Broadcast an insert to the new owner
        # TODO Unsubscribe from the assoc.

        if not EctoSync.subscribed?({schema, :inserted}, {assoc_info.related_key, related_id}) do
          do_unsubscribe(config)
        end

        EctoSync.Watcher.WatcherServer.broadcast(
          schema,
          :inserted,
          %{:id => new.id, assoc_info.related_key => related_id}
        )

        List.delete_at(assocs, possible_index)

      # Maybe we are assigned as assoc
      is_nil(possible_index) and related_id == owner_id and
          assoc_info.related == schema ->
        do_insert(assocs, new, assoc_info, config)

      true ->
        maybe_update(assocs, new, config)
    end
  end

  defp assoc_update(value, assoc, assoc_info, new, config) do
    {related?, resolved} = resolve_assoc(assoc_info, value, new, config)

    if related? and config.event == :inserted do
      do_insert(assoc, resolved, assoc_info, config)
    else
      maybe_update(assoc, new, config)
    end
  end

  defp maybe_update(values, new, config) when is_list(values),
    do: Enum.map(values, &maybe_update(&1, new, config)) |> Enum.reject(&is_nil/1)

  defp maybe_update(%schema{} = value, new, config) do
    if same_record?(value, new) do
      if value != new do
        new
      end

      preloads = find_preloads(config.preloads[new.__struct__] || value)

      get_preloaded(schema, new.id, preloads, config)
    else
      value
    end
  end

  defp maybe_update_has_through(value_or_values, new, config) do
    # For each preloaded assoc, check if there is another schema that has it as a HasThrough.
    # If so, update that association based on its path for the assoc.
    value_or_values
  end

  defp has_through_update(value, assoc, config) when is_list(assoc) do
    Enum.map(assoc, &has_through_update(value, &1, config))
  end

  defp has_through_update(%schema{} = value, %assoc_schema{} = assoc, config) do
    config.schemas
    |> EctoGraph.paths(schema, assoc_schema)
    |> IO.inspect(label: "#{schema}, #{assoc_schema}")
    |> EctoGraph.prewalk(value, fn _, assoc, info -> assoc_update(value, assoc, info, assoc, config) end)
  end

  defp do_insert(assocs, resolved, %{where: where}, %{schema: schema} = config)
       when is_list(assocs) do
    preloads =
      case assocs do
        [] -> config.preloads[schema] || []
        _ -> find_preloads(config.preloads[schema] || assocs || [])
      end

    # In case a many to many assocs has to be inserted.
    {new, inserted} =
      case resolved do
        {new, {related_schema, id}} ->
          {new, get_preloaded(related_schema, id, preloads, config)}

        new ->
          {new, get_preloaded(schema, new.id, preloads, config)}
      end

    in_where = match_where?(inserted, where)

    if in_where do
      EctoSync.subscribe(inserted)

      Enum.map(assocs, &maybe_update(&1, new, config)) ++ [inserted]
    else
      assocs
    end
  end

  defp do_insert(assoc, new, %{where: where}, %{schema: schema} = config) do
    preloads = find_preloads(config.preloads[schema] || assoc)

    new = get_preloaded(schema, new.id, preloads, config)

    (match_where?(new, where) && new) || assoc
  end

  defp get_preloaded(schema, id, preloads, config) do
    repo = config.repo

    config =
      Config.maybe_put_get_fun(config, fn schema, id ->
        from(schema, where: [id: ^id]) |> repo.one() |> repo.preload(preloads, force: true)
      end)

    get_from_cache(%{config | schema: schema, id: id, preloads: %{schema => preloads}})
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

  defp same_record?(%Ecto.Association.NotLoaded{}, _, _), do: false

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
      value = struct(schema, id: id)
    else
      schema
    end
    |> EctoSync.unsubscribe([])
    {schema, id}
  end

  defp match_where?(_struct, []) do
    true
  end

  defp match_where?(struct, [{field, condition} | conditions]) do
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
end
