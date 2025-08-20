defmodule EctoSync.Syncer do
  @moduledoc false
  alias EctoSync.PubSub
  alias EctoSync.Config
  alias Ecto.Changeset
  alias Ecto.Association.{BelongsTo, Has, ManyToMany}
  import EctoSync.Helpers

  def sync(from_cache_or_value, event, opts \\ [])

  def sync(:cached, {{_, :deleted}, _} = event, opts) do
    event
    |> Config.new(opts)
    |> do_unsubscribe()
  end

  def sync(:cached, {{_, :inserted}, _} = event, opts) do
    value =
      event
      |> Config.new(opts)
      |> get_from_cache()

    EctoSync.subscribe(value)
    value
  end

  def sync(:cached, event, opts), do: Config.new(event, opts) |> get_from_cache()

  def sync(
        %Changeset{data: %{__struct__: schema} = old} = changeset,
        {{schema, _}, _} = event,
        opts
      ) do
    config = Config.new(event, opts)
    changeset_fn = opts[:changeset]

    preloads = find_preloads(config.preloads[schema] || old)

    new = get_preloaded(schema, config.id, preloads, config)

    if same_record?(old, new) do
      changeset_fn.(new, changeset.params)
    else
      changeset
    end
  end

  def sync(value_or_values, {{_, :deleted}, _} = event, opts) do
    config = Config.new(event, opts)
    do_unsubscribe(config)
    do_sync(value_or_values, config.id, config)
  end

  def sync([%{__struct__: schema} = first | _] = values, {{schema, :inserted}, _} = event, opts) do
    config = Config.new(event, opts)
    preloads = find_preloads(config.preloads[schema] || values)

    new = get_preloaded(schema, config.id, preloads, config)

    EctoSync.subscribe(new)

    do_sync(values, new, config) ++ [new]
  end

  def sync(value_or_values, event, opts) do
    config = Config.new(event, opts)

    new = get_from_cache(config)

    do_sync(value_or_values, new, config)
  end

  defp do_sync(nil, new, %{event: :inserted}), do: new

  defp do_sync([], new, %{event: :inserted}) do
    [new]
  end

  defp do_sync(values, new, config) when is_list(values),
    do: Enum.map(values, &do_sync(&1, new, config))

  defp do_sync(
         %{__struct__: value_schema} = value,
         %{__struct__: new_schema} = new,
         config
       )
       when is_struct(value) do
    if same_record?(value, new) do
      preloads = find_preloads(config.preloads[new_schema] || value)

      get_preloaded(value_schema, config.id, preloads, config)
    else
      value_schema
      |> path_to(new_schema, config)
      |> Enum.reduce(value, &deep_update(&2, &1, new, config))
    end
  end

  defp do_sync(%{__struct__: value_schema} = value, new, %{schema: schema} = config) do
    case Map.get(config.join_modules, schema) do
      nil ->
        value_schema
        |> path_to(schema, config)
        |> Enum.reduce(value, &deep_update(&2, &1, new, config))

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

  defp deep_update(%Ecto.Association.NotLoaded{} = not_loaded, _, _, _) do
    not_loaded
  end

  defp deep_update(value, path, new, config) when is_list(value) do
    Enum.map(value, &deep_update(&1, path, new, config))
  end

  defp deep_update(value, {key, []}, id, %{schema: schema, event: :deleted} = config) do
    assoc_info = get_schema(value).__schema__(:association, key)

    {schema, id} =
      case assoc_info do
        %ManyToMany{related: related_schema, join_through: ^schema, join_keys: join_keys} ->
          [_, {child_key, _}] = join_keys
          {related_schema, Map.get(config.assocs, child_key)}

        _ ->
          {schema, id}
      end

    Map.update!(value, key, fn
      assocs when is_list(assocs) ->
        case find_by_primary_key(assocs, {schema, id}) do
          nil -> assocs
          index -> List.delete_at(assocs, index)
        end

      assoc ->
        if same_record?(assoc, {schema, id}) do
          nil
        else
          assoc
        end
    end)
  end

  defp deep_update(value, {key, []}, new, %{schema: schema} = config) do
    value_schema = get_schema(value)
    assoc_info = value_schema.__schema__(:association, key)

    Map.update!(value, key, fn
      %Ecto.Association.NotLoaded{} = not_loaded ->
        not_loaded

      assoc ->
        case {assoc_info, assoc} do
          {%Has{field: key, where: where}, assocs} ->
            possible_index = find_by_primary_key(assocs, new)
            related_id = Map.get(new, assoc_info.related_key)
            owner_id = Map.get(value, assoc_info.owner_key)

            in_where = match_where?(new, where)

            cond do
              # Maybe we are removed as assoc
              not is_nil(possible_index) and related_id != owner_id and
                  assoc_info.related == schema ->
                # Broadcast an insert to the new owner
                label = :persistent_term.get({EctoSync, {schema, :inserted}})

                PubSub.broadcast(
                  :"Elixir.#{config.pub_sub}.Adapter",
                  "ew_for_#{label}|#{assoc_info.related_key}|#{related_id}",
                  {label, %{:id => new.id, assoc_info.related_key => related_id}},
                  nil
                )

                List.delete_at(assocs, possible_index)

              # Maybe we are assigned as assoc
              is_nil(possible_index) and related_id == owner_id and
                  assoc_info.related == schema ->
                if in_where do
                  do_insert(value, key, assocs, new, config)
                else
                  assocs
                end

              true ->
                maybe_update(assocs, new, config)
            end

          {_, assoc} ->
            {related?, resolved} =
              resolve_assoc(assoc_info, value, new, config)

            if related? and config.event == :inserted do
              do_insert(value, key, assoc, resolved, config)
            else
              maybe_update(assoc, new, config)
            end
        end
    end)
  end

  defp deep_update(value, {key, nested}, new, config),
    do: Map.update!(value, key, &deep_update(&1, nested, new, config))

  defp deep_update(value, keys, new, config),
    do: Enum.reduce(keys, value, &deep_update(&2, &1, new, config))

  defp maybe_update(values, new, config) when is_list(values),
    do: Enum.map(values, &maybe_update(&1, new, config)) |> Enum.reject(&is_nil/1)

  defp maybe_update(value, new, config) do
    if same_record?(value, new) do
      preloads = find_preloads(config.preloads[new.__struct__] || value)

      get_preloaded(get_schema(value), new.id, preloads, config)
    else
      value
    end
  end

  defp do_insert(value, new, %Ecto.Association.NotLoaded{} = assoc, resolved, config) do
    assoc =
      if assoc.__cardinality__ == :many do
        []
      else
        resolved
      end

    do_insert(value, new, assoc, resolved, config)
  end

  defp do_insert(_value, _key, assocs, resolved, %{schema: schema} = config)
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

    EctoSync.subscribe(inserted)

    Enum.map(assocs, &maybe_update(&1, new, config)) ++ [inserted]
  end

  defp do_insert(_value, _key, assoc, new, %{schema: schema} = config) do
    preloads = find_preloads(config.preloads[schema] || assoc)

    new = get_preloaded(schema, new.id, preloads, config)

    EctoSync.subscribe(new)

    new
  end

  defp get_preloaded(schema, id, preloads, config) do
    repo = config.repo

    config =
      Config.maybe_put_get_fun(config, fn _schema, _id ->
        repo.get(schema, id) |> repo.preload(preloads, force: true)
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

  def find_by_primary_key([value | _] = values, needle) when is_struct(value) do
    schema_mod = get_schema(value)

    primary_key = primary_key(schema_mod)

    Enum.find_index(values, &same_record?(&1, needle, primary_key))
  end

  defp same_record?(v1, v2, primary_key \\ nil)

  defp same_record?(%Ecto.Association.NotLoaded{}, _, _), do: false

  defp same_record?(%{__struct__: schema_mod} = v1, %{__struct__: schema_mod} = v2, primary_key) do
    primary_key =
      if primary_key == nil do
        primary_key(schema_mod)
      else
        primary_key
      end

    Map.get(v1, primary_key) == Map.get(v2, primary_key)
  end

  defp same_record?(%{__struct__: schema_mod} = v1, {schema_mod, id}, primary_key) do
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
    value = struct(schema, id: id)
    EctoSync.unsubscribe(value, [])
    value
  end

  defp path_to(from, to, config) do
    Map.get(config.graph, {from, to}, [])
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
