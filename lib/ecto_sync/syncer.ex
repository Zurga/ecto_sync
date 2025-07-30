defmodule EctoSync.Syncer do
  @moduledoc false
  alias EctoSync.SyncConfig
  alias Ecto.Schema
  alias Ecto.Association.{BelongsTo, Has, ManyToMany}
  import EctoSync.Helpers

  @spec sync(
          atom() | Schema.t() | list(Schema.t()),
          {{struct(), atom()}, {integer() | String.t(), reference()}}
        ) :: Schema.t() | term()
  def sync(:cached, {{_, :deleted}, _} = event) do
    event
    |> SyncConfig.new()
    |> do_unsubscribe()
  end

  def sync(:cached, {{_, :inserted}, _} = event) do
    value =
      event
      |> SyncConfig.new()
      |> get_from_cache()

    EctoSync.subscribe(value)
    value
  end

  def sync(:cached, event), do: SyncConfig.new(event) |> get_from_cache()

  def sync(value_or_values, {{_, :deleted}, _} = event) do
    config = SyncConfig.new(event)
    do_unsubscribe(config)
    do_sync(value_or_values, config.id, config)
  end

  def sync(value_or_values, event) do
    config = SyncConfig.new(event)
    new = get_from_cache(config)

    do_sync(value_or_values, new, config)
  end

  defp do_sync(nil, new, %{event: :inserted}), do: new

  defp do_sync([], new, %{event: :inserted}) do
    [new]
  end

  defp do_sync(values, new, config) when is_list(values) do
    Enum.map(values, &do_sync(&1, new, config))
  end

  defp do_sync(
         %{__struct__: value_schema} = value,
         %{__struct__: new_schema} = new,
         config
       )
       when is_struct(value) do
    if same_record?(value, new) do
      get_preloaded(value_schema, config.id, find_preloads(value), config)
    else
      paths = path_to(value_schema, new_schema, config)

      Enum.reduce(paths, value, fn path, acc ->
        deep_update(acc, path, new, config)
      end)
    end
  end

  defp do_sync(%{__struct__: value_schema} = value, new, %{schema: schema} = config) do
    case Map.get(config.join_modules, schema) do
      nil ->
        paths = path_to(value_schema, schema, config)

        Enum.reduce(paths, value, fn path, acc ->
          deep_update(acc, path, new, config)
        end)

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

  defp deep_update(value, {key, []}, new, %{schema: schema} = config) do
    assoc_info =
      get_schema(value).__schema__(:association, key)

    assoc = Map.get(value, key)

    case {assoc_info, assoc} do
      {_, %Ecto.Association.NotLoaded{}} ->
        value

      {%Has{field: key}, assocs} ->
        possible_index = find_by_primary_key(assocs, new)
        related_id = (is_map(new) && Map.get(new, assoc_info.related_key)) || new
        owner_id = Map.get(value, assoc_info.owner_key)

        result =
          cond do
            # Maybe we are removed as assoc
            not is_nil(possible_index) and related_id != owner_id and
                assoc_info.related == schema ->
              # Broadcast an insert to the new owner
              # assoc_moved(new, owner_id, assoc_info)

              List.delete_at(assocs, possible_index)

            # Maybe we are assigned as assoc
            is_nil(possible_index) and related_id == owner_id and
                assoc_info.related == schema ->
              do_insert(value, key, assocs, new, config)

            true ->
              maybe_update(assocs, new, config)
          end

        Map.put(value, key, result)

      {_, assoc} ->
        {related?, resolved} =
          resolve_assoc(assoc_info, value, new, config)

        Map.put(
          value,
          key,
          if related? and config.event == :inserted do
            do_insert(value, key, assoc, resolved, config)
          else
            maybe_update(assoc, new, config)
          end
        )
    end
  end

  defp deep_update(value, {key, nested}, new, config) do
    Map.update!(value, key, &deep_update(&1, nested, new, config))
  end

  defp deep_update(value, keys, new, config) do
    Enum.reduce(keys, value, &deep_update(&2, &1, new, config))
    # Map.update!(value, key, &deep_update(&1, nested, new, config))
  end

  defp maybe_update(values, new, config) when is_list(values),
    do: Enum.map(values, &maybe_update(&1, new, config)) |> Enum.reject(&is_nil/1)

  defp maybe_update(%{__struct__: schema} = value, id, %{schema: schema, event: :deleted}) do
    if not same_record?(value, {schema, id}) do
      value
    end
  end

  defp maybe_update(value, id, %{schema: schema, event: :deleted} = config) do
    reduce_preloaded_assocs(value, fn {key, assoc_info}, acc ->
      {schema, id} =
        case assoc_info do
          %ManyToMany{related: related_schema, join_through: ^schema, join_keys: join_keys} ->
            [_, {child_key, _}] = join_keys
            {related_schema, Map.get(config.assocs, child_key)}

          _ ->
            {schema, id}
        end

      Map.update!(acc, key, fn
        assocs when is_list(assocs) ->
          case find_by_primary_key(assocs, {schema, id}) do
            nil -> assocs
            index -> List.delete_at(assocs, index)
          end
          |> maybe_update(id, config)

        assoc ->
          maybe_update(assoc, id, config)
      end)
    end)
  end

  defp maybe_update(value, new, config) do
    if same_record?(value, new) do
      preloads = find_preloads(value)

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
        [] -> []
        _ -> find_preloads(assocs)
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
    preloads = find_preloads(assoc || config.preloads)

    new = get_preloaded(schema, new.id, preloads, config)

    EctoSync.subscribe(new)

    new
  end

  defp get_preloaded(schema, id, preloads, config) do
    repo = config.repo

    config =
      SyncConfig.maybe_put_get_fun(config, fn _schema, _id ->
        repo.get(schema, id) |> repo.preload(preloads, force: true)
      end)

    get_from_cache([preloads], config)
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
end
