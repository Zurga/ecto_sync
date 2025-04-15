defmodule EctoSync.Syncer do
  @moduledoc false
  alias EctoSync.SyncConfig
  alias Ecto.Schema
  alias Ecto.Association.{BelongsTo, Has, ManyToMany}
  import EctoSync.Helpers

  @spec sync(atom() | Schema.t() | list(Schema.t()), SyncConfig.t()) :: Schema.t() | term()
  def sync(:cached, %SyncConfig{event: :deleted} = config) do
    do_unsubscribe(config)
  end

  def sync(:cached, %SyncConfig{event: :inserted} = config) do
    value = get_from_cache(config)
    EctoSync.subscribe(value)
    value
  end

  def sync(:cached, config), do: get_from_cache(config)

  def sync(value_or_values, %SyncConfig{event: :deleted} = config) do
    do_unsubscribe(config)
    do_sync(value_or_values, config.id, config)
  end

  def sync(value_or_values, %SyncConfig{} = config) do
    new = get_from_cache(config)

    do_sync(value_or_values, new, config)
  end

  defp do_sync([], new, %{event: :inserted}) do
    [new]
  end

  defp do_sync(
         [%{__struct__: schema} | _] = values,
         new,
         %{schema: schema, event: :inserted} = config
       ) do
    case find_by_primary_key(values, new) do
      nil -> values ++ [new]
      _ -> update_all(values, new, config)
    end
  end

  defp do_sync(values, new, config) when is_list(values) do
    Enum.map(values, &update_all(&1, new, config))
  end

  defp do_sync(value, new, config), do: update_all(value, new, config)

  defp update_all(values, new, config) when is_list(values),
    do: Enum.map(values, &update_all(&1, new, config))

  defp update_all(value, new, %{event: :inserted} = config) do
    reduce_assocs(value, fn {key, assoc_info}, acc ->
      {related?, resolved} =
        resolve_assoc(assoc_info, value, new, config)

      Map.update!(acc, key, fn assoc ->
        if related? do
          do_insert(acc, key, assoc, resolved, config)
        else
          update_all(assoc, new, config)
        end
      end)
    end)
  end

  defp update_all(%{__struct__: schema} = value, id, %{schema: schema, event: :deleted}) do
    if not same_record?(value, {schema, id}) do
      value
    end
  end

  defp update_all(value, id, %{schema: schema, event: :deleted} = config) do
    reduce_preloaded_assocs(value, fn {key, _info}, acc ->
      Map.update!(acc, key, fn
        assocs when is_list(assocs) ->
          case find_by_primary_key(assocs, {schema, id}) do
            nil -> assocs
            index -> List.delete_at(assocs, index)
          end
          |> update_all(id, config)

        assoc ->
          update_all(assoc, id, config)
      end)
    end)
  end

  defp update_all(value, new, config) do
    if same_record?(value, new) do
      preloads = find_preloads(value)

      get_preloaded(get_schema(value), new.id, preloads, config)
    else
      reduce_preloaded_assocs(value, fn {key, assoc_info}, acc ->
        assoc_updates(acc, new, key, assoc_info, config)
      end)
    end
  end

  defp assoc_updates(value, new, key, %BelongsTo{}, config),
    do: Map.update!(value, key, &update_all(&1, new, config))

  defp assoc_updates(value, new, key, %ManyToMany{related: schema}, %{schema: schema} = config),
    do: Map.update!(value, key, &update_all(&1, new, config))

  defp assoc_updates(value, new, key, %Has{field: key} = assoc_info, %{schema: schema} = config) do
    Map.update!(value, key, fn assocs ->
      possible_index = find_by_primary_key(assocs, new)
      related_id = Map.get(new, assoc_info.related_key)
      owner_id = Map.get(value, assoc_info.owner_key)

      cond do
        # Maybe we are removed as assoc
        not is_nil(possible_index) and related_id != owner_id and
            assoc_info.related == schema ->
          # Broadcast an insert to the new owner
          # assoc_moved(new, owner_id, assoc_info)

          List.delete_at(assocs, possible_index)
          |> update_all(new, config)

        # Maybe we are assigned as assoc
        is_nil(possible_index) and related_id == owner_id and
            assoc_info.related == schema ->
          do_insert(value, key, assocs, new, config)

        true ->
          update_all(assocs, new, config)
      end
    end)
  end

  defp assoc_updates(value, _new, _key, _, _config), do: value

  # defp assoc_updates(
  #        value,
  #        new,
  #        key,
  #        %Embedded{field: key, cardinality: :many} = assoc_info,
  #        %{schema: schema} = config
  #      ) do
  #   Map.update!(value, key, fn assocs ->
  #     possible_index = find_by_primary_key(assocs, new)
  #     related_id = Map.get(new, assoc_info.related_key)
  #     owner_id = Map.get(value, assoc_info.owner_key)

  #     cond do
  #       # Maybe we are removed as assoc
  #       not is_nil(possible_index) and related_id != owner_id and
  #           assoc_info.related == schema ->
  #         # Broadcast an insert to the new owner
  #         # assoc_moved(new, owner_id, assoc_info)

  #         List.delete_at(assocs, possible_index)
  #         |> update_all(new, config)

  #       # Maybe we are assigned as assoc
  #       is_nil(possible_index) and related_id == owner_id and
  #           assoc_info.related == schema ->
  #         do_insert(value, key, assocs, new, config)

  #       true ->
  #         update_all(assocs, new, config)
  #     end
  #   end)
  # end

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

    Enum.map(assocs, &update_all(&1, new, config)) ++ [inserted]
  end

  defp do_insert(_value, _key, assoc, new, %{schema: schema} = config) do
    preloads = find_preloads(assoc || config.preloads)

    new = get_preloaded(schema, new.id, preloads, config)

    EctoSync.subscribe(new)

    new
  end

  defp get_preloaded(schema, id, preloads, config) do
    repo = config.repo

    get_from_cache(
      [preloads],
      SyncConfig.maybe_put_get_fun(config, fn _schema, _id ->
        repo.get(schema, id) |> repo.preload(preloads, force: true)
      end)
    )
  end

  defp resolve_assoc(
         %ManyToMany{related: related_schema, join_through: schema, join_keys: join_keys},
         value,
         new,
         %{schema: schema}
       ) do
    [{related_key, parent_key}, {id_key, _}] =
      join_keys

    parent_id = Map.get(value, parent_key, false)

    child_id = Map.get(new, related_key)

    {parent_id == child_id, {new, {related_schema, Map.get(new, id_key)}}}
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
end
