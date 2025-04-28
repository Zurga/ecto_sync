defmodule EctoSync.Subscriber do
  @moduledoc """
  Internal module that holds all the subscription logic.
  """
  @events ~w/inserted updated deleted/a
  import EctoSync.Helpers
  alias Ecto.Association
  alias Ecto.Association.{BelongsTo, Has, ManyToMany}

  def subscribe(watcher_identifier_or_struct, id \\ nil)

  def subscribe([value | _] = list, opts) when is_struct(value),
    do: Enum.flat_map(list, &subscribe(&1, opts))

  def subscribe(value, opts) when is_struct(value) do
    subscribe_events(value)
    |> Enum.concat(
      flat_map_assocs(value, opts[:assocs] || [], fn parent, assoc_info ->
        subscribe_events(parent, assoc_info)
      end)
    )
    |> then(fn events ->
      if opts[:inserted] do
        schema = get_schema(value)
        Enum.concat(events, subscribe({schema, :inserted}, nil))
      else
        events
      end
    end)
    |> Enum.uniq()
    |> Enum.sort()
  end

  def subscribe(watcher_identifier, id) do
    Enum.map(subscribe_events(watcher_identifier, id), &{&1, id})
  end

  def subscribe_events(label_or_schema, assoc \\ nil)

  def subscribe_events(struct, %BelongsTo{field: field}) do
    struct
    |> Map.get(field)
    |> subscribe_events()
  end

  def subscribe_events(struct, %Has{related_key: related_key, related: schema}) do
    parent_id = primary_key(struct)
    assoc_field = {related_key, parent_id}

    [{{schema, :inserted}, assoc_field} | subscribe_events(struct)]
  end

  def subscribe_events(
        struct,
        %ManyToMany{
          join_through: join_through,
          join_keys: [{parent_key, _} | _]
        }
      ) do
    id = primary_key(struct)

    event_label = fn
      event when is_binary(join_through) ->
        String.to_atom("#{join_through}_#{event}")

      event ->
        {join_through, event}
    end

    Enum.map(@events, &{event_label.(&1), {parent_key, id}})
  end

  def subscribe_events(label_or_schema, _) when is_atom(label_or_schema) do
    if ecto_schema_mod?(label_or_schema) do
      subscribe_events({label_or_schema, :all})
    else
      List.wrap(label_or_schema)
    end
  end

  def subscribe_events(values, _) when is_list(values) do
    Enum.map(values, &subscribe_events(&1)) |> List.flatten()
  end

  def subscribe_events(value, _) when is_struct(value) do
    schema = get_schema(value)
    id = primary_key(value)

    if ecto_schema_mod?(schema) do
      ~w/updated deleted/a
      |> Enum.map(&{{schema, &1}, id})
    else
    end
  end

  def subscribe_events({schema, event} = watcher_identifier, id)
      when is_atom(schema) and event in [:all | @events] do
    case watcher_identifier do
      {schema, :all} ->
        Enum.map(@events, &{{schema, &1}, id})

      _ ->
        List.wrap(watcher_identifier)
    end
  end

  def unsubscribe([value | _] = values, opts) when is_struct(value) do
    Enum.flat_map(values, &unsubscribe(&1, opts))
  end

  def unsubscribe(value, opts \\ []) when is_struct(value) do
    subscribe_events(value)
    |> Enum.concat(
      flat_map_assocs(value, opts[:assocs] || [], fn parent, assoc_info ->
        subscribe_events(parent, assoc_info)
      end)
    )
    |> Enum.map(fn {watcher_identifier, id} = event ->
      EctoWatch.unsubscribe(watcher_identifier, id)
      event
    end)
  end

  def unsubscribe(watcher_identifier, id) do
    EctoWatch.unsubscribe(watcher_identifier, id)
  end

  defp flat_map_assocs(parent, assoc_keys, func, acc \\ [])

  defp flat_map_assocs(%Ecto.Association.NotLoaded{}, _, _, acc), do: acc

  defp flat_map_assocs(parents, assoc_keys, func, acc) when is_list(parents),
    do:
      Enum.reduce(
        parents,
        acc,
        &flat_map_assocs(&1, assoc_keys, func, &2)
      )
      |> List.flatten()

  defp flat_map_assocs(parent, nil, func, acc),
    do: [
      func.(parent, nil) | acc
    ]

  defp flat_map_assocs(parent, assoc_keys, func, acc) when is_list(assoc_keys),
    do:
      Enum.reduce(assoc_keys, acc, &flat_map_assocs(parent, &1, func, &2))
      |> List.flatten()

  defp flat_map_assocs(nil, _, _, acc), do: acc

  defp flat_map_assocs(parent, assoc_keys, func, acc) when is_struct(parent) do
    {key, nested} =
      case assoc_keys do
        {_, _} -> assoc_keys
        key -> {key, nil}
      end

    schema =
      get_schema(parent)

    assoc =
      schema.__schema__(:association, key)

    parent
    |> Map.get(key)
    |> case do
      nil ->
        acc

      value when is_list(value) or not is_struct(value, Association.NotLoaded) ->
        acc = [func.(parent, assoc) | acc]
        flat_map_assocs(value, nested, func, acc)

      _ ->
        [{{assoc.related, :inserted}, {assoc.related_key, primary_key(parent)}} | acc]
    end
  end
end
