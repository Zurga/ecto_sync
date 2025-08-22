defmodule EctoSync.Subscriber do
  @moduledoc false
  require Logger
  import EctoSync.Helpers

  alias Ecto.Association
  alias Ecto.Association.{BelongsTo, Has, HasThrough, ManyToMany}

  @events ~w/inserted updated deleted/a

  def subscribe(watcher_identifier_or_struct, id \\ nil)

  def subscribe(values, opts) when is_list(values) do
    values
    |> Enum.flat_map(&subscribe(&1, opts))
    |> add_opts(opts)
    |> Enum.uniq()
    |> IO.inspect()
    |> Enum.map(fn {{watcher_identifier, id}, opts} ->
      do_subscribe(watcher_identifier, id, opts)
    end)
  end

  def subscribe(schema_mod, event)
      when is_atom(schema_mod) and is_atom(event) and not is_nil(event) do
    [do_subscribe({schema_mod, event}, nil, [])]
  end

  def subscribe([value | _] = list, opts) when is_struct(value),
    do: Enum.flat_map(list, &subscribe(&1, opts))

  def subscribe(value, opts) when is_struct(value) do
    subscribe_events(value)
    |> add_opts(opts)
    |> Enum.concat(subscribe_events_assocs(value, opts[:assocs] || []))
    |> IO.inspect(label: :after_sub_assoc)
    |> then(fn events ->
      if opts[:inserted] do
        schema = get_schema(value)
        Enum.concat(events, {subscribe({schema, :inserted}, nil), opts})
      else
        events
      end
    end)
    |> Enum.uniq()
    |> Enum.sort()
    |> IO.inspect(label: :after_sort)
    |> Enum.map(fn {{watcher_identifier, id}, opts} ->
      do_subscribe(watcher_identifier, id, opts)
    end)
  end

  def subscribe(watcher_identifier, id) do
    Enum.map(subscribe_events(watcher_identifier, id), &do_subscribe(&1, id, []))
  end

  defp do_subscribe(watcher_identifier, id, opts) do
    encoded_identifier = get_encoded_label(watcher_identifier)

    pids =
      EctoSync.subscriptions(watcher_identifier, id)
      |> Enum.map(&elem(&1, 0))

    if self() not in pids do
      Logger.debug("EventRegistry | #{inspect({watcher_identifier, id})}")
      Registry.register(EventRegistry, {encoded_identifier, id}, opts)

      EctoWatch.subscribe(encoded_identifier, id)
    end

    {watcher_identifier, id}
  end

  def subscriptions(watcher_identifier, id) do
    encoded = get_encoded_label(watcher_identifier)

    Registry.lookup(EventRegistry, {encoded, id})
  end

  def subscribe_events(label_or_schema, assoc \\ nil)

  def subscribe_events(struct, %BelongsTo{field: field}) do
    struct
    |> Map.get(field)
    |> subscribe_events()
  end

  def subscribe_events(struct, %Has{related_key: related_key, related: schema, field: field}) do
    parent_id = primary_key(struct)
    assoc_field = {related_key, parent_id}
    assocs = Map.get(struct, field)

    [{{schema, :inserted}, assoc_field} | subscribe_events(struct)] ++
      [Enum.map(assocs, &subscribe_events/1)]
  end

  def subscribe_events(struct, %HasThrough{through: [k | through]}),
    do:
      subscribe_events_assocs(
        Map.get(struct, k) |> IO.inspect(label: :assoc),
        through |> IO.inspect(label: :throug)
      )

  def subscribe_events(struct, %ManyToMany{
        join_through: join_through,
        join_keys: [{parent_key, _} | _]
      }) do
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

  def unsubscribe(value, opts \\ [])

  def unsubscribe(watcher_identifier, id)
      when is_tuple(watcher_identifier) or is_atom(watcher_identifier) do
    id = (is_list(id) && nil) || id

    try do
      encoded_identifier =
        watcher_identifier
        |> get_encoded_label()

      case EctoWatch.unsubscribe(encoded_identifier, id) do
        :ok -> Registry.unregister(EventRegistry, {encoded_identifier, id})
        error -> error
      end
    catch
      ArgumentError ->
        raise ArgumentError, "no watcher found for #{inspect(watcher_identifier)}"
    end
  end

  def unsubscribe([value | _] = values, opts) when is_struct(value) do
    Enum.flat_map(values, &unsubscribe(&1, opts))
  end

  def unsubscribe(value, opts) when is_struct(value) do
    subscribe_events(value)
    |> Enum.concat(
      subscribe_events_assocs(value, opts[:assocs] || [], fn parent, assoc_info ->
        subscribe_events(parent, assoc_info)
      end)
    )
    |> Enum.map(fn {watcher_identifier, id} = event ->
      unsubscribe(watcher_identifier, id)
      event
    end)
  end

  defp subscribe_events_assocs(parent, assoc_keys, acc \\ [])

  defp subscribe_events_assocs(%Ecto.Association.NotLoaded{}, _, acc), do: acc

  defp subscribe_events_assocs(parents, assoc_keys, acc) when is_list(parents) do
    Enum.reduce(
      parents,
      acc,
      &subscribe_events_assocs(&1, assoc_keys, &2)
    )
    |> List.flatten()
  end

  defp subscribe_events_assocs(parent, nil, acc), do: [subscribe_events(parent, nil) | acc]

  defp subscribe_events_assocs(parent, assoc_keys, acc) when is_list(assoc_keys) do
    Enum.reduce(assoc_keys, acc, &subscribe_events_assocs(parent, &1, &2))
    |> List.flatten()
  end

  defp subscribe_events_assocs(nil, _, acc), do: acc

  defp subscribe_events_assocs(parent, true, acc) when is_struct(parent) do
    walk_preloaded_assocs(parent, acc, fn key, assoc_info, assoc, acc ->
      IO.inspect(assoc_info)
      subscribe_events(parent, assoc_info) ++ subscribe_events(assoc) ++ acc
    end)
    |> Enum.filter(fn
      [] -> false
      _ -> true
    end)
  end

  defp subscribe_events_assocs(parent, assoc_keys, acc) when is_struct(parent) do
    {key, nested} =
      case assoc_keys do
        {_, _} -> assoc_keys
        key -> {key, nil}
      end

    opts = [assocs: nested]

    schema = get_schema(parent)
    assoc_info = schema.__schema__(:association, key)

    parent
    |> Map.get(key)
    |> case do
      nil ->
        acc

      [] ->
        events =
          subscribe_events(parent, assoc_info)
          |> add_opts(opts)
          |> IO.inspect(label: :events)

        events ++ acc

      value when is_list(value) or not is_struct(value, Association.NotLoaded) ->
        events =
          subscribe_events(parent, assoc_info)
          |> add_opts(opts)
          |> IO.inspect(label: :events)

        subscribe_events_assocs(value, nested, events ++ acc)

      _ ->
        events =
          subscribe_events(parent, nil)
          |> add_opts(opts)
          |> IO.inspect(label: :events)

        {related, related_key} =
          case assoc_info do
            %ManyToMany{join_keys: [{related_key, _} | _], join_through: related} ->
              {related, related_key}

            %{related_key: related_key, related: related} ->
              {related, related_key}
          end

        ([{{related, :inserted}, {related_key, primary_key(parent)}}]
         |> add_opts(opts)) ++
          events ++
          acc
    end
  end

  defp add_opts(list, opts) when is_list(list), do: List.flatten(list) |> Enum.map(&{&1, opts})
  defp add_opts(tuple, opts), do: {tuple, opts}
end
