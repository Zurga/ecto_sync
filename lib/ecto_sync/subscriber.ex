defmodule EctoSync.Subscriber do
  @moduledoc false
  require Logger
  import EctoSync.Helpers

  alias Ecto.Association
  alias Ecto.Association.{BelongsTo, Has, HasThrough, ManyToMany}

  @type watcher_identifier() :: {atom(), atom()} | atom()
  @events ~w/inserted updated deleted/a

  def subscribe(watcher_identifier_or_struct, opts \\ [])

  def subscribe(values, opts) when is_list(values) do
    values
    |> Enum.flat_map(&subscribe(&1, opts))
    |> add_opts(opts)
    |> merge_assocs()
    |> Enum.uniq()
    |> Enum.map(fn {{watcher_identifier, id}, opts} ->
      do_subscribe(watcher_identifier, id, opts)
    end)
  end

  def subscribe({schema_mod, event} = watcher_identifier, opts)
      when is_atom(schema_mod) and is_atom(event) and not is_nil(event) do
    watcher_identifier
    |> subscribe_events()
    |> add_opts(opts)
    |> Enum.map(fn {{watcher_identifier, id}, opts} ->
      do_subscribe(watcher_identifier, id, opts)
    end)
  end

  def subscribe([value | _] = list, opts) when is_struct(value),
    do: Enum.flat_map(list, &subscribe(&1, opts))

  def subscribe(%schema{} = value, opts) when is_struct(value) do
    subscribe_events(value)
    |> add_opts(opts)
    |> Enum.concat(subscribe_events_assocs(value, opts[:assocs] || []))
    |> then(fn events ->
      if opts[:inserted] do
        [{{{schema, :inserted}, nil}, opts} | events]
      else
        events
      end
    end)
    |> merge_assocs()
    |> Enum.map(fn {{watcher_identifier, id}, opts} ->
      do_subscribe(watcher_identifier, id, opts)
    end)
    |> Enum.uniq()
  end

  def subscribe(watcher_identifier, id) when is_binary(id) or is_number(id) do
    watcher_identifier
    |> subscribe_events(id)
    |> Enum.map(&do_subscribe(&1, id, []))
  end

  def subscribe(label, []) when is_atom(label) do
    label
    |> subscribe_events(label)
    |> Enum.map(&do_subscribe(&1, nil, preloads: []))
  end

  defp do_subscribe(watcher_identifier, id, opts) do
    encoded_identifier =
      get_encoded_label(watcher_identifier)

    pids =
      subscriptions(watcher_identifier, id)
      |> Enum.map(&elem(&1, 0))

    if self() not in pids do
      # Logger.debug("EventRegistry | #{inspect({watcher_identifier, id, opts})}")
      Logger.debug("EventRegistryRegister | #{inspect({encoded_identifier, id, opts})}")

      Registry.register(
        EventRegistry,
        {encoded_identifier, id},
        opts
      )

      # Watcher.subscribe(encoded_identifier, id)
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
    parent_id = id(struct)
    assoc_field = {related_key, parent_id}
    assocs = Map.get(struct, field)
    [{{schema, :inserted}, assoc_field}] ++ [Enum.map(assocs, &subscribe_events/1)]
  end

  def subscribe_events(struct, %HasThrough{through: through}) do
    preloads =
      through
      |> Enum.reverse()
      |> Enum.reduce([], fn k, acc ->
        [{k, acc}]
      end)

    subscribe_events_assocs(struct, preloads)
  end

  def subscribe_events(struct, %ManyToMany{
        join_through: join_through,
        join_keys: [{parent_key, _} | _]
      }) do
    id = id(struct)

    Enum.map(@events, &{{join_through, &1}, {parent_key, id}})
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

  def subscribe_events(%schema{} = value, _) when is_struct(value) do
    id = id(value)

    if ecto_schema_mod?(schema) do
      ~w/updated deleted/a
      |> Enum.map(&{{schema, &1}, {primary_key(schema), id}})
    else
    end
  end

  def subscribe_events({schema, event} = watcher_identifier, id)
      when is_atom(schema) and event in [:all | @events] do
    case watcher_identifier do
      {schema, :all} ->
        Enum.map(@events, &{{schema, &1}, {primary_key(schema), id}})

      {_, :inserted} ->
        [{watcher_identifier, nil}]

      {_schema, _event} ->
        [{watcher_identifier, id}]
    end
  end

  def subscribed?({_schema, _event} = watcher_identifier, id) do
    pids =
      subscriptions(watcher_identifier, id)
      |> Enum.map(&elem(&1, 0))

    self() in pids
  end

  @doc """
  Unsubscribe from notifications from watchers that you previously subscribe. It
  receives the same params for `subscribe/2`.

  Examples:

      iex> EctoSync.Watcher.unsubscribe({Comment, :updated})
      iex> EctoSync.Watcher.unsubscribe({Comment, :updated}, {:post_id, post_id})
  """
  @spec unsubscribe(watcher_identifier(), term()) :: :ok | {:error, term()}
  def unsubscribe(value, opts \\ [])

  def unsubscribe(watcher_identifier, id) when is_binary(watcher_identifier) do
    [:updated, :deleted]
    |> Enum.map(&unsubscribe({watcher_identifier, &1}, id))
  end

  def unsubscribe(watcher_identifier, id)
      when is_tuple(watcher_identifier) or is_atom(watcher_identifier) do
    id = (is_list(id) && nil) || id

    # try do
    encoded_identifier = get_encoded_label(watcher_identifier)

    # case Watcher.unsubscribe(encoded_identifier, id) do
    Registry.unregister(EventRegistry, {encoded_identifier, id})
    # error -> error
    #   end
    # catch
    #   ArgumentError ->
    #     raise ArgumentError, "no watcher found for #{inspect(watcher_identifier)}"
    # end
  end

  def unsubscribe([value | _] = values, opts) when is_struct(value) do
    Enum.flat_map(values, &unsubscribe(&1, opts))
  end

  def unsubscribe(value, opts) when is_struct(value) do
    subscribe_events(value)
    |> add_opts(opts)
    |> Enum.concat(subscribe_events_assocs(value, opts[:assocs] || []))
    |> Enum.map(fn {{watcher_identifier, id} = event, _} ->
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

  defp subscribe_events_assocs(parent, nil, acc),
    do: [subscribe_events(parent, nil) |> add_opts(assocs: []) | acc]

  defp subscribe_events_assocs(parent, assoc_keys, acc) when is_list(assoc_keys) do
    Enum.reduce(assoc_keys, acc, &subscribe_events_assocs(parent, &1, &2))
    |> List.flatten()
  end

  defp subscribe_events_assocs(nil, _, acc), do: acc

  defp subscribe_events_assocs(parent, true, acc) when is_struct(parent) do
    walk_preloaded_assocs(parent, acc, fn _key, assoc_info, assoc, acc ->
      subscribe_events(parent, assoc_info) ++ subscribe_events(assoc) ++ acc
    end)
    |> Enum.filter(fn
      [] -> false
      _ -> true
    end)
  end

  defp subscribe_events_assocs(%schema{} = parent, assoc_keys, acc) when is_struct(parent) do
    {key, nested} =
      case assoc_keys do
        {key, []} -> {key, nil}
        {_, _} -> assoc_keys
        key -> {key, nil}
      end

    opts = [assocs: nested || []]

    assoc_info = schema.__schema__(:association, key)

    parent
    |> Map.get(key)
    |> case do
      empty when is_nil(empty) or empty == [] ->
        events =
          subscribe_events(parent, assoc_info)
          |> add_opts(opts)

        events ++ acc

      %Association.NotLoaded{} ->
        {related, related_key} =
          case assoc_info do
            %ManyToMany{join_keys: [{related_key, _} | _], join_through: related} ->
              {related, related_key}

            %{related_key: related_key, related: related} ->
              {related, related_key}
          end

        ([{{related, :inserted}, {related_key, id(parent)}}]
         |> Enum.map(&add_opts(&1, opts))) ++
          acc

      value ->
        opts =
          case assoc_info do
            %Association.Has{related_key: related_key} ->
              [parent: {related_key, id(parent)}] ++ opts

            _ ->
              opts
          end

        events =
          subscribe_events(parent, assoc_info)
          |> add_opts(opts)

        subscribe_events_assocs(value, nested, events ++ acc)
    end
  end

  defp add_opts(list, opts) when is_list(list),
    do: List.flatten(list) |> Enum.map(&add_opts(&1, opts))

  defp add_opts({{{_, _}, _}, _} = tuple, _opts), do: tuple
  defp add_opts(tuple, opts), do: {tuple, opts}

  defp merge_assocs(watchers) do
    watchers
    |> Enum.group_by(fn {identifier_id, _opts} -> identifier_id end, fn {_, opts} -> opts end)
    |> Enum.map(fn {watcher_identifier, opts} ->
      opts =
        Enum.reduce(opts, [], fn opt, acc ->
          kw_deep_merge(acc, opt)
        end)

      {watcher_identifier, opts}
    end)
  end
end
