defmodule EctoSync do
  @moduledoc """
  A Cache updater and router for events emitted when database entries are updated. Subscribers can provide a list of records that they want to receive updates on. Additionally, they can provide a function that will act as a means of authorization on the updates they should get.

  Using the subscribe function a process can subscribe to all messages for a given struct.
  """

  @type subscriptions() :: list({EctoWatch.watcher_identifier(), term()})
  @type schema_or_list_of_schemas() :: Ecto.Schema.t() | list(Ecto.Schema.t())
  @events ~w/inserted updated deleted/a
  @cache_name :ecto_sync
  @counter_key {__MODULE__, :repo_counters}

  defstruct pub_sub: nil,
            repo: nil,
            cache_name: nil,
            watchers: [],
            schemas: nil

  use Supervisor
  require Logger
  alias EctoSync.{Config, PubSub, Subscriber, Syncer, Watcher}

  alias Ecto.Association.{BelongsTo, Has, ManyToMany}
  import EctoSync.Helpers

  def decrement_row_ref(keyable) do
    update_counter(keyable, &(&1 - 1))
  end

  @doc """
  Gets a value from cache.

  See `sync/3` for options available.
  """
  def get(event, opts \\ []) do
    config = Config.new(event, opts)
    Syncer.sync(:cached, config)
  end

  def increment_row_ref(keyable) do
    update_counter(keyable, &(&1 + 1))
  end

  @impl true
  @doc false
  def init(state) do
    schemas =
      state.watchers
      |> Enum.map(fn
        {%{table_name: table}, _, _} ->
          table

        tuple ->
          elem(tuple, 0)
      end)
      |> Enum.uniq()
      |> EctoGraph.new()

    :persistent_term.put(__MODULE__, %{state | schemas: schemas})

    children = [
      {Cachex, state.cache_name},
      {Phoenix.PubSub, name: state.pub_sub, adapter: PubSub},
      {Watcher, [repo: state.repo, pub_sub: state.pub_sub, watchers: state.watchers]},
      {Registry, keys: :duplicate, name: EventRegistry}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc """
  Determines whether or not the event was sent by this process
  """
  def should_update?({_schema, _event, {_, ref}} = sync_params) do
    current = get_row_ref(sync_params)

    if is_nil(current) do
      true
    else
      update_counter(sync_params, fn _ -> ref end)
      current < ref
    end
  end

  @doc """
  Starts EctoSync. 

  ## Options
  - `:cache_name`, the name of the cache that used to cache changes
  - `:repo`, the repo to track changes in.
  - `:watchers`, a list of watchers that EctoSync will pass on to EctoWatch.
  - `:pub_sub`, the PubSub module to use for sending events, defaults to `:ecto_sync_pub_sub`.
  """
  def start_link(opts \\ [name: __MODULE__]) do
    state =
      %__MODULE__{
        cache_name: opts[:cache_name] || @cache_name,
        repo: opts[:repo],
        pub_sub: opts[:pub_sub] || :ecto_sync_pub_sub,
        watchers: opts[:watchers]
      }

    Supervisor.start_link(__MODULE__, state, name: __MODULE__)
  end

  @doc """
  Subscribe to Ecto.Schema(s) provided. The input can be one of following:
   - an Ecto.Schema struct, 
   - a list of Ecto.Schema struct, 
   - an EctoWatch identifier.

  When an Ecto.Schema struct or list of structs is provided, the process is subscribed to all `:updated` and `:deleted` events for the Ecto.Schema that represents the struct.

  ### Examples

      iex> defmodule Test do
      ...>   use Ecto.Schema
      ...>   schema do
      ...>     field :name, :string
      ...>   end
      ...> end

      iex> EctoSync.subscribe(Test)
      [{{Test, :inserted}, nil}, {{Test, :updated}, nil}, {{Test, :deleted}, nil}]

      iex> EctoSync.subscribe(%Test{id: 1})
      [{{Test, :updated}, 1}, {{Test, :deleted}, 1}]
  """
  @spec subscribe(schema_or_list_of_schemas() | EctoWatch.watcher_identifier(), list()) ::
          list(term())
  defdelegate subscribe(values, opts \\ []), to: Subscriber

  @spec subscriptions(EctoWatch.watcher_identifier(), term()) :: [{pid(), Registry.value()}]
  @doc """
  Returns a list of pids that are subscribed to the given watcher identifier.
  """
  defdelegate subscriptions(watcher_identifier, id \\ nil), to: Subscriber

  @doc """
  Performs the actual syncing of a given value. Based on the input and the event, certain behaviour can be expected.

  ## `:inserted` event
  |struct|input|output|
  |------------|-----|------|
  |`%Post{id: 1}`|`[]`|` [%Post{id: 1}]`|
  |`%Post{id: 2}`|`[%Post{id: 1}]`|` [%Post{id: 1}, %Post{id: 2}]`|
  |`%Comment{id: 1, post_id: 1}`|`[%Post{id: 1, comments: []}]`|`[%Post{id: 1, comments: [%Comment{id: 1}]}]`|

  ## `:updated` event

  |struct|input|output|
  |------------|-----|------|
  |`%Post{id: 1, name: "Updated name"}`|`%Post{id:1, name: "Name"}`|`%Post{id: 1}, name: "Updated name"}`|
  |`%Post{id: 2}`|`[%Post{id: 1}]`|`[%Post{id: 1}, %Post{id: 2}]`|
  |`%Comment{id: 1, post_id: 1}`|`[%Post{id: 1, comments: []}]`|` [%Post{id: 1, comments: [%Comment{id: 1}]}]`|
  |`%Comment{id: 1, post_id: 2}`|`[%Post{id: 1, comments: [%Comment{id: 1}]}, %Post{id: 2, comments: []}]`|` [%Post{id: 1, comments: []}, %Post{id: 2, comments: [%Comment{id: 1}]}]`|

  ## `:deleted` event

  |struct|input|output|
  |------------|-----|------|
  |`%Post{id: 1}`|`[]`|`[]`|
  |`%Post{id: 1}`|`%Post{id: 1}`|`%Post{id: 1}`|
  |`%Post{id: 2}`|`[%Post{id: 1}, %Post{id: 2}]`|`[%Post{id: 1}]`|
  |`%Comment{id: 1, post_id: 1}`|` [%Post{id: 1, comments: [%Comment{id: 1}]}]`|`[%Post{id: 1, comments: []}]`|

  """
  @type syncable() :: term() | Ecto.Schema.t() | list(Ecto.Schema.t())
  @spec sync(syncable(), {{struct(), atom()}, {integer() | String.t(), reference()}}) ::
          syncable()
  def sync(value, sync_params, opts \\ [force: false])

  def sync(value, sync_params, opts)
      when is_list(value) or is_struct(value) or is_nil(value) or is_map(value) do
    if should_update?(sync_params) or opts[:force] do
      config = Config.new(sync_params, opts)
      Syncer.sync(value, config)
    else
      value
    end
  end

  def sync(value, _sync_config, _opts), do: value

  @doc """

  """
  defdelegate subscribed?(value, opts), to: Subscriber

  @doc """
  Unsubscribe the current process from events. Possible inputs are:
    - `Ecto.Schema` struct
    - a list of `Ecto.Schema` structs
    - watcher_identifier tuple and id

  ### Examples
      iex> EctoSync.unsubscribe(person)
      :ok

      iex> EctoSync.unsubscribe([person1, person2])
      :ok

      iex> EctoSync.unsubscribe({Person, :updated}, 1)
      :ok
      
      iex> EctoSync.unsubscribe({Person, :inserted}, nil)
      :ok
  """
  @spec unsubscribe(schema_or_list_of_schemas() | Watcher.watcher_identifier(), term()) ::
          list(term())
  defdelegate unsubscribe(alue, id \\ []), to: Subscriber

  @spec watchers(list(), module(), list()) :: list()
  @doc """
  Create watcher specifications for a given schema.

    * `schema` - The Ecto.Schema to subscribe to.

  ### Options
    - `:extra_columns`, which extra columns should be included  
    - `:assocs`, a preload like keyword list of associations to subscribe to. 
      If assocs are specified in the options, the necessary extra_columns will be added 
      or merged to the `:extra_columns` option. 

  ### Examples
  Assuming the same schemas are present as in the Use Cases page.

  ```elixir
  # Generate events for a schema without associations
  watchers(MyApp.User)
  # => [
  #      {MyApp.User, :inserted, []},
  #      {MyApp.User, :updated, []},
  #      {MyApp.User, :deleted, []}
  #    ]

  # Generate events for all associations
  watchers(User, assocs: [posts: [:comments, :tags, :labels]])
  # => Includes events for:
  # [
  #   {Label, :deleted, [extra_columns: []]},
  #   {Label, :inserted, [extra_columns: []]},
  #   {Label, :updated, [extra_columns: []]},
  #   {Person, :deleted, [extra_columns: []]},
  #   {Person, :inserted, [extra_columns: []]},
  #   {Person, :updated, [extra_columns: []]},
  #   {Post, :deleted, [extra_columns: [:person_id]]},
  #   {Post, :deleted, [extra_columns: [:post_id]]},
  #   {Post, :inserted, [extra_columns: [:person_id]]},
  #   {Post, :inserted, [extra_columns: [:post_id]]},
  #   {Post, :updated, [extra_columns: [:person_id]]},
  #   {Post, :updated, [extra_columns: [:post_id]]},
  #   {PostsTags, :deleted, [extra_columns: [:tag_id, :post_id]]},
  #   {PostsTags, :inserted, [extra_columns: [:tag_id, :post_id]]},
  #   {PostsTags, :updated, [extra_columns: [:tag_id, :post_id]]},
  #   {Tag, :deleted, [extra_columns: []]},
  #   {Tag, :inserted, [extra_columns: []]},
  #   {Tag, :updated, [extra_columns: []]}
  # ]
  ```
  """
  def watchers(watchers \\ [], schema)

  def watchers(watchers, schema) when is_list(watchers) and is_atom(schema),
    do: watchers(watchers, schema, [])

  def watchers(schema, opts) when is_atom(schema), do: watchers([], schema, opts)

  @doc "See `watchers/2`."
  def watchers(watchers, schema, opts) do
    unless ecto_schema_mod?(schema) do
      raise ArgumentError, "Expected a module alias to an Ecto Schema"
    end

    do_watchers(watchers, schema, opts)
    |> Enum.group_by(
      &{elem(&1, 0), elem(&1, 1), elem(&1, 2)[:label]},
      &elem(&1, 2)[:extra_columns]
    )
    |> Enum.map(fn {{mod, event, label}, extra_columns} ->
      {mod, event,
       [label: label, extra_columns: List.flatten(extra_columns) |> Enum.reject(&is_nil/1)]}
    end)
  end

  defp do_watchers(
         watchers,
         %ManyToMany{join_through: join_through, related: related, join_keys: join_keys},
         opts
       )
       when is_atom(join_through) do
    [{owner_key, _}, {related_key, _}] = join_keys

    [{join_through, extra_columns: [owner_key, related_key]}, {related, opts}]
    |> Enum.reduce(watchers, fn {schema, opts}, watchers ->
      do_watchers(watchers, schema, opts)
    end)
  end

  defp do_watchers(
         watchers,
         %ManyToMany{
           join_through: join_through,
           related: related,
           join_keys: join_keys
         },
         opts
       )
       when is_binary(join_through) do
    [{owner_key, _}, {related_key, _}] = join_keys
    association_columns = [owner_key, related_key]

    @events
    |> Enum.reduce(watchers, fn event, watchers ->
      label = encode_watcher_identifier({join_through, event})

      :persistent_term.put({EctoSync, {join_through, event}}, label)
      :persistent_term.put({EctoSync, label}, {join_through, event})

      [
        {%{
           table_name: join_through,
           primary_key: :id,
           columns: association_columns,
           association_columns: association_columns
         }, event, extra_columns: association_columns, label: label}
        | watchers
      ]
    end)
    |> do_watchers(related, opts)
  end

  defp do_watchers(watchers, %BelongsTo{related: related}, opts) do
    do_watchers(watchers, related, opts)
  end

  defp do_watchers(watchers, %Has{related: related, related_key: related_key}, opts) do
    do_watchers(watchers, related, Keyword.put(opts, :extra_columns, [related_key]))
  end

  defp do_watchers(watchers, nil, _opts), do: watchers

  defp do_watchers(watchers, schema, opts) do
    {assoc_fields, opts} =
      Keyword.pop(opts, :assocs, [])

    {columns, opts} = Keyword.pop(opts, :extra_columns, [])

    extra_columns = merge_extra_columns(schema, columns, assoc_fields)

    opts = Keyword.put(opts, :extra_columns, extra_columns)

    watchers =
      (watchers ++
         Enum.map(@events, fn event ->
           label = encode_watcher_identifier({Keyword.get(opts, :label, schema), event})

           :persistent_term.put({EctoSync, {schema, event}}, label)
           :persistent_term.put({EctoSync, label}, {schema, event})

           opts = Keyword.put(opts, :label, label)
           {schema, event, opts}
         end))
      |> Enum.uniq()

    Enum.reduce(assoc_fields, watchers, fn
      key, watchers when is_tuple(key) or is_atom(key) ->
        {key, nested} =
          case key do
            {_, _} -> key
            key -> {key, []}
          end

        assoc = schema.__schema__(:association, key)
        do_watchers(watchers, assoc, Keyword.put([], :assocs, nested))

      _, watchers ->
        watchers
    end)
  end

  defp merge_extra_columns(schema, columns, assoc_fields) do
    Enum.reduce(assoc_fields, columns, fn key, columns ->
      key =
        case key do
          {key, _} -> key
          _ -> key
        end

      assoc_info = schema.__schema__(:association, key)

      case assoc_info do
        %BelongsTo{owner_key: key} ->
          [key | columns]

        nil ->
          Logger.warning("#{schema} does not have associated key: #{inspect(key)}")
          columns

        _ ->
          columns
      end
    end)
    |> Enum.reverse()
    |> Enum.uniq()
  end

  defp coerce_to_ref_key({%schema_mod{}, event_or_id}), do: {schema_mod, event_or_id}
  defp coerce_to_ref_key({_ecto_schema, _event_or_id} = key), do: key

  defp coerce_to_ref_key({schema, event, {%{id: id}, _ref}}) do
    case event do
      :inserted = event -> {schema, event}
      _other -> {schema, id}
    end
  end

  defp coerce_to_ref_key(%Ecto.Changeset{data: struct}) do
    case struct.__meta__.state do
      :loaded -> coerce_to_ref_key(struct)
      :built -> {struct, :inserted}
    end
  end

  defp coerce_to_ref_key(%ecto_schema{} = struct) do
    id = Enum.map(ecto_schema.__schema__(:primary_key), &Map.get(struct, &1))
    {ecto_schema, id}
  end

  defp get_row_ref(keyable) do
    key = coerce_to_ref_key(keyable)
    counter_map = Process.get(@counter_key, %{})
    counter_map[key]
  end

  defp update_counter(keyable, fun) do
    key = coerce_to_ref_key(keyable)
    counter_map = Process.get(@counter_key, %{})
    updated = Map.update(counter_map, key, 1, fun)
    Process.put(@counter_key, updated)

    updated[key]
  end
end
