defmodule EctoSync do
  @moduledoc """
  A Cache updater and router for events emitted when database entries are updated. Subscribers can provide a list of records that they want to receive updates on. Additionally, they can provide a function that will act as a means of authorization on the updates they should get.

  Using the subscribe function a process can subscribe to all messages for a given struct.
  """

  @type subscriptions() :: list({EctoWatch.watcher_identifier(), term()})
  @type schema_or_list_of_schemas() :: Ecto.Schema.t() | list(Ecto.Schema.t())
  @events ~w/inserted updated deleted/a
  @cache_name :ecto_sync

  defstruct pub_sub: nil,
            repo: nil,
            cache_name: nil,
            watchers: [],
            schemas: []

  use Supervisor
  require Logger
  alias EctoSync.{PubSub, Subscriber, SyncConfig, Syncer}
  alias Ecto.Association.{BelongsTo, Has, ManyToMany}
  import EctoSync.Helpers

  def start_link(opts \\ [name: __MODULE__]) do
    state =
      %__MODULE__{
        cache_name: opts[:cache_name] || @cache_name,
        repo: opts[:repo],
        pub_sub: opts[:pub_sub],
        watchers: opts[:watchers],
        schemas: opts[:schemas]
      }

    Supervisor.start_link(__MODULE__, state, name: __MODULE__)
  end

  @impl true
  def init(state) do
    :persistent_term.put(SyncConfig, state)

    children = [
      {Cachex, state.cache_name},
      {Phoenix.PubSub, name: :ecto_sync_pub_sub, adapter: PubSub},
      {EctoWatch, [repo: state.repo, pub_sub: :ecto_sync_pub_sub, watchers: state.watchers]},
      {Registry, keys: :duplicate, name: EventRegistry}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @spec watchers(list(), module(), list()) :: list()
  @doc """
  Create watcher specifications for a given schema.

    * `schema` - The Ecto.Schema to subscribe to.

  ### Options
    - `:extra_columns`, which extra columns should be included  
    - `:assocs`, a preload like keyword list of associations to subscribe to. If assocs are specified in the options, the necessary extra_columns will be added or merged to the `:extra_columns` option. 

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
  watchers(MyApp.User, assocs: [:comments, posts: :tags])
  # => Includes events for:
  # [
  #      {MyApp.User, :inserted, []}, {MyApp.User, :updated, []}, {MyApp.User, :deleted, []}
  #      {MyApp.Post, :inserted, []}, {MyApp.Post, :updated, []}, {MyApp.Post, :deleted, []}
  #      {MyApp.PostsTags, :inserted, []}, {MyApp.PostsTags, :updated, []}, {MyApp.PostsTags, :deleted, []}
  #      {{MyApp.Tag, :updated, []}, {MyApp.Tag, :deleted, []}
  #      {MyApp.Comment, :inserted, []}, {MyApp.Comment, :updated, []}, {MyApp.Comment, :deleted, []}
  # ]

  # Generate events with three-level deep associations, selectively including `posts` but not `comments`
  watchers(MyApp.User, assocs: [has: [posts: [has: false]]])
  # => Includes events for:
  #      {MyApp.User, :inserted, []}, {MyApp.User, :updated, []}, {MyApp.User, :deleted, []}
  #      {MyApp.Post, :inserted, []}, {MyApp.Post, :updated, []}, {MyApp.Post, :deleted, []}
  #      (Does not include `MyApp.Comment`)

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
  end

  @spec subscriptions(EctoWatch.watcher_identifier(), term()) :: [{pid(), Registry.value()}]
  @doc """
  Returns a list of pids that are subscribed to the given watcher identifier.
  """
  def subscriptions(watcher_identifier, id \\ nil) do
    Registry.lookup(EventRegistry, {watcher_identifier, id})
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
  # @spec subscribe(schema_or_list_of_schemas() | EctoWatch.watcher_identifier(), term(), term()) ::
  # subscriptions()
  def subscribe(values), do: subscribe(values, [])

  def subscribe(values, opts) when is_list(values) do
    values
    |> Enum.flat_map(&Subscriber.subscribe(&1, opts))
    |> Enum.uniq()
    |> Enum.map(fn {watcher_identifier, id} ->
      do_subscribe(watcher_identifier, id, opts)
    end)
  end

  def subscribe(value, opts) do
    for {watcher_identifier, id} <- Subscriber.subscribe(value, opts) do
      do_subscribe(watcher_identifier, id, opts)
    end
  end

  @doc """
  Performs the actual syncing of a given value. Based on the input and the event, certain behaviour can be expected.

  ## `:inserted` event
  |event_struct|input|output|
  |------------|-----|------|
  |`%Post{id: 1}`|`[]`|` [%Post{id: 1}]`|
  |`%Post{id: 2}`|`[%Post{id: 1}]`|` [%Post{id: 1}, %Post{id: 2}]`|
  |`%Comment{id: 1, post_id: 1}`|`[%Post{id: 1, comments: []}]`|` [%Post{id: 1, comments: [%Comment{id: 1}]}]`|

  ## `:updated` event


  ## `:deleted` event

  """
  defdelegate sync(value, sync_config), to: Syncer

  defdelegate unsubscribe(watcher_identifier, id), to: Subscriber

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
      label = String.to_atom("#{join_through}_#{event}")

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

    extra_columns =
      merge_extra_columns(schema, columns, assoc_fields)

    opts = Keyword.put(opts, :extra_columns, extra_columns)

    watchers =
      (watchers ++ Enum.map(@events, &{schema, &1, opts}))
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

  defp do_subscribe(watcher_identifier, id, opts) do
    if self() not in subscriptions(watcher_identifier, id) do
      Logger.debug("EventRegistry | #{inspect({watcher_identifier, id})}")
      Registry.register(EventRegistry, {watcher_identifier, id}, opts)
      EctoWatch.subscribe(watcher_identifier, id)
      {watcher_identifier, id}
    end
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
  end
end
