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
            schemas: [],
            graph: nil,
            join_modules: nil,
            edge_fields: nil

  use Supervisor
  require Logger
  alias EctoSync.{PubSub, Subscriber, Syncer}

  alias Ecto.Association.{BelongsTo, Has, ManyToMany}
  import EctoSync.Helpers

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

  @impl true
  @doc false
  def init(state) do
    {vertex_pairs, join_modules, edge_fields} =
      state.watchers
      |> Enum.map(fn
        {%{table_name: table}, _, _} ->
          table

        tuple ->
          elem(tuple, 0)
      end)
      |> Enum.uniq()
      |> EctoSync.Graph.new()

    :persistent_term.put(__MODULE__, %{
      state
      | graph: vertex_pairs,
        join_modules: join_modules,
        edge_fields: edge_fields
    })

    children = [
      {Cachex, state.cache_name},
      {Phoenix.PubSub, name: state.pub_sub, adapter: PubSub},
      {EctoWatch, [repo: state.repo, pub_sub: state.pub_sub, watchers: state.watchers]},
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
  end

  @spec subscriptions(EctoWatch.watcher_identifier(), term()) :: [{pid(), Registry.value()}]
  @doc """
  Returns a list of pids that are subscribed to the given watcher identifier.
  """
  defdelegate subscriptions(watcher_identifier, id \\ nil), to: Subscriber

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
  def sync(value, sync_config, opts \\ [])
      when is_list(value) or is_struct(value) or is_nil(value) or is_map(value),
      do: Syncer.sync(value, sync_config, opts)

  def sync(value, _sync_config, _opts), do: value

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
  @spec unsubscribe(schema_or_list_of_schemas() | EctoWatch.watcher_identifier(), term()) ::
          list(term())
  defdelegate unsubscribe(value, id \\ []), to: Subscriber

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
  end
end
