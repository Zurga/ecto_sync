# Original code copied and maybe modified from EctoWatch
defmodule EctoSync.Adapters.Postgres.Notify do
  @moduledoc """
  A library to allow you to easily get notifications about database changes directly from PostgreSQL.
  """

  alias EctoSync.Helpers

  alias EctoSync.Adapters.Postgres.Notify.{
    WatcherServer,
    WatcherSupervisor,
    WatcherTriggerValidator
  }

  use Supervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(options) do
    # TODO:
    # Allow passing in options specific to Postgrex.Notifications.start_link/1
    # https://hexdocs.pm/postgrex/Postgrex.Notifications.html#start_link/1

    postgrex_notifications_options =
      options.repo_mod.config()
      |> Keyword.put(:name, :ecto_sync_postgrex_notifications)

    children = [
      {Postgrex.Notifications, postgrex_notifications_options},
      {WatcherSupervisor, options},
      {WatcherTriggerValidator, nil}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  @type watcher_identifier() :: {atom(), atom()} | atom()

  @doc """
  Subscribe to notifications from watchers.

  Examples:

      iex> EctoSync.Watcher.subscribe({Comment, :updated})

    When subscribing to a watcher with the `label` option specified as `:comment_updated_custom`:

      iex> EctoSync.Watcher.subscribe(:comment_updated_custom)

    You can subscribe to notifications just from specific primary key values:

      iex> EctoSync.Watcher.subscribe({Comment, :updated}, user_id)

    Or you can subscribe to notifications just from a specific foreign column (**the column must be in the watcher's `extra_columns` list):

      iex> EctoSync.Watcher.subscribe({Comment, :updated}, {:post_id, post_id})
  """
  @spec subscribe(watcher_identifier(), term()) :: :ok | {:error, term()}
  def subscribe(watcher_identifier, id \\ nil) do
    validate_watcher_running!()

    with :ok <- validate_identifier(watcher_identifier),
         {:ok, {_pub_sub_mod, _channel_name, debug?}} <-
           WatcherServer.pub_sub_subscription_details(watcher_identifier, id) do
      if(debug?, do: debug_log(watcher_identifier, "Subscribing to watcher"))
    else
      {:error, error} ->
        raise ArgumentError, error
    end
  end

  def unsubscribe(watcher_identifier, id \\ nil) do
    validate_watcher_running!()

    with :ok <- validate_identifier(watcher_identifier),
         {:ok, {pub_sub_mod, channel_name, debug?}} <-
           WatcherServer.pub_sub_subscription_details(watcher_identifier, id) do
      if(debug?, do: debug_log(watcher_identifier, "Unsubscribing to watcher"))

      Phoenix.PubSub.unsubscribe(pub_sub_mod, channel_name)
    else
      {:error, error} ->
        raise ArgumentError, error
    end
  end

  @doc """
  Returns details about a watcher for reflection purposes

  For example if you need to know what the function/triggers are in the database.

  Examples:

      iex> EctoSync.Watcher.subscribe({Comment, :updated})

    Or for a label:

      iex> EctoSync.Watcher.subscribe(:comment_updated_custom)
  """
  @spec details(watcher_identifier()) :: %{
          repo_mod: module(),
          schema_definition: %{
            schema_prefix: binary(),
            table_name: binary(),
            primary_key: binary(),
            columns: [atom()],
            association_columns: [atom()],
            label: term()
          },
          function_name: binary(),
          trigger_name: binary(),
          notify_channel: binary()
        }
  def details(watcher_identifier) do
    WatcherServer.details(watcher_identifier)
  end

  defp validate_identifier({schema_mod, update_type})
       when (is_atom(schema_mod) or is_binary(schema_mod)) and is_atom(update_type) do
    cond do
      is_binary(schema_mod) ->
        :ok

      !Helpers.ecto_schema_mod?(schema_mod) ->
        raise ArgumentError,
              "Expected atom to be an Ecto schema module. Got: #{inspect(schema_mod)}"

      update_type not in ~w[inserted updated deleted]a ->
        raise ArgumentError,
              "Unexpected update_type: #{inspect(update_type)}.  Expected :inserted, :updated, or :deleted"

      true ->
        :ok
    end
  end

  defp validate_identifier(label) when is_atom(label) do
    :ok
  end

  defp validate_identifier(other) do
    raise ArgumentError,
          "Invalid subscription (expected either `{schema_module, :inserted | :updated | :deleted}` or a label): #{inspect(other)}"
  end

  defp validate_watcher_running! do
    if !Process.whereis(__MODULE__) do
      raise "EctoSync.Watcher is not running. Please start it by adding it to your supervision tree or using EctoSync.Watcher.start_link/1"
    end
  end

  defp debug_log(watcher_identifier, message) do
    Helpers.debug_log(watcher_identifier, message)
  end
end
