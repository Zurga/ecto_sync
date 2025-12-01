defmodule EctoSync.PubSub do
  @moduledoc false
  @behaviour Phoenix.PubSub.Adapter

  use Supervisor

  alias Phoenix.PubSub.PG2

  @impl true
  def node_name(_), do: node()

  @impl true
  def broadcast(adapter_name, topic, {{schema_event, identifiers}, ref}, _dispatcher) do
    message =
      case :persistent_term.get({EctoSync, schema_event}, schema_event) do
        {schema, event} ->
          {schema, event, {identifiers, ref}}
          # label -> {label, event, {identifiers, ref}}
      end

    pubsub =
      Module.split(adapter_name)
      |> Enum.at(0)
      |> String.to_existing_atom()

    Registry.dispatch(pubsub, topic, fn entries ->
      for {pid, _} <- entries do
        send(pid, {EctoSync, message})
      end
    end)

    {:error, :already_dispatched}
  end

  @impl true
  defdelegate direct_broadcast(adapter_name, node_name, topic, message, dispatcher), to: PG2

  # @impl true
  defdelegate start_link(opts), to: PG2

  @impl true
  defdelegate init(args), to: PG2
end
