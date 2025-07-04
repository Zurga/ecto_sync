defmodule EctoSync.PubSub do
  @moduledoc false
  @behaviour Phoenix.PubSub.Adapter

  use Supervisor

  alias Phoenix.PubSub.PG2

  @impl true
  def node_name(_), do: node()

  @impl true
  def broadcast(adapter_name, topic, {schema_event, %{id: id}} = message, _dispatcher) do
    PG2.broadcast(adapter_name, topic, message, __MODULE__)
    schema_event = :persistent_term.get({EctoSync, schema_event}, schema_event)

    pubsub =
      Module.split(adapter_name)
      |> Enum.at(0)
      |> String.to_existing_atom()

    Registry.dispatch(pubsub, topic, fn entries ->
      if entries != [] do
        ref = :erlang.make_ref()

        for {pid, _} <- entries do
          send(pid, {schema_event, {id, ref}})
        end
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
