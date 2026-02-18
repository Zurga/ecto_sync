defmodule EctoSync.Adapters.Postgres.Replication do
  use Postgrex.ReplicationConnection

  alias EctoSync.Adapters.Postgres.Protocol

  require Logger

  defstruct [
    :publications,
    :protocol,
    :slot,
    :state,
    :table_schema_map,
    subscribers: %{}
  ]

  def start_link(opts) do
    conn_opts = [auto_reconnect: true]
    publications = opts[:publications] || raise ArgumentError, message: "`:publications` missing"
    slot = opts[:slot] || raise ArgumentError, message: "`:slot` missing"

    Postgrex.ReplicationConnection.start_link(
      __MODULE__,
      {slot, publications},
      conn_opts ++ opts
    )
  end

  @impl true
  def init({slot, pubs}) do
    {:ok,
     %__MODULE__{
       slot: slot,
       publications: pubs,
       protocol: Protocol.new()
     }}
  end

  @impl true
  def handle_connect(%__MODULE__{slot: slot} = state) do
    query = """
    CREATE_REPLICATION_SLOT #{slot} TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT
    """

    Logger.debug("[create slot] query=#{query}")

    {:query, query, %{state | state: :create_slot}}
  end

  @impl true
  def handle_connect(state) do
    {:noreply, state}
  end

  @impl true
  def handle_result(
        [%Postgrex.Result{} | _],
        %__MODULE__{state: :create_slot, publications: pubs, slot: slot} = state
      ) do
    opts = [proto_version: 1, publication_names: pubs]

    query = "START_REPLICATION SLOT #{slot} LOGICAL 0/0 #{escape_options(opts)}"

    Logger.debug("[start streaming] query=#{query}")

    {:stream, query, [], %{state | state: :streaming}}
  end

  @impl true
  def handle_data(msg, state) do
    {return_msgs, tx, protocol} =
      Protocol.handle_message(msg, state.protocol)

    if not is_nil(tx) do
      Enum.map(tx.operations, fn %{type: type, table: table} = operation ->
        EctoSync.Publisher.publish(
          table,
          type,
          (type == :deleted && operation.old_record) || operation.record
        )
      end)
    end

    {:noreply, return_msgs, %{state | protocol: protocol}}
  end

  defp escape_options(opts) do
    parts =
      Enum.map_intersperse(opts, ", ", fn {k, v} -> [Atom.to_string(k), ?\s, escape_string(v)] end)

    [?\s, ?(, parts, ?)]
  end

  defp escape_string(value) do
    [?', :binary.replace(to_string(value), "'", "''", [:global]), ?']
  end
end
