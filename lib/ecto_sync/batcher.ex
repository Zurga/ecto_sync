defmodule EctoSync.Batcher do
  @callback handle_sync(
              schema :: binary(),
              batch :: [Phoenix.LiveView.unsigned_params()],
              socket :: Phoenix.LiveView.Socket.t()
            ) ::
              {:noreply, Phoenix.LiveView.Socket.t()}
              | {:reply, map(), Phoenix.LiveView.Socket.t()}

  def on_mount({schema, opts}, _params, _session, socket) do
    handler = Keyword.fetch!(opts, :handler)
    max_size = Keyword.fetch!(opts, :max_size)
    timeout = Keyword.fetch!(opts, :timeout)

    socket =
      socket
      |> Phoenix.LiveView.attach_hook("batcher_info:batch:#{inspect(schema)}", :handle_info, fn
        {{schema, _}, _} = sync_args, socket ->
          socket =
            case add_to_batch(socket, schema, sync_args) do
              {socket, batch} when length(batch) == max_size ->
                flush_batch(socket, schema, handler)

              {socket, _} ->
                reschedule_flush_batch(socket, schema, timeout)
            end

          {:halt, socket}

        _msg, socket ->
          {:cont, socket}
      end)
      |> Phoenix.LiveView.attach_hook("batcher_info:flush:#{inspect(schema)}", :handle_info, fn
        {:flush_batch, schema}, socket ->
          socket =
            case get_batch(socket, schema) do
              batch when length(batch) > 0 -> flush_batch(socket, schema, handler)
              _ -> socket
            end

          {:halt, socket}

        _msg, socket ->
          {:cont, socket}
      end)

    {:cont, socket}
  end

  defp add_to_batch(socket, schema, params) do
    # note: batch schemas are in reverse order
    batch = [params | get_batch(socket, schema)]
    {put_batch(socket, schema, batch), batch}
  end

  defp flush_batch(socket, schema, handler) do
    socket = cancel_timeout(socket, schema)
    batch = get_batch(socket, schema) |> Enum.reverse()

    Enum.reduce(batch, socket, fn sync_args, socket ->
      handler.handle_sync(sync_args, socket)
    end)
    |> put_batch(schema, [])
  end

  defp reschedule_flush_batch(socket, schema, timeout) do
    socket = cancel_timeout(socket, schema)
    timeout_ref = Process.send_after(self(), {:flush_batch, schema}, timeout)
    socket |> put_timeout_ref(schema, timeout_ref)
  end

  defp cancel_timeout(socket, schema) do
    if timeout_ref = get_timeout_ref(socket, schema) do
      Process.cancel_timer(timeout_ref)
    end

    socket |> put_timeout_ref(schema, nil)
  end

  defp get_batch(socket, schema) do
    socket.private[{__MODULE__, schema, :batch}] || []
  end

  def get_timeout_ref(socket, schema) do
    socket.private[{__MODULE__, schema, :timeout_ref}]
  end

  defp put_batch(socket, schema, batch) do
    socket |> Phoenix.LiveView.put_private({__MODULE__, schema, :batch}, batch)
  end

  defp put_timeout_ref(socket, schema, timeout_ref) do
    socket |> Phoenix.LiveView.put_private({__MODULE__, schema, :timeout_ref}, timeout_ref)
  end
end
