defmodule EctoSyncTest.AdapterTests do
  use EctoSync.RepoCase, async: false

  @watchers [{Label, :inserted, label: :label}]
            |> EctoSync.watchers(Post,
              assocs: [:tags, :labels, person: [:favourite_tags]],
              extra_columns: [:person_id]
            )

  describe "postgres WAL adapter" do
    setup [:do_setup, :wal]

    test "events are sent" do
      EctoSync.subscribe({Post, :inserted})

      EctoSync.subscriptions({Post, :inserted})

      {:ok, %{id: id}} = TestRepo.insert(%Post{})

      receive do
        {:ecto_sync, {Post, :inserted, _} = sync_args} ->
          assert id == EctoSync.get(sync_args).id
      after
        1000 ->
          raise "no message on inserted"
      end
    end
  end

  describe "postgres NOTIFY adapter" do
    setup [:do_setup, :notify]

    test "events are sent" do
      EctoSync.subscribe({Post, :inserted})

      EctoSync.subscriptions({Post, :inserted})

      {:ok, %{id: id}} = TestRepo.insert(%Post{})

      receive do
        {:ecto_sync, {Post, :inserted, _} = sync_args} ->
          assert id == EctoSync.get(sync_args).id
      after
        1000 ->
          raise "no message on inserted"
      end
    end
  end

  defp do_setup(_) do
    start_supervised!(TestRepo)

    :ok
  end

  defp wal(_) do
    start_link_supervised!({
      EctoSync,
      repo: TestRepo, adapter: EctoSync.Adapters.Postgres.Wal, watchers: @watchers
    })

    :ok
  end

  defp notify(_) do
    start_link_supervised!({
      EctoSync,
      repo: TestRepo, adapter: EctoSync.Adapters.Postgres.Notify, watchers: @watchers
    })

    :ok
  end
end
