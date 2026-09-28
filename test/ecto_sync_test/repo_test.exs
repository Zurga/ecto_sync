defmodule EctoSync.RepoTest do
  use EctoSync.RepoCase, async: false
  import EctoSync
  import EctoSync.Helpers
  require Ecto.Query

  setup [:do_setup]

  describe "EctoSync.Repo.__prepare_op__" do
    test "raises will decrement the local counter", %{person: person} do
      current_counter = get_local_id_counters()

      assert_raise Ecto.ChangeError,
                   "value `\"blablabla\"` for `Person.id` in `update` does not match type :binary_id",
                   fn ->
                     person
                     |> Ecto.Changeset.change(%{id: "blablabla"})
                     |> TestSyncRepo.update()
                   end

      assert current_counter == get_local_id_counters()
    end

    test "invalid changesets will decrement local counter", %{person: person} do
      current_counter = get_local_id_counters()

      person
      |> Ecto.Changeset.change(%{name: ""})
      |> Ecto.Changeset.validate_length(:name, min: 1)
      |> TestSyncRepo.update()

      assert current_counter == get_local_id_counters()
    end

    test "if primary key is set, it won't be overridden" do
      id = Ecto.UUID.generate()
      {:ok, person} = TestSyncRepo.insert(%Person{id: id})
      assert person.id == id
    end
  end

  describe "EctoSync.Repo" do
    test "own updates are not received", %{person: person} do
      preloads = [posts: [:person]]
      person = do_preload(person, preloads, TestSyncRepo)

      subscribe(person, assocs: preloads)

      {:ok, post} = TestSyncRepo.insert(Ecto.Changeset.change(%Post{}, %{person_id: person.id}))

      receive do
        {:ecto_sync, {Post, :inserted, _} = sync_args} ->
          assert person == EctoSync.sync(person, sync_args)

          assert do_preload(person, preloads, TestSyncRepo) ==
                   EctoSync.sync(person, sync_args, force: true)
      end

      {:ok, _updated} =
        Ecto.Changeset.change(post, %{name: "updated"})
        |> TestSyncRepo.update()

      receive do
        {:ecto_sync, {Post, :updated, _} = sync_args} ->
          assert person == EctoSync.sync(person, sync_args)

          assert do_preload(person, preloads) ==
                   EctoSync.sync(person, sync_args, preloads: %{Post => [:person]}, force: true)
      after
        500 ->
          raise "no updates"
      end
    end

    test "inserts from other process is received", %{person: person} do
      preloads = [posts: [:person]]
      person = do_preload(person, preloads, TestSyncRepo)

      subscribe(person, assocs: preloads)

      Task.async(fn ->
        {:ok, post} = TestSyncRepo.insert(Ecto.Changeset.change(%Post{}, %{person_id: person.id}))
      end)
      |> Task.await()

      receive do
        {:ecto_sync, {Post, :inserted, _} = sync_args} ->
          assert do_preload(person, preloads, TestSyncRepo) ==
                   EctoSync.sync(person, sync_args, force: true)
      end
    end

    test "updates from other process is received", %{person: person} do
      preloads = [posts: [:person]]
      person = do_preload(person, preloads, TestSyncRepo)

      subscribe(person, assocs: preloads)

      Task.async(fn ->
        {:ok, _} = TestSyncRepo.update(Ecto.Changeset.change(person, %{name: "updated"}))
      end)
      |> Task.await()

      receive do
        {:ecto_sync, {Person, :updated, _} = sync_args} ->
          assert TestSyncRepo.get(Person, person.id) |> do_preload(preloads, TestSyncRepo) ==
                   EctoSync.sync(person, sync_args)
      end
    end

    test "assoc updates are not received from own process", %{
      person_with_posts: %{posts: [post | _] = _posts} = person
    } do
      preloads = [posts: [:person]]

      subscribe(person, assocs: preloads)

      IO.puts("pre-update")

      {:ok, _updated} =
        Ecto.Changeset.change(post, %{name: "updated"})
        |> TestSyncRepo.update()

      IO.puts("updated")

      receive do
        {:ecto_sync, {Post, :updated, _} = sync_args} ->
          assert person == EctoSync.sync(person, sync_args)
      after
        500 ->
          raise "no updates"
      end
    end
  end

  defp do_setup(_) do
    start_supervised!(TestSyncRepo)

    start_supervised!({
      EctoSync,
      repo: TestSyncRepo,
      adapter: EctoSync.Adapters.Postgres.Wal,
      watchers:
        [{Label, :inserted, label: :label}]
        |> EctoSync.watchers(Post,
          assocs: [:tags, :labels, person: [:favourite_tags]],
          extra_columns: [:person_id]
        )
    })

    {:ok, person} = TestSyncRepo.insert(Ecto.Changeset.cast(%Person{}, %{name: "setup"}, [:name]))

    {:ok, person_with_posts} = TestSyncRepo.insert(%Person{posts: [%Post{}, %Post{}]})

    [
      person: person,
      preloads: [:person],
      person_with_posts: person_with_posts
      # person_with_posts_and_tags: person_with_post_and_tags
    ]
  end

  defp do_preload(value, preloads, repo \\ TestSyncRepo)

  defp do_preload({:ok, value}, preloads, repo) do
    {:ok, do_preload(value, preloads, repo)}
  end

  defp do_preload(value, preloads, repo) do
    assocs = value.__struct__.__schema__(:associations)
    fields = assocs ++ preloads

    Ecto.reset_fields(value, fields)
    |> repo.preload(preloads)
  end
end
