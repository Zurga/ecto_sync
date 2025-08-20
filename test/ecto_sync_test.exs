defmodule EctoSyncTest do
  use EctoSync.RepoCase, async: false
  import EctoSync
  import EctoSync.Helpers

  @association_columns [:post_id, :label_id]
  @posts_labels_events [
    {%{
       table_name: "posts_labels",
       primary_key: :id,
       columns: @association_columns,
       association_columns: @association_columns
     }, :deleted, extra_columns: @association_columns, label: :posts_labels_deleted},
    {%{
       table_name: "posts_labels",
       primary_key: :id,
       columns: @association_columns,
       association_columns: @association_columns
     }, :inserted, extra_columns: @association_columns, label: :posts_labels_inserted},
    {%{
       table_name: "posts_labels",
       primary_key: :id,
       columns: @association_columns,
       association_columns: @association_columns
     }, :updated, extra_columns: @association_columns, label: :posts_labels_updated}
  ]

  setup [:do_setup]

  describe "watchers/3" do
    test "all events are generated" do
      assert watchers_with_labels([
               {Post, :inserted, [extra_columns: []]},
               {Post, :updated, [extra_columns: []]},
               {Post, :deleted, [extra_columns: []]}
             ]) == EctoSync.watchers(Post)
    end

    test "adding a label to schema" do
      assert [
               {Post, :inserted, [label: :my_label_inserted, extra_columns: []]},
               {Post, :updated, [label: :my_label_updated, extra_columns: []]},
               {Post, :deleted, [label: :my_label_deleted, extra_columns: []]}
             ] == EctoSync.watchers(Post, label: :my_label)
    end

    test ":assocs option with keyword assocs" do
      assert watchers_with_labels([
               {Label, :deleted, [extra_columns: []]},
               {Label, :inserted, [extra_columns: []]},
               {Label, :updated, [extra_columns: []]},
               {Person, :deleted, [extra_columns: []]},
               {Person, :inserted, [extra_columns: []]},
               {Person, :updated, [extra_columns: []]},
               {Post, :deleted, [extra_columns: [:person_id]]},
               {Post, :inserted, [extra_columns: [:person_id]]},
               {Post, :updated, [extra_columns: [:person_id]]},
               {PostsTags, :deleted, [extra_columns: [:tag_id, :post_id]]},
               {PostsTags, :inserted, [extra_columns: [:tag_id, :post_id]]},
               {PostsTags, :updated, [extra_columns: [:tag_id, :post_id]]},
               {Tag, :deleted, [extra_columns: []]},
               {Tag, :inserted, [extra_columns: []]},
               {Tag, :updated, [extra_columns: []]}
               | @posts_labels_events
             ]) ==
               EctoSync.watchers(Person, assocs: [posts: [:comments, :tags, :labels]])
               |> Enum.sort()
    end

    test ":assocs option merges with other columns" do
      assert watchers_with_labels([
               {Post, :inserted, [extra_columns: [:id, :person_id]]},
               {Post, :updated, [extra_columns: [:id, :person_id]]},
               {Post, :deleted, [extra_columns: [:id, :person_id]]},
               {Person, :inserted, [extra_columns: []]},
               {Person, :updated, [extra_columns: []]},
               {Person, :deleted, [extra_columns: []]}
             ]) == EctoSync.watchers(Post, assocs: [:person], extra_columns: [:id])
    end

    test "raises with invalid inputs" do
      assert_raise(ArgumentError, fn -> EctoSync.watchers(Unexisting) end)
    end
  end

  describe "subscribe/3" do
    test "subscribe to Ecto.Schema struct", %{person_with_posts: %{posts: [post, post2]} = person} do
      assert [
               {{Person, :deleted}, person.id},
               {{Person, :updated}, person.id},
               {{Post, :deleted}, post.id},
               {{Post, :deleted}, post2.id},
               {{Post, :inserted}, {:person_id, person.id}},
               {{Post, :updated}, post.id},
               {{Post, :updated}, post2.id}
             ] ==
               subscribe(person, assocs: [:posts])
    end

    test "subscribe to Ecto.Schema struct with inserted opt", %{
      person_with_posts: %{posts: [post, post2]} = person
    } do
      assert [
               {{Person, :deleted}, person.id},
               {{Person, :inserted}, nil},
               {{Person, :updated}, person.id},
               {{Post, :deleted}, post.id},
               {{Post, :deleted}, post2.id},
               {{Post, :inserted}, {:person_id, person.id}},
               {{Post, :updated}, post.id},
               {{Post, :updated}, post2.id}
             ] ==
               subscribe(person, assocs: [:posts], inserted: true)
    end

    test "subscribe to a list of Ecto.Schema structs", %{
      person: person,
      person_with_posts: %{posts: [post, post2]} = person2
    } do
      assert [
               {{Person, :deleted}, person.id},
               {{Person, :updated}, person.id},
               {{Person, :deleted}, person2.id},
               {{Person, :updated}, person2.id},
               {{Post, :deleted}, post.id},
               {{Post, :updated}, post.id},
               {{Post, :deleted}, post2.id},
               {{Post, :updated}, post2.id},
               {{Post, :inserted}, {:person_id, person.id}},
               {{Post, :inserted}, {:person_id, person2.id}}
             ] ==
               subscribe([person, person2], assocs: [:posts])
               |> Enum.sort_by(&elem(&1, 1))
    end

    test "subscribe to assocs that are not preloaded", %{
      person_with_posts: %{posts: [post, post2]} = person
    } do
      assert [
               {{Person, :deleted}, person.id},
               {{Person, :updated}, person.id},
               {{Post, :deleted}, post.id},
               {{Post, :updated}, post.id},
               {{Post, :deleted}, post2.id},
               {{Post, :updated}, post2.id},
               {{Post, :inserted}, {:person_id, person.id}},
               {{PostsTags, :inserted}, {:post_id, post.id}},
               {{PostsTags, :inserted}, {:post_id, post2.id}}
             ] ==
               subscribe([person], assocs: [posts: :tags])
               |> Enum.sort_by(&elem(&1, 1))
    end

    test "no double subscribes", %{person: person} do
      for _ <- 1..3 do
        subscribe(person)
      end

      assert [{self(), []}] == subscriptions({Person, :updated}, person.id)
    end

    test "subscribe to label" do
      assert [{:label, []}] == subscribe(:label)
    end
  end

  describe "feature: automatic subscription on insert" do
  end

  describe "integrations" do
    test "subscribing with EctoWatch also works", %{person: person} do
      EctoWatch.subscribe(encode_watcher_identifier({Post, :inserted}), nil)

      {:ok, post} = TestRepo.insert(%Post{person_id: person.id})

      receive do
        {{Post, :inserted}, _} = sync_args ->
          synced = EctoSync.sync(post, sync_args)
          assert synced == post
          assert [^post] = EctoSync.sync([], sync_args)
      after
        500 ->
          raise "no inserts"
      end
    end

    test "types of sync arguments for insert", %{person: person} do
      assert [{{Post, :inserted}, nil}] == subscribe(Post, :inserted)
      person = do_preload(person, [:posts])

      {:ok, post} =
        TestRepo.insert(%Post{person_id: person.id})
        |> do_preload(person: [:posts])

      sync_opts = [preloads: %{Post => [person: [:posts]]}]

      receive do
        {{Post, :inserted}, _} = sync_args ->
          synced = EctoSync.sync(post, sync_args, sync_opts)

          assert do_preload(person, posts: [person: :posts]) ==
                   EctoSync.sync(person, sync_args, sync_opts)

          assert synced == post
          assert [^post] = EctoSync.sync([], sync_args, sync_opts)
          assert [%Post{}, ^post] = EctoSync.sync([%Post{}], sync_args, sync_opts)
          assert ^post = EctoSync.sync(nil, sync_args, sync_opts)
          assert "" == EctoSync.sync("", sync_args)
          assert 9 == EctoSync.sync(9, sync_args)
      after
        500 ->
          raise "no inserts"
      end

      # Check that we have not subscribed multiple times
      {:ok, _updated} =
        Ecto.Changeset.change(post, %{name: "updated"})
        |> TestRepo.update()

      assert [_] = flush()
    end

    test "only one message is sent after insert", %{person: person} do
      assert [{{Post, :inserted}, nil}] == subscribe(Post, :inserted)

      {:ok, _post} = TestRepo.insert(%Post{person_id: person.id})

      assert [_] = flush()
    end

    test "subscribe/2 full flow", %{person: person} do
      person = do_preload(person, [:posts])
      subscribe(person, assocs: [:posts])
      {:ok, %{id: post_id} = post} = TestRepo.insert(%Post{person_id: person.id})

      # assert [{{Post, :updated}, post_id} ] == subscribe({Post, :updated}, post_id)

      receive do
        {{Post, :inserted}, _} = sync_args ->
          assert do_preload(person, [:posts]) == EctoSync.sync(person, sync_args)
      end

      {:ok, updated} =
        Ecto.Changeset.change(post, %{name: "updated"})
        |> TestRepo.update()

      receive do
        {{Post, :updated}, _} = sync_args ->
          assert do_preload(person, [:posts]) == EctoSync.sync(person, sync_args)
      after
        500 ->
          raise "no updates"
      end
    end

    test "subscribe/2 to deletes", %{person: person} do
      {:ok, post} =
        TestRepo.insert(%Post{person_id: person.id})

      assert [
               {{Post, :deleted}, post.id},
               {{Post, :updated}, post.id}
             ] ==
               subscribe(post)

      {:ok, _updated} =
        Ecto.Changeset.change(post, %{name: "updated"})
        |> TestRepo.update()

      person = TestRepo.preload(person, [:posts], force: true)

      {:ok, _updated} = TestRepo.delete(post)
      expected = TestRepo.preload(person, [:posts], force: true)

      receive do
        {{Post, :deleted}, _} = sync_args ->
          synced = EctoSync.sync(person, sync_args)
          assert synced == expected
      after
        500 ->
          raise "no deletes"
      end
    end

    test "sync fun with list returns updated list", %{
      person_with_posts: %{posts: [post1 | _]} = person
    } do
      preloads = [:posts]
      subscribe(person, assocs: [:posts])

      {:ok, _} =
        Ecto.Changeset.change(post1, %{name: "updated"})
        |> TestRepo.update()

      expected = do_preload(person, preloads)

      sort = fn enum -> Enum.sort_by(enum, & &1.id) end

      receive do
        {{Post, :updated}, _} = sync_args ->
          synced = EctoSync.sync([person, person], sync_args)
          assert is_list(synced)

          for s <- synced do
            assert sort.(s.posts) == sort.(expected.posts)
          end
      after
        500 -> raise "no updates"
      end
    end

    test "multiple updates result in distinct values" do
      preloads = [posts: [person: [:posts]]]

      {:ok, %{posts: [post1]} = person1} =
        TestRepo.insert(%Person{posts: [%Post{}]})
        |> do_preload(preloads)

      subscribe(person1, assocs: [:posts])

      {:ok, _} =
        Ecto.Changeset.change(post1, %{name: "post1 update1"})
        |> TestRepo.update()

      person1_expected_after_update =
        TestRepo.get(Person, person1.id) |> do_preload(preloads)

      receive do
        {{Post, :updated}, _} = sync_args ->
          synced = EctoSync.sync(person1, sync_args)
          assert person1_expected_after_update == synced
      after
        500 -> raise "no updates for update1"
      end

      {:ok, _} =
        Ecto.Changeset.change(post1, %{name: "post1 update2"})
        |> TestRepo.update()

      person1_expected_after_update_2 =
        TestRepo.get(Person, person1.id) |> do_preload(preloads)

      receive do
        {{Post, :updated}, _} = sync_args ->
          synced = EctoSync.sync(person1_expected_after_update, sync_args)
          assert person1_expected_after_update_2 == synced
      after
        500 -> raise "no updates for update2"
      end
    end
  end

  describe "belongs_to" do
    @preloads [:person]
    test "insert" do
      {:ok, post} =
        %Post{}
        |> TestRepo.insert()
        |> do_preload(@preloads)

      subscribe(post, assocs: @preloads)

      {:ok, _person} = TestRepo.insert(%Person{})
      {:ok, _person} = TestRepo.insert(%Person{posts: [post]})
      expected = TestRepo.get(Post, post.id) |> do_preload(@preloads)

      receive do
        {{Post, :updated}, _} = sync_args ->
          synced = EctoSync.sync(post, sync_args)
          assert expected == synced
      after
        500 -> raise "no update"
      end
    end

    test "delete", %{
      person_with_posts_and_tags: %{posts: [post1 | _]} = person,
      person: other_person
    } do
      post1 = do_preload(post1, @preloads)

      subscribe(post1, assocs: @preloads)

      for p <- [person, other_person] do
        TestRepo.delete(p)
      end

      receive do
        {{Person, :deleted}, _} = sync_args ->
          synced = EctoSync.sync(post1, sync_args)
          assert do_preload(post1, @preloads) == synced
      after
        500 -> raise "no post update"
      end

      refute_received({{Person, :updated}, _})
    end

    test "update", %{person_with_posts_and_tags: %{posts: [post1 | _]} = person} do
      post1 = do_preload(post1, @preloads)

      subscribe(post1, assocs: @preloads)

      {:ok, _} =
        Ecto.Changeset.change(person, %{name: "updated"})
        |> TestRepo.update()

      receive do
        {{Person, :updated}, _} = sync_args ->
          synced = EctoSync.sync(post1, sync_args)
          assert do_preload(post1, @preloads) == synced
      after
        500 -> raise "no person update"
      end

      refute_received({{Person, :updated}, _})
    end

    test "update assoc is changed", %{
      person_with_posts_and_tags: %{posts: [post1, post2 | _]},
      person: other_person
    } do
      post1 = do_preload(post1, @preloads)

      subscribe(post1)

      {:ok, _} =
        Ecto.Changeset.change(post2, %{person_id: other_person.id})
        |> TestRepo.update()

      {:ok, preloaded} =
        Ecto.Changeset.change(post1, %{person_id: other_person.id})
        |> TestRepo.update()
        |> do_preload(@preloads)

      receive do
        {{Post, :updated}, _} = sync_args ->
          synced = EctoSync.sync(post1, sync_args)
          assert preloaded == synced
      after
        500 -> raise "no post update"
      end

      refute_received({{Person, :updated}, _})
    end

    test "preloads", %{person: person} do
      assert [{{Post, :inserted}, nil}] == subscribe(Post, :inserted)

      {:ok, post} = TestRepo.insert(%Post{person_id: person.id})

      receive do
        {{Post, :inserted}, _} = sync_args ->
          synced = EctoSync.sync(post, sync_args, preloads: %{Post => :person})
          assert synced == post |> do_preload([:person])
          assert [^post] = EctoSync.sync([], sync_args)
          assert ^post = EctoSync.sync(nil, sync_args)
      after
        500 ->
          raise "no inserts"
      end
    end
  end

  @preloads [posts: [:tags, :labels]]
  describe "has_many" do
    test "inserted", %{person_with_posts_and_tags: person} do
      person = do_preload(person, @preloads)

      subscribe(person, assocs: [posts: [:tags, :labels]])

      {:ok, _post} = TestRepo.insert(%Post{person_id: person.id})

      receive do
        {{Post, :inserted}, _} = sync_args ->
          synced = EctoSync.sync(person, sync_args)
          assert do_preload(person, @preloads) == synced
          synced
      after
        500 -> raise "nothing POSTS"
      end
    end

    test "updated", %{person_with_posts_and_tags: person} do
      %{posts: [post1 | _]} = person = do_preload(person, @preloads)

      subscribe(person, assocs: [:posts])

      {:ok, _} =
        Ecto.Changeset.change(post1, %{name: "updated"})
        |> TestRepo.update()

      receive do
        {{Post, :updated}, _} = sync_args ->
          %{posts: synced_posts} = EctoSync.sync(person, sync_args)
          %{posts: preloaded_posts} = do_preload(person, @preloads)
          assert preloaded_posts |> Enum.sort() == synced_posts |> Enum.sort()
      after
        500 -> raise "no post update"
      end
    end

    test "deleted", %{person_with_posts_and_tags: person} do
      %{posts: [post1 | _]} = person = do_preload(person, @preloads)

      subscribe(person, assocs: [:posts])

      {:ok, _} = TestRepo.delete(post1)

      receive do
        {{Post, :deleted}, _} = sync_args ->
          %{posts: synced_posts} = EctoSync.sync(person, sync_args)
          %{posts: preloaded_posts} = do_preload(person, @preloads)
          assert preloaded_posts |> Enum.sort() == synced_posts |> Enum.sort()
      after
        500 -> raise "no post update"
      end
    end

    test "assoc has moved to other row", %{
      person: person2,
      person_with_posts: person1
    } do
      preloads = [:posts]

      %{posts: [post1 | _]} = person1 = do_preload(person1, preloads)
      person2 = do_preload(person2, preloads)

      subscribe([person1, person2], assocs: [:posts])

      {:ok, _} =
        Ecto.Changeset.change(post1, %{person_id: person2.id})
        |> TestRepo.update()

      person1_expected_after_update = TestRepo.get(Person, person1.id) |> do_preload(preloads)

      person2_expected_after_update = TestRepo.get(Person, person2.id) |> do_preload(preloads)

      other_process =
        Task.async(fn ->
          subscribe(person2, assocs: [:posts])

          receive do
            {{Post, :inserted}, _} = sync_args ->
              synced = EctoSync.sync(person2, sync_args)

              assert person2_expected_after_update == synced
          after
            5000 -> raise "no updates in other process"
          end
        end)

      receive do
        {{Post, :updated}, _} = sync_args ->
          synced = EctoSync.sync(person1, sync_args)
          assert person1_expected_after_update == synced

          synced = EctoSync.sync(person2, sync_args)
          assert person2_expected_after_update == synced
      after
        500 -> raise "no updates for person1"
      end

      assert Task.await(other_process)
    end

    test "preloads", %{person_with_posts_and_tags: person} do
      person = do_preload(person, @preloads)

      subscribe(person, assocs: [posts: [:tags, :labels]])

      {:ok, _post} = TestRepo.insert(%Post{person_id: person.id})

      receive do
        {{Post, :inserted}, _} = sync_args ->
          synced = EctoSync.sync(person, sync_args, preloads: %{Post => [:tags, :labels]})
          assert do_preload(person, @preloads) == synced
          synced = EctoSync.sync(person, sync_args, preloads: %{Post => [:tags, :labels]})
          assert do_preload(person, @preloads) == synced
          synced
      after
        500 -> raise "nothing POSTS"
      end
    end
  end

  describe "has with where clause" do
    @preloads [
      :bad_posts,
      posts: [:tags, :labels],
      test_posts: [:tags, :labels],
      test_posts_again: [:tags, :labels]
    ]
    test "inserted", %{person_with_posts: person} do
      person = do_preload(person, @preloads)

      subscribe(person, assocs: @preloads)

      {:ok, _post} = TestRepo.insert(%Post{person_id: person.id, name: "test"})

      receive do
        {{Post, :inserted}, _} = sync_args ->
          synced = EctoSync.sync(person, sync_args, preloads: %{Post => [:tags, :labels]})
          assert do_preload(person, @preloads) == synced
      after
        500 -> raise "nothing POSTS"
      end
    end

    test "updated", %{person_with_posts: person} do
      %{posts: [post | _]} =
        person =
        do_preload(person, @preloads)
        |> IO.inspect()

      subscribe(person, assocs: @preloads)
      |> IO.inspect(label: :subscriptions)

      {:ok, _} =
        Ecto.Changeset.change(post, %{name: "test"})
        |> TestRepo.update()

      # flush()
      # |> IO.inspect()

      receive do
        {{Post, :updated}, _} = sync_args ->
          synced = EctoSync.sync(person, sync_args, preloads: %{Post => [:tags, :labels]})
          assert do_preload(person, @preloads) == synced
      after
        500 -> raise "nothing POSTS"
      end
    end
  end

  describe "many to many with join_through module" do
    @preloads [:favourite_tags, posts: :tags]
    test "inserted", %{person_with_posts_and_tags: person} do
      %{posts: [post1, post2]} = person = do_preload(person, @preloads)

      subscribe(person, assocs: @preloads)

      {:ok, _tag} =
        TestRepo.insert(%Tag{name: "inserted", posts: [post1]})
        |> do_preload([:posts])

      person =
        receive do
          {{PostsTags, :inserted}, _} = sync_args ->
            synced = EctoSync.sync(person, sync_args)
            assert do_preload(person, @preloads) == synced
            synced
        after
          500 -> raise "nothing POSTS"
        end

      {:ok, _tag} =
        %Tag{}
        |> Ecto.Changeset.change(%{name: "test"})
        |> Ecto.Changeset.put_assoc(:posts, [post2])
        |> TestRepo.insert()
        |> do_preload([:posts])

      receive do
        {{PostsTags, :inserted}, _} = sync_args ->
          synced =
            EctoSync.sync(person, sync_args)

          assert do_preload(person, @preloads) == synced
      after
        500 -> raise "nothing POSTS"
      end
    end

    test "join_through is updated", %{person_with_posts_and_tags: person} do
      %{posts: [%{tags: [from_tag | _]} | _]} = person = do_preload(person, @preloads)

      subscribe(person, assocs: [posts: :tags])

      from_tag
      |> do_preload([:posts])
      |> Ecto.Changeset.change()
      |> Ecto.Changeset.put_assoc(:posts, [])
      |> TestRepo.update()

      receive do
        {{PostsTags, _}, _} = sync_args ->
          synced = EctoSync.sync(person, sync_args)

          assert do_preload(person, @preloads) == synced
      after
        500 -> raise "nothing POSTS"
      end
    end

    test "updated", %{person_with_posts_and_tags: person} do
      %{posts: [%{tags: [tag | _]}, _post2]} = person = do_preload(person, @preloads)

      subscribe(person, assocs: [posts: :tags])

      {:ok, other_tag} = TestRepo.insert(%Tag{})

      {:ok, _tag} =
        Ecto.Changeset.change(other_tag, %{name: "other_updated"})
        |> TestRepo.update()

      {:ok, _tag} =
        Ecto.Changeset.change(tag, %{name: "updated"})
        |> TestRepo.update()
        |> do_preload([:posts])

      receive do
        {{Tag, :updated}, _} = sync_args ->
          synced = EctoSync.sync(person, sync_args)
          assert do_preload(person, @preloads) == synced
      after
        500 -> raise "no tag update"
      end
    end

    test "updated subscribe_assocs", %{person_with_posts_and_tags: person} do
      %{posts: [%{tags: [tag | _]}, _post2]} = person = do_preload(person, @preloads)

      subscribe(person, assocs: [posts: :tags])

      {:ok, other_tag} = TestRepo.insert(%Tag{})

      {:ok, %{id: tag_id}} =
        Ecto.Changeset.change(tag, %{name: "updated"})
        |> TestRepo.update()

      {:ok, _tag} =
        Ecto.Changeset.change(other_tag, %{name: "other_updated"})
        |> TestRepo.update()

      flush()
      |> Enum.each(fn
        {{Tag, :updated}, {^tag_id, _}} = sync_args ->
          synced = EctoSync.sync(person, sync_args)
          assert do_preload(person, @preloads) == synced

        {{Tag, :updated}, _} = sync_args ->
          synced = EctoSync.sync(person, sync_args)
          assert do_preload(person, @preloads) == synced

        {{Tag, :inserted}, _} ->
          false

        message ->
          raise "#{inspect(message)}"
      end)
    end

    test "deleted", %{person_with_posts_and_tags: person} do
      %{posts: [%{tags: [tag | _]}, _post2]} = person = do_preload(person, @preloads)

      subscribe(person, assocs: [posts: :tags])
      TestRepo.delete(tag)

      receive do
        {{Tag, :deleted}, _} = sync_args ->
          synced = EctoSync.sync(person, sync_args)
          assert do_preload(person, @preloads) == synced
      after
        500 -> raise "no tag delete"
      end
    end
  end

  describe "many to many with join through table" do
    @preloads [posts: [:labels]]
    test "inserted", %{person_with_posts_and_tags: person} do
      %{posts: [_post1, post2]} = person = do_preload(person, @preloads)

      {:ok, label} = TestRepo.insert(%Label{name: "new label"})

      subscribe(person, assocs: @preloads)

      {:ok, _} =
        Ecto.Changeset.change(post2, %{labels: [label | post2.labels]})
        |> TestRepo.update()

      receive do
        sync_args ->
          synced = EctoSync.sync(person, sync_args)
          assert do_preload(person, @preloads) == synced
      after
        500 -> raise "nothing POSTS"
      end
    end
  end

  describe "subscriptions/0" do
    test "subscriptions can be listed", %{person: person} do
      subscribe(person)
      assert [{self(), []}] == subscriptions({Person, :updated}, person.id)
    end

    test "subscriptions are up to date after unsubscribing", %{person: person} do
      subscribe(person)
      assert [{self(), []}] == subscriptions({Person, :updated}, person.id)
      unsubscribe(person)
      assert [] == subscriptions({Person, :updated}, person.id)
    end
  end

  describe "unsubscribe/1" do
    test "unsubscribe", %{
      person: person
    } do
      subscribe(person, assocs: [:posts])

      {:ok, _post} =
        TestRepo.insert(%Post{person_id: person.id})

      assert length(flush()) == 1
      unsubscribe(person, assocs: [:posts])

      {:ok, _post} =
        TestRepo.insert(%Post{person_id: person.id})

      assert flush() == []
    end
  end

  test "graph can be created" do
  end

  defp do_setup(_) do
    start_supervised!(TestRepo)
    {:ok, person} = TestRepo.insert(%Person{})

    {:ok, person_with_post_and_tags} =
      TestRepo.insert(%Person{
        posts: [
          %Post{tags: [%Tag{name: "tag"}, %Tag{name: "other_tag"}]},
          %Post{labels: [%Label{name: "label"}]}
        ]
      })

    {:ok, person_with_posts} = TestRepo.insert(%Person{posts: [%Post{}, %Post{}]})

    start_supervised!({
      EctoSync,
      repo: TestRepo,
      watchers:
        [{Label, :inserted, label: :label}]
        |> EctoSync.watchers(Post,
          assocs: [:tags, :labels, person: [:favourite_tags]],
          extra_columns: [:person_id]
        )
    })

    [
      person: person,
      preloads: [:person],
      person_with_posts: person_with_posts,
      person_with_posts_and_tags: person_with_post_and_tags
    ]
  end

  defp do_preload({:ok, value}, preloads) do
    {:ok, do_preload(value, preloads)}
  end

  defp do_preload(value, preloads) do
    assocs = value.__struct__.__schema__(:associations)
    fields = assocs ++ preloads

    Ecto.reset_fields(value, fields)
    |> TestRepo.preload(preloads, force: true)
  end

  defp flush(messages \\ []) do
    receive do
      message -> flush([message | messages])
    after
      500 ->
        messages
        |> Enum.reverse()
    end
  end

  defp watchers_with_labels(watchers) do
    watchers
    |> Enum.map(fn {schema, event, opts} = watcher ->
      if ecto_schema_mod?(schema) do
        label = encode_watcher_identifier({schema, event})
        {schema, event, Keyword.put(opts, :label, label)}
      else
        watcher
      end
    end)
  end
end
