defmodule EctoSync.SyncerTest do
  use EctoSync.RepoCase, async: false
  alias EctoSync.{Syncer, SyncParams}

  defmodule MockRepo do
    def get(schema, id) do
      struct(schema, %{id: id})
    end

    def preload(%Post{id: 1} = post, [person: []], _), do: %Post{id: 1, person: %Person{id: 1}}
    def preload(%Post{id: 2} = post, [person: []], _), do: %Post{id: 2, person: %Person{id: 2}}

    def preload(any, _preloads, _) do
      any
    end
  end

  setup :do_setup

  describe "inserted" do
    test "inserted with an empty list as value" do
      sync_params = SyncParams.new({Post, :inserted, {%{id: 1}, 1}}, [])
      assert [%Post{id: 1}] == Syncer.sync([], sync_params)
    end

    test "inserted with other schema inserts" do
      sync_params = SyncParams.new({Person, :inserted, {%{id: 1}, 1}}, [])
      assert [%Post{id: 1}, %Person{id: 1}] == Syncer.sync([%Post{id: 1}], sync_params)
    end

    test "inserted with other schema inserts and strict does not insert" do
      sync_params = SyncParams.new({Person, :inserted, {%{id: 2}, 1}}, strict: true)
      assert [%Post{id: 1}] == Syncer.sync([%Post{id: 1}], sync_params)
    end

    test "inserted does also call preloads" do
      sync_params =
        SyncParams.new({Post, :inserted, {%{id: 2}, 1}}, preloads: %{Post => [:person]})

      assert [%Post{id: 1, person: %Person{id: 1}}, %Post{id: 2, person: %Person{id: 2}}] ==
               Syncer.sync([%Post{id: 1, person: %Person{id: 1}}], sync_params)
    end
  end

  describe "updated" do
    test "updated with an empty list as value" do
      sync_params = SyncParams.new({Post, :updated, {%{id: 1}, 1}}, [])
      assert [%Post{id: 1}] == Syncer.sync([], sync_params)
    end
  end

  defp do_setup(_) do
    options =
      EctoSync.Options.new(
        adapter: nil,
        repo: MockRepo,
        watchers:
          [{Label, :inserted, label: :label}]
          |> EctoSync.watchers(Post,
            assocs: [:tags, :labels, person: [:favourite_tags]],
            extra_columns: [:person_id]
          )
      )

    start_supervised!({Registry, keys: :duplicate, name: EventRegistry})
    start_supervised!({Cachex, :ecto_sync})

    :persistent_term.put(EctoSync, options)
  end
end
