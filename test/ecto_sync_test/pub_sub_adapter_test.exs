defmodule EctoSync.PubSubAdapterTest do
  use ExUnit.Case, async: false

  describe "broadcast" do
    setup do
      start_supervised({Phoenix.PubSub, adapter: EctoSync.PubSub, name: :pub_sub})
      :ok
    end

    test "ref is the same for each subscriber for a message" do
      Phoenix.PubSub.subscribe(:pub_sub, "test")
      Phoenix.PubSub.subscribe(:pub_sub, "test")
      Phoenix.PubSub.subscribe(:pub_sub, "test")

      Phoenix.PubSub.broadcast(:pub_sub, "test", {{Test, :updated}, %{id: :world}})

      refs =
        for _ <- 1..3 do
          receive do
            %{ref: ref} -> ref
          after
            1000 ->
              :nothing
          end
        end

      refute Enum.uniq(refs) == [:nothing]
      assert Enum.uniq(refs) |> Enum.count() == 1
    end
  end
end
