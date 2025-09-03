defmodule EctoSync.HelpersTest do
  import EctoSync.Helpers
  use ExUnit.Case

  describe "kw_deep_merge/2" do
    test "keywords with atoms" do
      assert [posts: [tags: [], labels: []]] ==
               kw_deep_merge([posts: [tags: [], labels: []]], posts: [:tags, :labels])

      assert [posts: [tags: [], labels: []]] ==
               kw_deep_merge([posts: [:tags, :labels]], posts: [tags: [], labels: []])
    end
  end
end
