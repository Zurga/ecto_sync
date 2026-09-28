defmodule PostsTags do
  @moduledoc false
  use Schema

  schema "posts_tags" do
    belongs_to(:post, Post)
    belongs_to(:tag, Tag)
  end
end
