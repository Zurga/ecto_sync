defmodule PostsLabels do
  @moduledoc false

  use Schema

  schema "posts_labels" do
    belongs_to(:post, Post)
    belongs_to(:label, Label)
  end
end
