defmodule Post do
  @moduledoc false
  use Ecto.Schema

  schema "posts" do
    field(:name, :string)
    field(:body, :string)
    belongs_to(:person, Person, on_replace: :update)
    many_to_many(:tags, Tag, join_through: PostsTags, preload_order: [asc: :id])
    many_to_many(:labels, Label, join_through: "posts_labels", preload_order: [asc: :id])
  end
end
