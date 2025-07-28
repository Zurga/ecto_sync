defmodule Person do
  @moduledoc false
  use Ecto.Schema

  schema "persons" do
    field(:name, :string)
    has_many(:posts, Post)

    many_to_many(:favourite_people, __MODULE__,
      join_through: FavouritePeople,
      on_replace: :delete
    )

    many_to_many(:favourite_posts, Post, join_through: FavouritePosts, on_replace: :delete)
    many_to_many(:favourite_tags, Tag, join_through: FavouriteTags, on_replace: :delete)
  end
end
