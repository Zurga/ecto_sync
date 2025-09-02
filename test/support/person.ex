defmodule Person do
  @moduledoc false
  use Ecto.Schema

  schema "persons" do
    field(:name, :string)
    has_many(:posts, Post)
    has_many(:other_posts, Post, foreign_key: :other)
    has_many(:test_posts, Post, where: [name: "test"])
    has_many(:test_posts_again, Post, where: [name: "test"], preload_order: [asc: :id])
    has_many(:bad_posts, Post, where: [name: {:in, ["bad", "worst"]}, body: "blabla"])

    has_many(:all_tags, through: [:posts, :tags])

    # has_many(:fragmented_posts, Post,
    #   where: [name: {:fragment, fragment("lower(?)", p.name) == "fragmented"}]
    # )

    many_to_many(:favourite_people, __MODULE__,
      join_through: FavouritePeople,
      on_replace: :delete
    )

    many_to_many(:favourite_posts, Post, join_through: FavouritePosts, on_replace: :delete)
    many_to_many(:favourite_tags, Tag, join_through: FavouriteTags, on_replace: :delete)
  end
end
