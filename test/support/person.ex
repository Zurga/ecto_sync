defmodule Person do
  @moduledoc false
  use Ecto.Schema

  schema "persons" do
    field(:name, :string)
    has_many(:posts, Post)
    many_to_many(:favourite_tags, Tag, join_through: FavouriteTags, on_replace: :delete)
  end
end
