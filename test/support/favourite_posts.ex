defmodule FavouritePosts do
  @moduledoc false
  use Ecto.Schema

  schema "favourite_posts" do
    belongs_to(:person, Person)
    belongs_to(:post, Post)
  end
end
