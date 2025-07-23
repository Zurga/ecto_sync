defmodule FavouriteTags do
  @moduledoc false
  use Ecto.Schema

  schema "favourite_tags" do
    belongs_to(:person, Person)
    belongs_to(:tag, Tag)
  end
end
