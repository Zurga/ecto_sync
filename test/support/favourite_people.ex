defmodule FavouritePeople do
  @moduledoc false
  use Ecto.Schema

  schema "favourite_people" do
    belongs_to(:parent, Person)
    belongs_to(:child, Person)
  end
end
