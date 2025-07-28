defmodule TestRepo.Migrations.FavouritePeople do
  use Ecto.Migration

  def change do
    create table(:favourite_people) do
      add :parent_id, references(:persons, on_delete: :delete_all)
      add :child_id, references(:persons, on_delete: :delete_all)
    end

    create unique_index(:favourite_people, [:parent_id, :child_id])

  end
end
