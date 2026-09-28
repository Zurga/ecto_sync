defmodule TestRepo.Migrations.FavouritePeople do
  use Ecto.Migration

  def change do
    create table(:favourite_people, primary_key: false) do
      add :id, :binary_id, primary_key: true
      add :parent_id, references(:persons, on_delete: :delete_all, type: :binary_id), null: false
      add :child_id, references(:persons, on_delete: :delete_all, type: :binary_id), null: false
    end

    create unique_index(:favourite_people, [:parent_id, :child_id])

  end
end
