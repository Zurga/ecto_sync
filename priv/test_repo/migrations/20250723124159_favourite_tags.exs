defmodule TestRepo.Migrations.FavouriteTags do
  use Ecto.Migration

  def change do
    create table(:favourite_tags, primary_key: false) do
      add :id, :binary_id, primary_key: true
      add :tag_id, references(:tags, on_delete: :delete_all, type: :binary_id), null: false
      add :person_id, references(:persons, on_delete: :delete_all, type: :binary_id), null: false
    end

    create unique_index(:favourite_tags, [:tag_id, :person_id])
  end
end
