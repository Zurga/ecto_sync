defmodule TestRepo.Migrations.FavouriteTags do
  use Ecto.Migration

  def change do
    create table(:favourite_tags) do
      add :tag_id, references(:tags, on_delete: :delete_all)
      add :person_id, references(:persons, on_delete: :delete_all)
    end

    create unique_index(:favourite_tags, [:tag_id, :person_id])
  end
end
