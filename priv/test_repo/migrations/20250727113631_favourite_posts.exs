defmodule TestRepo.Migrations.FavouritePosts do
  use Ecto.Migration

  def change do

    create table(:favourite_posts, primary_key: false) do
      add :id, :binary_id, primary_key: true
      add :post_id, references(:posts, on_delete: :delete_all, type: :binary_id), null: false
      add :person_id, references(:persons, on_delete: :delete_all, type: :binary_id), null: false
    end

    create unique_index(:favourite_posts, [:post_id, :person_id])
  end
end
