defmodule TestRepo.Migrations.FavouritePosts do
  use Ecto.Migration

  def change do

    create table(:favourite_posts) do
      add :post_id, references(:posts, on_delete: :delete_all)
      add :person_id, references(:persons, on_delete: :delete_all)
    end

    create unique_index(:favourite_posts, [:post_id, :person_id])
  end
end
