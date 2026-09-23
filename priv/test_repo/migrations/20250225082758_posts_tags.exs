defmodule TestRepo.Migrations.PostsTags do
  use Ecto.Migration

  def change do
    create table(:posts_tags, primary_key: false) do
      add :id, :binary_id, primary_key: true
      add :tag_id, references(:tags, on_delete: :delete_all, type: :binary_id), null: false
      add :post_id, references(:posts, on_delete: :delete_all, type: :binary_id), null: false
    end

    create unique_index(:posts_tags, [:tag_id, :post_id])
  end
end
