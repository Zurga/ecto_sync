defmodule TestRepo.Migrations.PostsTags do
  use Ecto.Migration

  def change do
    create table(:posts_tags) do
      add :tag_id, references(:tags, on_delete: :delete_all)
      add :post_id, references(:posts, on_delete: :delete_all)
    end

    create unique_index(:posts_tags, [:tag_id, :post_id])
  end
end
