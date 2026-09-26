defmodule TestRepo.Migrations.PostsLabels do
  use Ecto.Migration

  def change do
    create table(:posts_labels, primary_key: false) do
      add :id, :binary_id, primary_key: true
      add :label_id, references(:labels, on_delete: :delete_all, type: :binary_id), null: false
      add :post_id, references(:posts, on_delete: :delete_all, type: :binary_id), null: false
    end

    create unique_index(:posts_labels, [:label_id, :post_id])
  end
end
