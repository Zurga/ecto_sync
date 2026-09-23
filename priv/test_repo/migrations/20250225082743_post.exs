defmodule TestRepo.Migrations.Post do
  use Ecto.Migration

  def change do
    create table("posts", primary_key: false) do
      add :id, :binary_id, primary_key: true
      add :name, :string
      add :body, :string
      add :person_id, references(:persons, on_delete: :nilify_all, type: :binary_id)
      add :comment_id, references(:posts, type: :binary_id)
      add :other, references(:persons, on_delete: :nilify_all, type: :binary_id)
    end
  end
end
