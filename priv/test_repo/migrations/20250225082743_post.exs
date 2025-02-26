defmodule TestRepo.Migrations.Post do
  use Ecto.Migration

  def change do
    create table("posts") do
      add :name, :string
      add :body, :string
      add :person_id, references(:persons, on_delete: :nilify_all)
    end
  end
end
