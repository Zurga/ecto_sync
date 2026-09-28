defmodule TestRepo.Migrations.Tag do
  use Ecto.Migration

  def change do
    create table("tags", primary_key: false) do
      add :id, :binary_id, primary_key: true
      add :name, :string
    end
  end
end
