defmodule TestRepo.Migrations.Tag do
  use Ecto.Migration

  def change do
    create table("tags") do
      add :name, :string
    end
  end
end
