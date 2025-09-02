defmodule TestRepo.Migrations.Person do
  use Ecto.Migration

  def change do
    create table("persons") do
      add :name, :string
      add :other, :integer, [:increment, start_value: 0]
    end
  end
end
