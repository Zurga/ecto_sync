defmodule TestRepo.Migrations.Person do
  use Ecto.Migration

  def change do
    create table("persons", primary_key: false) do
      add :id, :binary_id, primary_key: true
      add :name, :string
      add :other, :integer, [:increment, start_value: 0]
    end
  end
end
