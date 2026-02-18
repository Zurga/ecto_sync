defmodule TestRepo.Migrations.AddPublication do
  use Ecto.Migration

  def change do
    execute "create publication ecto_sync for all tables"
  end
end
