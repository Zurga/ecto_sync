defmodule TestRepo do
  @moduledoc false
  use Ecto.Repo, otp_app: :ecto_sync, adapter: Ecto.Adapters.Postgres
end

defmodule TestSyncRepo do
  @moduledoc false
  use EctoSync.Repo, otp_app: :ecto_sync, adapter: Ecto.Adapters.Postgres
end
