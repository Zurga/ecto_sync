import Config

config :logger, backends: []

config :ecto_sync, TestRepo,
  username: "postgres",
  password: "postgres",
  database: "ecto_sync_test",
  hostname: System.get_env("DB_HOST", "localhost"),
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: 30,
  queue_target: 5000,
  queue_interval: 5000

config :ecto_sync, TestSyncRepo,
  username: "postgres",
  password: "postgres",
  database: "ecto_sync_test_sync",
  hostname: System.get_env("DB_HOST", "localhost"),
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: 30,
  queue_target: 5000,
  queue_interval: 5000
