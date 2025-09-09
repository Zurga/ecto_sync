defmodule EctoSync.Telemetry do
  use Agent

  def start_link(_) do
    Agent.start_link(fn -> %{} end, name: __MODULE__)
  end

  def get_stats do
    Agent.get(__MODULE__, & &1)
  end

  def stats(schema, hit_or_miss) do
    default = %{schema => 1}

    Agent.update(__MODULE__, fn values ->
      Map.update(values, hit_or_miss, default, fn existing ->
        Map.update(existing, schema, 1, &(&1 + 1))
      end)
    end)
  end
end
