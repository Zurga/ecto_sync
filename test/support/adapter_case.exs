defmodule EctoSync.AdapterCase do
  @moduledoc false
  use ExUnit.CaseTemplate

  using do
    quote location: :keep do
      import EctoSync.RepoCase
      import Ecto
    end
  end
end
