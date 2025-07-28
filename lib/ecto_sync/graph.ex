defmodule EctoSync.Graph do
  @moduledoc false
  import EctoSync.Helpers, only: [ecto_schema_mod?: 1]
  defstruct [:edges, :nodes]
  require Logger
  @type t :: %__MODULE__{}

  def new(modules) do
    assocs =
      modules
      |> Enum.flat_map(fn module ->
        if ecto_schema_mod?(module) do
          module.__schema__(:associations)
          |> Enum.map(&module.__schema__(:association, &1))
        else
          []
        end
      end)

    join_modules =
      Enum.flat_map(assocs, fn
        %Ecto.Association.ManyToMany{
          join_through: join_through,
          owner: from,
          related: to,
          join_keys: [{from_fk, _}, {to_fk, _}]
        } ->
          [
            {join_through,
             %{
               from => {to_fk, to},
               to => {from_fk, from}
             }}
          ]

        _ ->
          []
      end)
      |> Enum.into(%{})

    {module_edges, fields} =
      assocs
      |> Enum.reduce(
        {[], %{}},
        fn %{owner: from, related: to, field: field} = assoc, {mod_acc, field_acc} ->
          field_acc = Map.update(field_acc, {from, to}, [field], &[field | &1])

          mod_acc =
            case assoc do
              %Ecto.Association.ManyToMany{
                join_through: join_through
              } ->
                [{from, join_through} | mod_acc]

              _ ->
                [{from, to} | mod_acc]
            end

          {mod_acc, field_acc}
        end
      )

    graph =
      Graph.new()
      |> Graph.add_edges(module_edges)

    vertices = Graph.vertices(graph)

    vertex_pairs =
      for v <- vertices, v2 <- vertices, reduce: %{} do
        acc ->
          if v != v2 and not is_nil(Graph.get_shortest_path(graph, v, v2)) do
            paths =
              Graph.get_paths(graph, v, v2)
              |> Enum.flat_map(fn path ->
                match_slice(
                  path,
                  join_modules,
                  &Map.get(fields, &1),
                  []
                )
                |> List.flatten()
              end)

            Map.put(acc, {v, v2}, paths)
          else
            acc
          end
      end

    {vertex_pairs, join_modules}
  end

  defp match_slice([_], _, _, acc) do
    acc |> Enum.reverse()
  end

  defp match_slice([parent, child], join_modules, fun, acc) do
    child =
      (get_in(join_modules, [child, parent]) || child)
      |> case do
        {_, child} -> child
        child -> child
      end

    fun.({parent, child})
    |> Enum.reduce(acc, &Keyword.put(&2, &1, []))
  end

  defp match_slice([parent, join, child | rest], join_modules, fun, acc)
       when is_atom(join) do
    if Enum.member?(Map.keys(join_modules), join) do
      Keyword.put(
        acc,
        fun.({parent, child}) |> Enum.at(0),
        match_slice([child | rest], join_modules, fun, [])
      )
    else
      for field <- fun.({parent, join}) do
        # fav, posts
        Keyword.put(acc, field, match_slice([join, child | rest], join_modules, fun, []))
      end
    end
  end
end
