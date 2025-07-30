defmodule EctoSync.Graph do
  @moduledoc false
  import EctoSync.Helpers, only: [ecto_schema_mod?: 1]
  require Logger

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

    {edges, edge_fields} =
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
      |> Graph.add_edges(edges)

    vertices = Graph.vertices(graph)

    vertex_pairs =
      for v <- vertices, v2 <- vertices, reduce: %{} do
        acc ->
          if v != v2 and not is_nil(Graph.get_shortest_path(graph, v, v2)) do
            paths =
              Graph.get_paths(graph, v, v2)
              |> Enum.flat_map(&normalize_path(&1, join_modules, edge_fields, []))

            Map.put(acc, {v, v2}, paths)
          else
            acc
          end
      end

    {vertex_pairs, join_modules}
  end

  defp normalize_path([_], _, _, acc) do
    acc |> Enum.reverse() |> List.flatten()
  end

  defp normalize_path([parent, child], join_modules, edge_fields, acc) do
    child =
      (get_in(join_modules, [child, parent]) || child)
      |> case do
        {_, child} -> child
        child -> child
      end

    Map.get(edge_fields, {parent, child})
    |> Enum.reduce(acc, &Keyword.put(&2, &1, []))
  end

  defp normalize_path([parent, join, child | rest], join_modules, edge_fields, acc)
       when is_atom(join) do
    {edge, next} =
      if Enum.member?(Map.keys(join_modules), join) do
        {{parent, child}, [child | rest]}
      else
        {{parent, join}, [join, child | rest]}
      end

    fields = Map.get(edge_fields, edge)

    for field <- fields do
      # fav, posts
      Keyword.put(acc, field, normalize_path(next, join_modules, edge_fields, []))
    end
  end
end
