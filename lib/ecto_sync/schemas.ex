defmodule EctoSync.Schemas do
  @moduledoc false
  import EctoSync.Helpers, only: [ecto_schema_mod?: 1, reduce_assocs: 3, resolve_through: 2]
  require Logger

  defstruct ~w/paths join_modules has_through edge_fields/a

  def new(modules) do
    modules = Enum.filter(modules, &ecto_schema_mod?/1)

    join_modules =
      modules
      |> Enum.reduce([], fn module, acc ->
        reduce_assocs(module, acc, fn
          {_, assoc}, acc ->
            case assoc do
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
                  | acc
                ]

              _ ->
                acc
            end
        end)
      end)
      |> Enum.into(%{})

    {edges, edge_fields} =
      modules
      |> Enum.reduce(
        {[], %{}},
        &reduce_assocs(&1, &2, fn {field, %{owner: from} = assoc}, {edge_acc, field_acc} ->
          {edges, related} =
            case assoc do
              %Ecto.Association.ManyToMany{
                join_through: join_through,
                join_keys: [{owner_key, _}, _],
                related: related
              } ->
                edges =
                  if is_binary(join_through) do
                    [{from, join_through}, {join_through, from}]
                  else
                    [{from, join_through}]
                  end

                {edges, related}

              %Ecto.Association.HasThrough{through: through} ->
                to = resolve_through(schema, through)

                {[{from, to}], to}

              %{related: to} ->
                {[{from, to}], to}
            end

          field_acc =
            Map.update(field_acc, {from, related}, [field], fn fields -> [field | fields] end)

          {edges ++ edge_acc, field_acc}
        end)
      )

    graph =
      Graph.new()
      |> Graph.add_edges(edges)

    vertices = Graph.vertices(graph)

    paths =
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

    %__MODULE__{paths: paths, join_modules: join_modules, edge_fields: edge_fields}
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

    case Map.get(edge_fields, {parent, child}) do
      nil ->
        acc

      fields ->
        Enum.reduce(fields, acc, &Keyword.put(&2, &1, []))
    end
  end

  defp normalize_path([parent, join, child | rest], join_modules, edge_fields, acc) do
    {edge, next} =
      if Enum.member?(Map.keys(join_modules), join) do
        {{parent, child}, [child | rest]}
      else
        {{parent, join}, [join, child | rest]}
      end

    case Map.get(edge_fields, edge) do
      nil ->
        acc

      fields ->
        for field <- fields do
          # fav, posts
          Keyword.put(acc, field, normalize_path(next, join_modules, edge_fields, []))
        end
    end
  end
end
