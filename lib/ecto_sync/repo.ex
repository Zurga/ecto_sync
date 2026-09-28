defmodule EctoSync.Repo do
  @moduledoc """
  Drop-in Repo wrapper that adds per-process write counters for Schema based Repo functions

  Usage:

      defmodule MyApp.Repo do
        use EctoSync.Repo,
          otp_app: :my_app
      end
  """
  alias Ecto.Changeset

  defmacro __using__(opts) do
    quote do
      use Ecto.Repo, unquote(opts)

      unquote(generate_wrappers())
    end
  end

  @write_funs [
    insert: 2,
    insert!: 2,
    update: 2,
    update!: 2,
    delete: 2,
    delete!: 2,
    insert_or_update: 2,
    insert_or_update!: 2
  ]

  defp generate_wrappers do
    for {fun, arity} <- @write_funs do
      args = Macro.generate_arguments(arity, __MODULE__)

      quote do
        defoverridable [{unquote(fun), unquote(arity)}]

        def unquote(fun)(unquote_splicing(args)) do
          repo = get_dynamic_repo()
          [_value, opts] = unquote(args)
          tuplet = Ecto.Repo.Supervisor.tuplet(repo, prepare_opts(unquote(fun), opts))

          value = unquote(__MODULE__).__handle__(unquote(fun), unquote(args), tuplet)

          try do
            case super(value, opts) do
              {:error, _} = result -> throw({:operation_failed, result})
              result -> result
            end
          rescue
            error ->
              unquote(__MODULE__).__handle__({:failed, unquote(fun)}, unquote(args), tuplet)

              reraise error, __STACKTRACE__
          end
        catch
          {:operation_failed, result} ->
            unquote(__MODULE__).__handle__({:failed, unquote(fun)}, unquote(args), {})

            result
        end
      end
    end
  end

  def __handle__(insert, [value | _args], {adapter_meta, _opts})
      when insert in ~w/insert insert!/a do
    %{adapter: adapter} = adapter_meta

    with {key, _source, :binary_id = type} <- binary_id(value) do
      if dump_value = Ecto.Type.adapter_autogenerate(adapter, type) do
        {:ok, cast_value} = Ecto.Type.adapter_load(adapter, type, dump_value)

        prepared =
          case value do
            %Changeset{changes: changes} = changeset ->
              %{
                changeset
                | changes: Map.put_new(changes, key, cast_value)
              }

            _ when is_map(value) ->
              Map.put_new(value, key, cast_value)
          end

        EctoSync.increment_row_ref(prepared)
        prepared
      end
    else
      _ ->
        value
    end
  end

  @doc false
  def __handle__(delete, [value | _args], _tuplet) when delete in ~w/delete delete!/a do
    value
  end

  @doc false
  def __handle__(fun_name, [value | _args], _tuplet) when fun_name in ~w/update update!/a do
    EctoSync.increment_row_ref(value)
    value
  end

  @doc false
  def __handle__({:failed, _fun}, [value | _args], _tuplet) do
    EctoSync.decrement_row_ref(value)
    value
  end

  defp binary_id(%Changeset{data: data}), do: binary_id(data)

  defp binary_id(%schema_mod{}) do
    schema_mod.__schema__(:autogenerate_id)
  end

  defp binary_id(_other), do: false
end
