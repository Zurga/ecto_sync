defmodule EctoSync.Repo do
  @moduledoc """
  Drop-in Repo wrapper that adds per-process write counters for Schema based Repo functions

  Usage:

      defmodule MyApp.Repo do
        use EctoSync.Repo,
          otp_app: :my_app
      end
  """

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
          unquote(__MODULE__).__capture_op__(
            unquote(fun),
            unquote(args),
            &EctoSync.increment_row_ref/1
          )

          try do
            case super(unquote_splicing(args)) do
              {:error, _} = result -> throw({:operation_failed, result})
              result -> result
            end
          rescue
            error ->
              unquote(__MODULE__).__capture_op__(
                unquote(fun),
                unquote(args),
                &EctoSync.increment_row_ref/1
              )

              reraise error, __STACKTRACE__
          end
        catch
          {:operation_failed, result} ->
            unquote(__MODULE__).__capture_op__(
              unquote(fun),
              unquote(args),
              &EctoSync.increment_row_ref/1
            )

            result
        end
      end
    end
  end

  @doc false
  def __capture_op__(fun_name, args, ecto_sync_fun) do
    key =
      case {fun_name, args} do
        {insert, [struct, _opts]} when insert in ~w/insert insert!/a ->
          {struct, :inserted}

        {_op, [struct_or_changeset, _opts]} ->
          struct_or_changeset

        _ ->
          nil
      end

    unless is_nil(key) do
      ecto_sync_fun.(key)
    end
  end
end
