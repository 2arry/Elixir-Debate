defmodule Fairway.Application do
  @moduledoc """
  Starts Fairway at boot when the application environment names a
  configuration file:

      config :fairway, config_file: "/etc/fairway.yml"

  The file is validated before anything is started. If it is invalid the
  application does not start, and the reason names the key at fault.

  Without `:config_file` nothing is started, and the host application is
  expected to put `{Fairway, config_file: path}` in its own supervision tree.
  """

  use Application

  require Logger

  @impl true
  def start(_type, _args) do
    with {:ok, children} <- children(Application.get_env(:fairway, :config_file)) do
      Supervisor.start_link(children, strategy: :one_for_one, name: __MODULE__)
    end
  end

  defp children(nil), do: {:ok, []}

  defp children(path) do
    case Fairway.Config.load(path) do
      {:ok, config} ->
        {:ok, [{Fairway, config: config}]}

      {:error, error} ->
        message = Exception.message(error)
        Logger.error(message)
        {:error, {:invalid_config, message}}
    end
  end
end
