defmodule LoggerFluentdBackend.Receiver do
  use GenServer
  require Logger

  def start_link() do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init(_options) do
    {:ok, nil}
  end

  # Agregar metodo de handleo de datos.
  def handle_cast({:forward_log, level, _node, msg}, config) do
    case level do
      "warn" ->
        Logger.warn(msg)

      "error" ->
        Logger.error(msg)

      _ ->
        Logger.error(msg)
    end

    {:noreply, config}
  end
end
