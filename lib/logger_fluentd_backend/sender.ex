defmodule LoggerFluentdBackend.Sender do
  use GenServer

  defmodule State do
    defstruct socket: nil,
              node_list: nil
  end

  def init(_) do
    env = Application.get_env(:logger, :logger_fluentd_backend, [])
    node_list = Keyword.get(env, :node_list, [])
    {:ok, %State{node_list: node_list}}
  end

  def send(tag, data, host, port, serializer) do
    options = [host: host, port: port, serializer: serializer]
    :ok = GenServer.cast(__MODULE__, {:send, tag, data, options})
  end

  def send(tag, data, host, port), do: send(tag, data, host, port, :msgpack)

  def stop() do
    GenServer.call(__MODULE__, {:stop, []})
  end

  def handle_call({:stop, _}, _from, _state) do
    {:reply, :ok, %State{socket: nil}}
  end

  def terminate(_reason, %State{socket: socket}) when not is_nil(socket) do
    Socket.Stream.close(socket)
  end

  def start_link() do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def handle_cast({_, _, data, options} = msg, %State{socket: nil} = state) do
    socket = connect(data, options, state)
    handle_cast(msg, %State{state | socket: socket})
  end

  def handle_cast({:send, tag, data, options}, %State{socket: socket} = state) do
    packet = serializer(options[:serializer]).([tag, now(), data])
    new_state =
    case Socket.Stream.send(socket, packet) do
      {:error, _reason} ->
        #fail sending msg to the fluentd Server, sending msg to the distributed system.
        GenServer.abcast(state.node_list, LoggerFluentdBackend.Receiver, {:forward_log, data.level, node(), data.message})
        %State{state| socket: nil}
      _ ->
        state
    end
    {:noreply, new_state}
  end

  defp connect(data, options, state) do
    host = options[:host] || "localhost"
    port = options[:port] || 24224
    #add better error handler
    case Socket.TCP.connect(host, port, packet: 0) do
      {:ok, socket} ->
        socket
      {:error, _reason} ->
        #fail to connect to the fluentd Server, sending msg to the distributed system.
        GenServer.abcast(state.node_list, LoggerFluentdBackend.Receiver, {:forward_log, data.level, node(), data.message})
    end
  end

  defp serializer(:msgpack), do: &Msgpax.pack!/1
  defp serializer(:json), do: &Poison.encode!/1
  defp serializer(f) when is_function(f, 1), do: f

  defp now() do
    {msec, sec, _} = :os.timestamp()
    msec * 1_000_000 + sec
  end
end
