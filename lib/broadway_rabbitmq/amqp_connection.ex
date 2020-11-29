defmodule BroadwayRabbitMQ.AMQPConnection do
  use GenServer

  defstruct [:url, :index, :connection]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(options) when is_list(options) do
    name = Keyword.fetch!(options, :name)
    GenServer.start_link(__MODULE__, options, name: name)
  end

  @spec open_channel(atom()) :: {:ok, AMQP.Channel.t()} | {:error, reason :: term()}
  def open_channel(connection) when is_atom(connection) do
    GenServer.call(connection, :open_channel)
  end

  @impl true
  def init(options) do
    state = %__MODULE__{
      url: Keyword.fetch!(options, :url),
      index: Keyword.fetch!(options, :index),
      connection: nil
    }

    {:ok, state, {:continue, :connect}}
  end

  @impl true
  def handle_continue(:connect, state) do
    case AMQP.Connection.open(state.url) do
      {:ok, connection} ->
        true = Process.link(connection.pid)
        state = %__MODULE__{state | connection: connection}
        {:noreply, state}

      {:error, _reason} ->
        :backoff
    end
  end

  @impl true
  def handle_info({:EXIT, pid, reason}, %__MODULE__{connection: %{pid: pid}} = state) do
    :backoff
  end

  @impl true
  def handle_call(:open_channel, _from, state) do
    {:reply, AMQP.Channel.open(state.connection), state}
  end
end
