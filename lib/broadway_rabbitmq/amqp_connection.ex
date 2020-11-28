defmodule BroadwayRabbitMQ.AMQPConnection do
  use GenServer

  def start_link(options) when is_list(options) do
    GenServer.start_link(__MODULE__, options)
  end

  @impl true
  def init(options) do
  end
end
