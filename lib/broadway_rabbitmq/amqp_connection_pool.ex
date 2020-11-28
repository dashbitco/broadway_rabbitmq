defmodule BroadwayRabbitMQ.AMQPConnectionPool do
  use Supervisor

  @spec start_link({keyword(), atom()}) :: Supervisor.on_start()
  def start_link({options, broadway_name}) when is_list(options) and is_atom(broadway_name) do
    name = Module.concat(broadway_name, AMQPConnectionPool)
    Supervisor.start_link(__MODULE__, options, name: name)
  end

  @impl true
  def init(producer_options) do
    pool_options = producer_options[:connection_pool]

    indexes =
      pool_options[:size]
      |> List.duplicate(:unused)
      |> Enum.with_index()
      |> Enum.map(fn {:unused, index} -> index end)

    children =
      for index <- indexes do
        options = Keyword.put(pool_options, :index, index)
        {BroadwayRabbitMQ.AMQPConnection, pool_options}
      end

    Supervisor.init(children, strategy: :one_for_one)
  end
end
