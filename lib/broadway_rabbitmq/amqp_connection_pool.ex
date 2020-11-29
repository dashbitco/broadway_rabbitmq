defmodule BroadwayRabbitMQ.AMQPConnectionPool do
  use Supervisor

  @spec start_link({keyword(), atom()}) :: Supervisor.on_start()
  def start_link({options, broadway_name}) when is_list(options) and is_atom(broadway_name) do
    name = Module.concat(broadway_name, AMQPConnectionPool)
    Supervisor.start_link(__MODULE__, {options, broadway_name}, name: name)
  end

  @impl true
  def init({producer_options, broadway_name}) do
    pool_options = Keyword.fetch!(producer_options, :connection_pool)

    indexes =
      pool_options[:size]
      |> List.duplicate(:unused)
      |> Enum.with_index()
      |> Enum.map(fn {:unused, index} -> index end)

    children =
      for index <- indexes do
        name = Module.concat(broadway_name, :"AMQPConnection#{index}")
        connection_options = [url: pool_options[:url], index: index, name: name]
        {BroadwayRabbitMQ.AMQPConnection, connection_options}
      end

    Supervisor.init(children, strategy: :one_for_one)
  end
end
