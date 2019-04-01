defmodule BroadwayRabbitmq.RabbitmqClient do
  @moduledoc false

  alias AMQP.{Basic, Channel, Connection}

  @typep config :: %{
           connection: keyword,
           declare: keyword,
           qos: keyword
         }

  @callback init(opts :: any) :: {:ok, queue_name :: String.t(), config} | {:error, any}
  @callback setup_channel(queue_name :: String.t(), config) :: {:ok, Channel.t()} | {:error, any}
  @callback ack(channel :: Channel.t(), delivery_tag :: Basic.delivery_tag()) :: any
  @callback reject(channel :: Channel.t(), delivery_tag :: Basic.delivery_tag()) :: any
  @callback consume(channel :: Channel.t(), queue_name :: Basic.queue()) :: Basic.consumer_tag()
  @callback cancel(channel :: Channel.t(), Basic.consumer_tag()) :: :ok | Basic.error()
  @callback close_connection(conn :: Connection.t()) :: :ok | {:error, any}
end
