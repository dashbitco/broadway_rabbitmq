defmodule BroadwayRabbitMQ.ChannelPool do
  @moduledoc """
  A behaviour used to handle channel pooling to a custom pool.
  If you want to use a pool with BroadwayRabbitMQ, your pool must implement this behaviour.
  When configuring the BroadwayRabbitMQ connection, set the value `{:custom_pool, module, options}`.
  Where module is any module implementing this behaviour and options will be passed to the callbacks.
  """

  @doc """
  Invoked when BroadwayRabbitMQ needs a channel.
  """
  @callback checkout_channel(options :: term()) ::
              {:ok, AMQP.Channel.t()} | {:error, reason :: Exception.t()}

  @doc """
  Invoked when BroadwayRabbitMQ doesn't need a channel anymore.
  In case your pool fails to handle this properly, BroadwayRabbitMQ will try to close the channel itself.
  """
  @callback checkin_channel(options :: term()) :: :ok | {:error, reason :: Exception.t()}
end
