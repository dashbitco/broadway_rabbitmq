defmodule BroadwayRabbitMQ.ChannelPool do
  @moduledoc """
  A behaviour used to implement custom AMQP channel pools.

  By default, BroadwayRabbitMQ handles its own pool of AMQP connections and channels. However,
  there might be use cases where you want to use your own existing pool or custom pooling logic.
  In those cases, you can implement a custom channel pool using this behaviour.

  To use a custom pool, pass `{:custom_pool, module, args}` as the value of the `:connection`
  option in `BroadwayRabbitMQ.Producer`. `module` needs to implement this behaviour, and `args` is
  passed down to `c:checkout_channel/1` and `c:checkin_channel/2`.

  ## Examples

  Imagine we pass this option when starting the producer:

      connection: {:custom_pool, MyPool, _amqp_connection = :big_pool}

  Then, we could define the custom pool as:

      defmodule MyPool do
        @behaviour BroadwayRabbitMQ.ChannelPool

        @impl true
        def checkout_channel(name) do
          conn = %AMQP.Connection{pid: Process.whereis(name)}

          case AMQP.Channel.open(conn) do
            {:ok, channel} -> {:ok, channel}
            {:error, reason} -> {:error, %RuntimeError{message: inspect(reason)}}
          end
        end

        @impl true
        def checkin_channel(_name, channel) do
          case AMQP.Channel.close(channel) do
            :ok -> :ok
            {:error, reason} -> {:error, %RuntimeError{message: inspect(reason)}}
          end
        end
      end

  """

  @moduledoc since: "0.8.0"

  @doc """
  Invoked to check out a AMQP channel from the pool.

  If there is an error, you can return any exception as `{:error, exception}`.

  This callback is invoked from a `BroadwayRabbitMQ.Producer` process. If you need
  the PID of that process, call `self()` in your implementation. This can be useful
  for things like linking or monitoring.
  """
  @callback checkout_channel(args :: term()) ::
              {:ok, AMQP.Channel.t()} | {:error, reason :: Exception.t()}

  @doc """
  Invoked to check a channel back into the pool.

  `channel` is a channel that was returned by `c:checkout_channel/1`.

  If there is an error, you can return any exception as `{:error, exception}`.

  In case your pool fails to handle this properly, BroadwayRabbitMQ will try to close the channel
  by itself using `AMQP.Channel.close/1`.
  """
  @callback checkin_channel(args :: term(), channel :: AMQP.Channel.t()) ::
              :ok | {:error, reason :: Exception.t()}
end
