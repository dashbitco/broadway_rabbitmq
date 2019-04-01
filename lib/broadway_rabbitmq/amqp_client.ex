defmodule BroadwayRabbitmq.AmqpClient do
  @moduledoc false

  alias AMQP.{
    Connection,
    Channel,
    Queue,
    Basic
  }

  require Logger

  @behaviour BroadwayRabbitmq.RabbitmqClient

  @default_prefetch_count 50

  @impl true
  def init(opts) do
    with {:ok, queue_name} <- validate(opts, :queue_name),
         {:ok, conn_opts} <- validate_conn_opts(opts),
         {:ok, declare_opts} <- validate_declare_opts(opts),
         {:ok, qos_opts} <- validate_qos_opts(opts) do
      {:ok, queue_name,
       %{
         connection: conn_opts,
         declare: declare_opts,
         qos: qos_opts
       }}
    end
  end

  @impl true
  def setup_channel(queue_name, config) do
    with {:ok, conn} <- Connection.open(config.connection),
         {:ok, channel} <- Channel.open(conn),
         {:ok, _} <- Queue.declare(channel, queue_name, config.declare),
         :ok <- Basic.qos(channel, config.qos) do
      {:ok, channel}
    end
  end

  @impl true
  def ack(channel, delivery_tag) do
    Basic.ack(channel, delivery_tag)
  end

  @impl true
  def reject(channel, delivery_tag) do
    Basic.reject(channel, delivery_tag)
  end

  @impl true
  def consume(channel, queue_name) do
    {:ok, consumer_tag} = Basic.consume(channel, queue_name)
    consumer_tag
  end

  @impl true
  def cancel(channel, consumer_tag) do
    Basic.cancel(channel, consumer_tag)
  end

  @impl true
  def close_connection(conn) do
    if Process.alive?(conn.pid) do
      Connection.close(conn)
    else
      :ok
    end
  end

  defp validate(opts, key, default \\ nil) when is_list(opts) do
    validate_option(key, opts[key] || default)
  end

  defp validate_option(:queue_name, value) when not is_binary(value) or value == "",
    do: validation_error(:queue_name, "a non empty string", value)

  defp validate_option(_, value), do: {:ok, value}

  defp validation_error(option, expected, value) do
    {:error, "expected #{inspect(option)} to be #{expected}, got: #{inspect(value)}"}
  end

  defp validate_conn_opts(opts) do
    {:ok, opts[:connection] || []}
    # TODO: validate options
    #   :username,
    #   :password,
    #   :virtual_host,
    #   :host,
    #   :port,
    #   :channel_max,
    #   :frame_max,
    #   :heartbeat,
    #   :connection_timeout,
    #   :ssl_options,
    #   :client_properties,
    #   :socket_options
  end

  defp validate_declare_opts(opts) do
    {:ok, opts[:declare] || []}
    # TODO: validate options
    #   :durable,
    #   :auto_delete,
    #   :exclusive,
    #   :passive
  end

  defp validate_qos_opts(opts) do
    qos = Keyword.put_new(opts[:qos] || [], :prefetch_count, @default_prefetch_count)
    {:ok, qos}
    # TODO: validate options
    #   :prefetch_size,
    #   :prefetch_count,
    #   :global (don't use it. It doesn't make any difference with the current implementation)
  end
end
