defmodule BroadwayRabbitMQ.AmqpClient do
  @moduledoc false

  alias AMQP.{
    Connection,
    Channel,
    Basic
  }

  require Logger

  @behaviour BroadwayRabbitMQ.RabbitmqClient

  @default_prefetch_count 50
  @supported_options [
    :queue,
    :connection,
    :qos,
    :backoff_min,
    :backoff_max,
    :backoff_type,
    :requeue,
    :metadata
  ]

  @requeue_options [
    :never,
    :always,
    :once
  ]

  @default_metadata []

  @requeue_default_option :always

  @impl true
  def init(opts) do
    with {:ok, opts} <- validate_supported_opts(opts, "Broadway", @supported_options),
         {:ok, metadata} <- validate(opts, :metadata, @default_metadata),
         {:ok, queue} <- validate(opts, :queue),
         {:ok, requeue} <- validate(opts, :requeue, @requeue_default_option),
         {:ok, conn_opts} <- validate_conn_opts(opts),
         {:ok, qos_opts} <- validate_qos_opts(opts) do
      {:ok, queue,
       %{
         connection: conn_opts,
         qos: qos_opts,
         requeue: requeue,
         metadata: metadata
       }}
    end
  end

  @impl true
  def setup_channel(config) do
    with {:ok, conn} <- Connection.open(config.connection),
         {:ok, channel} <- Channel.open(conn),
         :ok <- Basic.qos(channel, config.qos) do
      {:ok, channel}
    end
  end

  @impl true
  def ack(channel, delivery_tag) do
    Basic.ack(channel, delivery_tag)
  end

  @impl true
  def reject(channel, delivery_tag, opts) do
    Basic.reject(channel, delivery_tag, opts)
  end

  @impl true
  def consume(channel, queue) do
    {:ok, consumer_tag} = Basic.consume(channel, queue)
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

  defp validate_option(:queue, value) when not is_binary(value) or value == "",
    do: validation_error(:queue, "a non empty string", value)

  defp validate_option(:requeue, value) when value not in @requeue_options,
    do: validation_error(:queue, "any of #{inspect(@requeue_options)}", value)

  defp validate_option(:metadata, value) when is_list(value) do
    if Enum.all?(value, &is_atom/1),
      do: {:ok, value},
      else: validation_error(:metadata, "a list of atoms", value)
  end

  defp validate_option(_, value), do: {:ok, value}

  defp validation_error(option, expected, value) do
    {:error, "expected #{inspect(option)} to be #{expected}, got: #{inspect(value)}"}
  end

  defp validate_conn_opts(opts) do
    group = :connection
    conn_opts = opts[group] || []

    supported = [
      :username,
      :password,
      :virtual_host,
      :host,
      :port,
      :channel_max,
      :frame_max,
      :heartbeat,
      :connection_timeout,
      :ssl_options,
      :client_properties,
      :socket_options
    ]

    validate_supported_opts(conn_opts, group, supported)
  end

  defp validate_qos_opts(opts) do
    group = :qos
    qos_opts = opts[group] || []
    supported = [:prefetch_size, :prefetch_count]

    qos_opts
    |> Keyword.put_new(:prefetch_count, @default_prefetch_count)
    |> validate_supported_opts(group, supported)
  end

  defp validate_supported_opts("amqp" <> _ = uri, :connection, _supported_opts) do
    {:ok, uri}
  end

  defp validate_supported_opts(opts, group_name, supported_opts) do
    opts
    |> Keyword.keys()
    |> Enum.reject(fn k -> k in supported_opts end)
    |> case do
      [] -> {:ok, opts}
      keys -> {:error, "Unsupported options #{inspect(keys)} for #{inspect(group_name)}"}
    end
  end
end
