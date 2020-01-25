defmodule BroadwayRabbitMQ.AmqpClient do
  @moduledoc false

  alias AMQP.{
    Connection,
    Channel,
    Basic,
    Queue
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
    :metadata,
    :declare,
    :bindings,
    :broadway,
    :merge_options
  ]

  @default_metadata []

  @impl true
  def init(opts) do
    with {:ok, merge_opts} <- validate_merge_opts(opts),
         opts = Keyword.merge(opts, merge_opts),
         {:ok, opts} <- validate_supported_opts(opts, "Broadway", @supported_options),
         {:ok, metadata} <- validate(opts, :metadata, @default_metadata),
         {:ok, queue} <- validate(opts, :queue),
         {:ok, conn_opts} <- validate_conn_opts(opts),
         {:ok, declare_opts} <- validate_declare_opts(opts, queue),
         {:ok, bindings} <- validate_bindings(opts),
         {:ok, qos_opts} <- validate_qos_opts(opts) do
      {:ok,
       %{
         connection: conn_opts,
         queue: queue,
         declare_opts: declare_opts,
         bindings: bindings,
         qos: qos_opts,
         metadata: metadata
       }}
    end
  end

  @impl true
  def setup_channel(config) do
    with {:ok, conn} <- Connection.open(config.connection),
         {:ok, channel} <- Channel.open(conn),
         :ok <- Basic.qos(channel, config.qos),
         {:ok, queue} <- maybe_declare_queue(channel, config.queue, config.declare_opts),
         :ok <- maybe_bind_queue(channel, queue, config.bindings) do
      {:ok, channel}
    end
  end

  defp maybe_declare_queue(_channel, queue, _declare_opts = nil) do
    {:ok, queue}
  end

  defp maybe_declare_queue(channel, queue, declare_opts) do
    with {:ok, %{queue: queue}} <- Queue.declare(channel, queue, declare_opts) do
      {:ok, queue}
    end
  end

  defp maybe_bind_queue(_channel, _queue, _bindings = []) do
    :ok
  end

  defp maybe_bind_queue(channel, queue, [{exchange, opts} | bindings]) do
    case Queue.bind(channel, queue, exchange, opts) do
      :ok -> maybe_bind_queue(channel, queue, bindings)
      {:error, reason} -> {:error, reason}
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
  def consume(channel, config) do
    {:ok, consumer_tag} = Basic.consume(channel, config.queue)
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

  defp validate_merge_opts(opts) do
    case Keyword.fetch(opts, :merge_options) do
      {:ok, fun} when is_function(fun, 1) ->
        index = opts[:broadway][:index] || raise "missing broadway index"
        merge_opts = fun.(index)

        if Keyword.keyword?(merge_opts) do
          {:ok, merge_opts}
        else
          message =
            "The :merge_options function should return a keyword list, " <>
              "got: #{inspect(merge_opts)}"

          {:error, message}
        end

      {:ok, other} ->
        {:error, ":merge_options must be a function with arity 1, got: #{inspect(other)}"}

      :error ->
        {:ok, _merge_opts = []}
    end
  end

  defp validate(opts, key, default \\ nil) when is_list(opts) do
    validate_option(key, opts[key] || default)
  end

  defp validate_option(:queue, value) when not is_binary(value),
    do: validation_error(:queue, "a string", value)

  defp validate_option(:connection, value) when not (is_binary(value) or is_list(value)),
    do: validation_error(:connection, "a URI or a keyword list", value)

  defp validate_option(:metadata, value) when is_list(value) do
    if Enum.all?(value, &is_atom/1),
      do: {:ok, value},
      else: validation_error(:metadata, "a list of atoms", value)
  end

  defp validate_option(:bindings, value) when is_list(value) do
    if Enum.all?(value, &is_tuple/1),
      do: {:ok, value},
      else: validation_error(:bindings, "a list of bindings (keyword lists)", value)
  end

  defp validate_option(_, value), do: {:ok, value}

  defp validation_error(option, expected, value) do
    {:error, "expected #{inspect(option)} to be #{expected}, got: #{inspect(value)}"}
  end

  defp validate_conn_opts(opts) do
    case Keyword.get(opts, :connection, []) do
      uri when is_binary(uri) ->
        case uri |> to_charlist() |> :amqp_uri.parse() do
          {:ok, _amqp_params} -> {:ok, uri}
          {:error, reason} -> {:error, "Failed parsing AMQP URI: #{inspect(reason)}"}
        end

      opts when is_list(opts) ->
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

        validate_supported_opts(opts, _group = :connection, supported)
    end
  end

  defp validate_declare_opts(opts, queue) do
    case Keyword.fetch(opts, :declare) do
      :error when queue == "" ->
        {:error, "can't use \"\" (server autogenerate) as the queue name without the :declare"}

      :error ->
        {:ok, nil}

      {:ok, declare_opts} ->
        supported = [:durable, :auto_delete, :exclusive, :passive, :no_wait, :arguments]
        validate_supported_opts(declare_opts, :declare, supported)
    end
  end

  defp validate_bindings(opts) do
    with {:ok, bindings} <- validate(opts, :bindings, _default = []) do
      Enum.reduce_while(bindings, {:ok, bindings}, fn
        {exchange, binding_opts}, acc when is_binary(exchange) ->
          supported = [:routing_key, :no_wait, :arguments]

          case validate_supported_opts(binding_opts, :bindings, supported) do
            {:ok, _bindings_opts} -> {:cont, acc}
            {:error, reason} -> {:halt, {:error, reason}}
          end

        {other, _opts}, _acc ->
          {:error, "the exchange in a binding should be a string, got: #{inspect(other)}"}
      end)
    end
  end

  defp validate_qos_opts(opts) do
    group = :qos
    qos_opts = opts[group] || []
    supported = [:prefetch_size, :prefetch_count]

    qos_opts
    |> Keyword.put_new(:prefetch_count, @default_prefetch_count)
    |> validate_supported_opts(group, supported)
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
