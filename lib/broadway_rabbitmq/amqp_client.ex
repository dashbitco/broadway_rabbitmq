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

  @connection_opts_schema [
    username: [type: :any],
    password: [type: :any],
    virtual_host: [type: :any],
    host: [type: :any],
    port: [type: :any],
    channel_max: [type: :any],
    frame_max: [type: :any],
    heartbeat: [type: :any],
    connection_timeout: [type: :any],
    ssl_options: [type: :any],
    client_properties: [type: :any],
    socket_options: [type: :any],
    auth_mechanisms: [type: :any]
  ]

  @binding_opts_schema [
    routing_key: [type: :any],
    no_wait: [type: :any],
    arguments: [type: :any]
  ]

  @opts_schema [
    queue: [type: :string, required: true],
    connection: [type: {:custom, __MODULE__, :__validate_connection_options__, []}, default: []],
    name: [type: {:custom, __MODULE__, :__validate_connection_name__, []}, default: :undefined],
    qos: [
      type: :keyword_list,
      keys: [
        prefetch_size: [type: :non_neg_integer],
        prefetch_count: [type: :non_neg_integer, default: 50]
      ],
      default: []
    ],
    backoff_min: [type: :non_neg_integer],
    backoff_max: [type: :non_neg_integer],
    backoff_type: [type: {:one_of, [:exp, :rand, :rand_exp, :stop]}],
    metadata: [type: {:custom, __MODULE__, :__validate_metadata__, []}, default: []],
    declare: [
      type: :keyword_list,
      keys: [
        durable: [type: :any],
        auto_delete: [type: :any],
        exclusive: [type: :any],
        passive: [type: :any],
        no_wait: [type: :any],
        arguments: [type: :any]
      ]
    ],
    bindings: [type: {:custom, __MODULE__, :__validate_bindings__, []}, default: []],
    broadway: [type: :any],
    merge_options: [type: {:fun, 1}],
    after_connect: [type: {:fun, 1}]
  ]

  @impl true
  def init(opts) do
    with {:ok, opts} <- validate_merge_opts(opts),
         {:ok, opts} <- NimbleOptions.validate(opts, @opts_schema),
         :ok <- validate_declare_opts(opts[:declare], opts[:queue]) do
      {:ok,
       %{
         connection: Keyword.fetch!(opts, :connection),
         queue: Keyword.fetch!(opts, :queue),
         name: Keyword.fetch!(opts, :name),
         declare_opts: Keyword.get(opts, :declare, nil),
         bindings: Keyword.fetch!(opts, :bindings),
         qos: Keyword.fetch!(opts, :qos),
         metadata: Keyword.fetch!(opts, :metadata),
         after_connect: Keyword.get(opts, :after_connect, fn _channel -> :ok end)
       }}
    else
      {:error, %NimbleOptions.ValidationError{} = error} -> {:error, Exception.message(error)}
      {:error, message} when is_binary(message) -> {:error, message}
    end
  end

  @impl true
  def setup_channel(config) do
    with {name, config} <- Map.pop(config, :name, :undefined),
         {:ok, conn} <- Connection.open(config.connection, name),
         {:ok, channel} <- Channel.open(conn),
         :ok <- call_after_connect(config, channel),
         :ok <- Basic.qos(channel, config.qos),
         {:ok, queue} <- maybe_declare_queue(channel, config.queue, config.declare_opts),
         :ok <- maybe_bind_queue(channel, queue, config.bindings) do
      {:ok, channel}
    end
  end

  defp call_after_connect(config, channel) do
    case config.after_connect.(channel) do
      :ok ->
        :ok

      {:error, reason} ->
        {:error, reason}

      other ->
        raise "unexpected return value from the :after_connect function: #{inspect(other)}"
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
          {:ok, Keyword.merge(opts, merge_opts)}
        else
          message =
            "The :merge_options function should return a keyword list, " <>
              "got: #{inspect(merge_opts)}"

          {:error, message}
        end

      {:ok, other} ->
        {:error, ":merge_options must be a function with arity 1, got: #{inspect(other)}"}

      :error ->
        {:ok, opts}
    end
  end

  def __validate_metadata__(value) do
    if is_list(value) and Enum.all?(value, &is_atom/1) do
      {:ok, value}
    else
      {:error, "expected :metadata to be a list of atoms, got: #{inspect(value)}"}
    end
  end

  def __validate_connection_name__(value) do
    if is_binary(value) or value == :undefined do
      {:ok, value}
    else
      {:error, "expected :name to be a string or the atom :undefined, got: #{inspect(value)}"}
    end
  end

  def __validate_connection_options__(uri) when is_binary(uri) do
    case uri |> to_charlist() |> :amqp_uri.parse() do
      {:ok, _amqp_params} -> {:ok, uri}
      {:error, reason} -> {:error, "Failed parsing AMQP URI: #{inspect(reason)}"}
    end
  end

  def __validate_connection_options__(opts) when is_list(opts) do
    with {:error, %NimbleOptions.ValidationError{} = error} <-
           NimbleOptions.validate(opts, @connection_opts_schema),
         do: {:error, Exception.message(error) <> " (in option :connection)"}
  end

  def __validate_connection_options__(other) do
    {:error, "expected :connection to be a URI or a keyword list, got: #{inspect(other)}"}
  end

  defp validate_declare_opts(declare_opts, queue) do
    if queue == "" and is_nil(declare_opts) do
      {:error, "can't use \"\" (server autogenerate) as the queue name without the :declare"}
    else
      :ok
    end
  end

  def __validate_bindings__(value) when is_list(value) do
    Enum.each(value, fn
      {exchange, binding_opts} when is_binary(exchange) ->
        case NimbleOptions.validate(binding_opts, @binding_opts_schema) do
          {:ok, _bindings_opts} ->
            :ok

          {:error, %NimbleOptions.ValidationError{} = reason} ->
            throw({:error, Exception.message(reason)})
        end

      {other, _opts} ->
        throw({:error, "the exchange in a binding should be a string, got: #{inspect(other)}"})

      other ->
        message =
          "expected :bindings to be a list of bindings ({exchange, bind_options} tuples), " <>
            "got: #{inspect(other)}"

        throw({:error, message})
    end)

    {:ok, value}
  catch
    :throw, {:error, message} -> {:error, message}
  end

  def __validate_bindings__(other) do
    {:error, "expected bindings to be a list of {exchange, opts} tuples, got: #{inspect(other)}"}
  end
end
