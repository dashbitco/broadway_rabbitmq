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

  defp validate_supported_opts(uri, :connection, _supported_opts) when is_binary(uri) do
    validate_uri_options(uri)
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

  defp validate_uri_options(uri) do
    uri_query =
      case URI.parse(uri) do
        %URI{query: nil} -> nil
        %URI{query: query} -> query |> URI.decode_query()
      end

    case uri |> to_charlist() |> :amqp_uri.parse() do
      {:ok,
       {:amqp_params_network, _username, _password, _vhost, _location, _port, channel_max,
        frame_max, heartbeat, connection_timeout, tls_options, _functions_for_erlang_module, _,
        _}} ->
        validate_uri_options(
          %{
            channel_max: channel_max,
            frame_max: frame_max,
            heartbeat: heartbeat,
            connection_timeout: connection_timeout,
            tls_options: tls_options
          },
          uri_query,
          uri
        )

      {:error, reason} ->
        {:error, "Failed parsing AMQP URI: #{inspect(reason)}"}
    end
  end

  ###
  #  Quick exit if there are no options to check
  ###

  defp validate_uri_options(_, nil, uri) do
    {:ok, uri}
  end

  ###
  # If you supply tls options over the ampq unsecured (ampq://) protocol :amqp
  # dumps them silently but if you supply it ampqs protocol without tls options
  # :amqp kicks out a useful warning.
  ###

  defp validate_uri_options(
         %{tls_options: :none} = options,
         %{"cacertfile" => _} = uri_query,
         uri
       ) do
    warn_tls_config_over_unsecure("cacertfile")
    remaining_uri_query = Map.delete(uri_query, "cacertfile")
    validate_uri_options(options, remaining_uri_query, uri)
  end

  defp validate_uri_options(
         %{tls_options: :none} = options,
         %{"certfile" => _} = uri_query,
         uri
       ) do
    warn_tls_config_over_unsecure("certfile")
    remaining_uri_query = Map.delete(uri_query, "certfile")
    validate_uri_options(options, remaining_uri_query, uri)
  end

  defp validate_uri_options(
         %{tls_options: :none} = options,
         %{"server_name_indication" => _} = uri_query,
         uri
       ) do
    warn_tls_config_over_unsecure("server_name_indication")
    remaining_uri_query = Map.delete(uri_query, "server_name_indication")
    validate_uri_options(options, remaining_uri_query, uri)
  end

  defp validate_uri_options(
         %{tls_options: :none} = options,
         %{"keyfile" => _} = uri_query,
         uri
       ) do
    warn_tls_config_over_unsecure("keyfile")
    remaining_uri_query = Map.delete(uri_query, "keyfile")
    validate_uri_options(options, remaining_uri_query, uri)
  end

  defp validate_uri_options(
         %{tls_options: :none} = options,
         %{"verify" => _} = uri_query,
         uri
       ) do
    warn_tls_config_over_unsecure("verify")
    remaining_uri_query = Map.delete(uri_query, "verify")
    validate_uri_options(options, remaining_uri_query, uri)
  end

  #######
  # This catches an odd behavoir that will allow you to register more or less
  # any option to verify, e.g. verify_some, but only two will actually do the
  # job.
  ######

  defp validate_uri_options(%{tls_options: tls_options} = options, uri_query, uri)
       when is_list(tls_options) do
    verify = Keyword.get(tls_options, :verify)
    validate_verify_tls_option(verify)
    post_tls_options = Map.delete(options, :tls_options)
    validate_uri_options(post_tls_options, uri_query, uri)
  end

  ######
  # Checks remaining keys and trys to make suggestions for typos
  ######

  defp validate_uri_options(_options, uri_query, uri) do
    # this catches extra & that :amqp ignores
    valid_uri_options_list = [
      "",
      "channel_max",
      "connection_timeout",
      "frame_max",
      "heartbeat",
      "cacertfile",
      "certfile",
      "server_name_indication",
      "keyfile",
      "verify"
    ]

    unknown_uri_params = remove_known_uri_keys(valid_uri_options_list, uri_query) |> Map.to_list()

    test_remaining_options(valid_uri_options_list |> tl, unknown_uri_params, uri)
  end

  defp test_remaining_options(known_uri_params, [unknown_param | rest], uri) do
    rated_list =
      for {key, _value} <- [unknown_param],
          uri_param <- known_uri_params do
        {uri_param, key, String.jaro_distance(uri_param, key)}
      end

    {real_option, you_said, _} =
      rated_list
      |> List.keysort(2)
      |> List.last()

    did_you_mean_warning(you_said, real_option)

    test_remaining_options(known_uri_params, rest, uri)
  end

  defp test_remaining_options(_, [], uri) do
    {:ok, uri}
  end

  defp remove_known_uri_keys([head | tail], uri_query) do
    remove_known_uri_keys(tail, Map.delete(uri_query, head))
  end

  defp remove_known_uri_keys([], uri_query) do
    uri_query
  end

  defp warn_tls_config_over_unsecure(tls_key) do
    Logger.warn(
      "You passed the tls option #{inspect(tls_key)}to an unsecure protocol(ampq://). If you want to use tls then please specify amqps://"
    )
  end

  defp validate_verify_tls_option(:verify_none) do
    :ok
  end

  defp validate_verify_tls_option(:verify_peer) do
    :ok
  end

  defp validate_verify_tls_option(unknown_option) do
    verify_peer_distance = Atom.to_string(unknown_option) |> String.jaro_distance("verify_peer")
    verify_none_distance = Atom.to_string(unknown_option) |> String.jaro_distance("verify_none")

    return_warning_for_verify_tls_option(
      verify_none_distance,
      verify_peer_distance,
      unknown_option
    )
  end

  defp return_warning_for_verify_tls_option(verify_peer, verify_none, unknown_option)
       when verify_peer > verify_none do
    Logger.warn(
      "You set your tls option verify as #{inspect(unknown_option)} did you mean #{:verify_peer}?"
    )
  end

  defp return_warning_for_verify_tls_option(verify_peer, verify_none, unknown_option)
       when verify_peer < verify_none do
    Logger.warn(
      "You set your tls option verify as #{inspect(unknown_option)} did you mean #{:verify_none}?"
    )
  end

  defp did_you_mean_warning(you_said, did_you_mean) do
    Logger.warn(
      "You attempted to pass #{inspect(you_said)} as an option did you mean #{
        inspect(did_you_mean)
      }?"
    )
  end
end
