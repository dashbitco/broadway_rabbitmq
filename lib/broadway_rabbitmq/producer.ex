defmodule BroadwayRabbitMQ.Producer do
  @valid_ack_values [:ack, :reject, :reject_and_requeue, :reject_and_requeue_once]

  @opts_schema [
    # Internal.
    client: [type: :atom, doc: false],
    # Handled by Broadway.
    broadway: [type: :any, doc: false],
    buffer_size: [
      type: :non_neg_integer,
      doc: """
      Optional, but required if `:prefetch_count` under `:qos` is
      set to `0`. Defines the size of the buffer to store events without demand.
      Can be `:infinity` to signal no limit on the buffer size. This is used to
      configure the GenStage producer, see the `GenStage` docs for more details.
      Defaults to `:prefetch_count * 5`.
      """
    ],
    buffer_keep: [
      type: {:in, [:first, :last]},
      doc: """
      Optional. Used in the GenStage producer configuration.
      Defines whether the `:first` or `:last` entries should be kept on the
      buffer in case the buffer size is exceeded. Defaults to `:last`.
      """
    ],
    on_success: [
      type: {:in, @valid_ack_values},
      doc: """
      Configures the acking behaviour for successful messages.
      See the "Acking" section below for all the possible values.
      This option can also be changed for each message through
      `Broadway.Message.configure_ack/2`.
      """,
      default: :ack
    ],
    on_failure: [
      type: {:in, @valid_ack_values},
      doc: """
      Configures the acking behaviour for failed messages.
      See the "Acking" section below for all the possible values.
      This option can also be changed for each message through
      `Broadway.Message.configure_ack/2`.
      """,
      default: :reject_and_requeue
    ],
    backoff_min: [
      type: :non_neg_integer,
      doc: """
      The minimum backoff interval (default: `1_000`).
      """
    ],
    backoff_max: [
      type: :non_neg_integer,
      doc: """
      The maximum backoff interval (default: `30_000`).
      """
    ],
    backoff_type: [
      type: {:in, [:rand_exp, :exp, :rand, :stop]},
      default: :rand_exp,
      doc: """
      The backoff strategy: `:stop` for no backoff and
      to stop, `:exp` for exponential, `:rand` for random, and `:rand_exp` for
      random exponential (default: `:rand_exp`).
      """
    ]
  ]

  @moduledoc """
  A RabbitMQ producer for Broadway.

  ## Features

    * Automatically acknowledges/rejects messages.
    * Handles connection outages using backoff for retries.

  For a quick getting started on using Broadway with RabbitMQ, please see
  the [RabbitMQ Guide](https://hexdocs.pm/broadway/rabbitmq.html).

  ## Options

  #{NimbleOptions.docs(@opts_schema)}
  #{NimbleOptions.docs(BroadwayRabbitMQ.AmqpClient.__opts_schema__())}

  ## Example

      Broadway.start_link(MyBroadway,
        name: MyBroadway,
        producer: [
          module:
            {BroadwayRabbitMQ.Producer,
            queue: "my_queue",
            connection: [
              username: "user",
              password: "password",
              host: "192.168.0.10"
            ],
            qos: [
              prefetch_count: 50
            ]},
          concurrency: 5
        ],
        processors: [
          default: []
        ]
      )

  ## Back-pressure and `:prefetch_count`

  Unlike the RabbitMQ client that has a default `:prefetch_count` = 0,
  which disables back-pressure, BroadwayRabbitMQ overwrite the default
  value to `50` enabling the back-pressure mechanism. You can still define
  it as `0`, however, if you do this, make sure the machine has enough
  resources to handle the number of messages coming from the broker, and set
  `:buffer_size` to an appropriate value.

  This is important because the BroadwayRabbitMQ producer does not work
  as a poller like BroadwaySQS. Instead, it maintains an active connection
  with a subscribed consumer that receives messages continuously as they
  arrive in the queue. This is more efficient than using the `basic.get`
  method, however, it removes the ability of the GenStage producer to control
  the demand. Therefore we need to use the `:prefetch_count` option to
  impose back-pressure at the channel level.

  ## Connection loss and backoff

  In case the connection cannot be opened or if a stablished connection is lost,
  the producer will try to reconnect using an exponential random backoff strategy.
  The strategy can be configured using the `:backoff_type` option.

  ## Unsupported options

  Currently, Broadway does not accept options for `Basic.consume/4` which
  is called internally by the producer with default values. That means options
  like `:no_ack` are not supported. If you have a scenario where you need to
  customize those options, please open an issue, so we can consider adding this
  feature.

  ## Declaring queues and binding them to exchanges

  In RabbitMQ, it's common for consumers to declare the queue they're going
  to consume from and bind it to the appropriate exchange when they start up.
  You can do these steps (either or both) when setting up your Broadway pipeline
  through the `:declare` and `:bindings` options.

      Broadway.start_link(MyBroadway,
        name: MyBroadway,
        producer: [
          module:
            {BroadwayRabbitMQ.Producer,
            queue: "my_queue",
            declare: [],
            bindings: [{"my-exchange", []}]},
          concurrency: 5
        ],
        processors: [
          default: []
        ]
      )

  ## Acking

  You can use the `:on_success` and `:on_failure` options to control how messages
  are acked on RabbitMQ. By default, successful messages are acked and failed
  messages are rejected. You can set `:on_success` and `:on_failure` when starting
  the RabbitMQ producer, or change them for each message through
  `Broadway.Message.configure_ack/2`. You can also ack a message *before* the end of the Broadway
  pipeline by using `Broadway.Message.ack_immediately/1`, which determines whether to ack or
  reject based on `:on_success`/`:on_failure` too.

  Here is the list of all possible values supported by `:on_success` and `:on_failure`:

    * `:ack` - acknowledge the message. RabbitMQ will mark the message as acked and
      will not redeliver it to any other consumer. This is done via `AMQP.Basic.ack/3`.

    * `:reject` - rejects the message without requeuing (basically, discards
       the message).  RabbitMQ will not redeliver the message to any other
       consumer, but a queue can be configured to send rejected messages to a
       [dead letter exchange](https://www.rabbitmq.com/dlx.html), where another
       consumer can see why it was dead lettered, how many times, and so on, and
       potentially republish it. Rejecting is done through `AMQP.Basic.reject/3`
       with the `:requeue` option set to `false`.

    * `:reject_and_requeue` - rejects the message and tells RabbitMQ to requeue it so
      that it can be delivered to a consumer again. `:reject_and_requeue`
      always requeues the message. If the message is unprocessable, this will
      cause an infinite loop of retries. Rejecting is done through `AMQP.Basic.reject/3`
       with the `:requeue` option set to `true`.

    * `:reject_and_requeue_once` - rejects the message and tells RabbitMQ to requeue it
      the first time. If a message was already requeued and redelivered, it will be
      rejected and not requeued again. This feature uses Broadway-specific message metadata,
      not RabbitMQ's dead lettering feature. Rejecting is done through `AMQP.Basic.reject/3`.

  ### Choosing the right requeue strategy

  Choose the requeue strategy carefully.

  If you set the value to `:reject` or `:reject_and_requeue_once`, make sure you handle failed
  messages properly, either by logging them somewhere or redirecting them to a dead-letter queue
  for future inspection. These strategies are useful when you want to implement **at most once**
  processing: you want your messages to be processed at most once, but if they fail, you prefer
  that they're not re-processed. It's common to pair this requeue strategy with the use of
  `Broadway.Message.ack_immediately/1` in order to ack the message before doing any work,
  so that if the consumer loses connection to RabbitMQ while processing, the message will have
  been acked and RabbitMQ will not deliver it to another consumer. For example:

      def handle_message(_, message, _context) do
        Broadway.Message.ack_immediately(message)
        process_message(message)
        message
      end

  `:reject_and_requeue` is commonly used when you are implementing **at least once** processing
  semantics. You want messages to be processed at least once, so if something goes wrong and they
  get rejected, they'll be requeued and redelivered to a consumer.
  When using `:reject_and_requeue`, pay attention that requeued messages by default will
  be instantly redelivered, which may result in very high unnecessary workload.
  One way to handle this is by using [Dead Letter Exchanges](https://www.rabbitmq.com/dlx.html)
  and [TTL and Expiration](https://www.rabbitmq.com/ttl.html).

  ## Metadata

  You can retrieve additional information about your message by setting the `:metadata` option
  when starting the producer. This is useful in a handful of situations like when you are
  interested in the message headers or in knowing if the message is new or redelivered.
  Metadata is added to the `metadata` field in the `Broadway.Message` struct.

  These are the keys in the metadata map that are *always present*:

    * `:amqp_channel` - It contains the `AMQP.Channel` struct. You can use it to do things
      like publish messages back to RabbitMQ (for use cases such as RPCs). You *should not*
      do things with the channel other than publish messages with `AMQP.Basic.publish/5`. Other
      operations may result in undesired effects.

  Here is the list of all possible values supported by `:metadata`:

    * `:delivery_tag` - an integer that uniquely identifies the delivery on a channel.
      It's used internally in AMQP client library methods, like acknowledging or rejecting a message.

    * `:redelivered` - a boolean representing if the message was already rejected and requeued before.

    * `:exchange` - the name of the exchange the queue was bound to.

    * `:routing_key` - the name of the queue from which the message was consumed.

    * `:message_count` - the current number of messages in the queue.

    * `:content_type` - the MIME type of the message.

    * `:content_encoding` - the MIME content encoding of the message.

    * `:headers` - the headers of the message, which are returned in tuples of type
      `{String.t(), argument_type(), term()}`. The last value of the tuple is the value of
      the header. You can find a list of argument types
      [here](https://hexdocs.pm/amqp/readme.html#types-of-arguments-and-headers).

    * `:persistent` - a boolean stating whether or not the message was published with disk persistence.

    * `:priority` - an integer representing the message priority on the queue.

    * `:correlation_id` - it's a useful property of AMQP protocol to correlate RPC requests.
      You can read more about RPC in RabbitMQ
      [here](https://www.rabbitmq.com/tutorials/tutorial-six-python.html).

    * `:message_id` - application specific message identifier.

    * `:timestamp` - a timestamp associated with the message.

    * `:type` - message type as a string.

    * `:user_id` - a user identifier that could have been assigned during message publication.
      RabbitMQ validated this value against the active connection when the message was published.

    * `:app_id` - publishing application identifier.

    * `:cluster_id` - RabbitMQ cluster identifier.

    * `:reply_to` - name of the reply queue.

  """

  use GenStage

  require Logger

  alias Broadway.{Message, Acknowledger, Producer}
  alias BroadwayRabbitMQ.Backoff

  @behaviour Acknowledger
  @behaviour Producer

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    maybe_warn_unspecified_on_failure_opt(opts)
    {opts, client_opts} = Keyword.split(opts, Keyword.keys(@opts_schema) -- [:broadway])

    opts =
      case NimbleOptions.validate(opts, @opts_schema) do
        {:ok, opts} -> opts
        {:error, reason} -> raise ArgumentError, Exception.message(reason)
      end

    client = Keyword.get(opts, :client, BroadwayRabbitMQ.AmqpClient)
    gen_stage_opts = Keyword.take(opts, [:buffer_size, :buffer_keep])
    on_success = Keyword.fetch!(opts, :on_success)
    on_failure = Keyword.fetch!(opts, :on_failure)
    backoff_opts = Keyword.take(opts, [:backoff_min, :backoff_max, :backoff_type])

    config = init_client!(client, client_opts)

    send(self(), {:connect, :no_init_client})

    prefetch_count = config[:qos][:prefetch_count]
    options = producer_options(gen_stage_opts, prefetch_count)

    {:producer,
     %{
       client: client,
       channel: nil,
       consumer_tag: nil,
       config: config,
       backoff: Backoff.new(backoff_opts),
       channel_ref: nil,
       opts: client_opts,
       on_success: on_success,
       on_failure: on_failure
     }, options}
  end

  @impl true
  def handle_demand(_incoming_demand, state) do
    {:noreply, [], state}
  end

  @impl true
  def handle_info({:basic_consume_ok, %{consumer_tag: tag}}, state) do
    {:noreply, [], %{state | consumer_tag: tag}}
  end

  # RabbitMQ sends this in a few scenarios, like if the queue this consumer
  # is consuming from gets deleted. See https://www.rabbitmq.com/consumer-cancel.html.
  def handle_info({:basic_cancel, %{consumer_tag: tag}}, %{consumer_tag: tag} = state) do
    Logger.warn("Received AMQP basic_cancel from RabbitMQ")
    state = disconnect(state)
    {:noreply, [], connect(state, :init_client)}
  end

  def handle_info({:basic_cancel_ok, %{consumer_tag: tag}}, %{consumer_tag: tag} = state) do
    {:noreply, [], %{state | consumer_tag: nil}}
  end

  def handle_info({:basic_deliver, payload, meta}, state) do
    %{channel: channel, client: client, config: config} = state
    %{delivery_tag: tag, redelivered: redelivered} = meta

    ack_data = %{
      delivery_tag: tag,
      client: client,
      redelivered: redelivered,
      on_success: state.on_success,
      on_failure: state.on_failure
    }

    metadata =
      meta
      |> Map.take(config[:metadata])
      |> Map.put(:amqp_channel, channel)

    message = %Message{
      data: payload,
      metadata: metadata,
      acknowledger: {__MODULE__, _ack_ref = channel, ack_data}
    }

    {:noreply, [message], state}
  end

  def handle_info({:EXIT, conn_pid, reason}, %{channel: %{conn: %{pid: conn_pid}}} = state) do
    Logger.warn("AMQP connection went down with reason: #{inspect(reason)}")
    {:noreply, [], connect(state, :init_client)}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{channel_ref: ref} = state) do
    Logger.warn("AMQP channel went down with reason: #{inspect(reason)}")
    state = disconnect(state)
    {:noreply, [], connect(state, :init_client)}
  end

  def handle_info({:connect, mode}, state) when mode in [:init_client, :no_init_client] do
    {:noreply, [], connect(state, mode)}
  end

  def handle_info(_, state) do
    {:noreply, [], state}
  end

  @impl true
  def terminate(_reason, state) do
    _state = disconnect(state)
    :ok
  end

  @impl Acknowledger
  def ack(_ack_ref = channel, successful, failed) do
    ack_messages(successful, channel, :successful)
    ack_messages(failed, channel, :failed)
  end

  @impl Acknowledger
  def configure(_channel, ack_data, options) do
    Enum.each(options, fn
      {name, val} when name in [:on_success, :on_failure] -> assert_valid_ack_option!(name, val)
      {other, _value} -> raise ArgumentError, "unsupported configure option #{inspect(other)}"
    end)

    ack_data = Map.merge(ack_data, Map.new(options))
    {:ok, ack_data}
  end

  defp assert_valid_ack_option!(name, value) do
    unless value in @valid_ack_values do
      raise ArgumentError, "unsupported value for #{inspect(name)} option: #{inspect(value)}"
    end
  end

  @impl Producer
  def prepare_for_draining(%{channel: nil} = state) do
    {:noreply, [], state}
  end

  def prepare_for_draining(state) do
    %{client: client, channel: channel, consumer_tag: consumer_tag} = state

    case client.cancel(channel, consumer_tag) do
      {:ok, ^consumer_tag} ->
        {:noreply, [], state}

      {:error, error} ->
        Logger.error("Could not cancel producer while draining. Channel is #{error}")
        {:noreply, [], state}
    end
  end

  defp producer_options(opts, 0) do
    if opts[:buffer_size] do
      opts
    else
      raise ArgumentError, ":prefetch_count is 0, specify :buffer_size explicitly"
    end
  end

  defp producer_options(opts, prefetch_count) do
    Keyword.put_new(opts, :buffer_size, prefetch_count * 5)
  end

  defp ack_messages(messages, channel, kind) do
    errors =
      Enum.flat_map(messages, fn %{acknowledger: {_module, _channel, ack_data}} = msg ->
        case apply_ack_func(kind, ack_data, channel) do
          :ok ->
            []

          {:error, reason} ->
            Logger.error("""
            Could not ack or reject message.

            Message: #{inspect(msg)}
            Reason: #{inspect(reason)}
            """)

            [{msg, reason}]
        end
      end)

    case errors do
      [] ->
        :ok

      [{msg, reason} | _other_errors] ->
        raise RuntimeError, """
        Could not ack or reject one or more messages. An example failure is provided. There may \
        be more in logging.

        Message: #{inspect(msg)}
        Reason: #{inspect(reason)}
        """
    end
  end

  defp apply_ack_func(:successful, ack_data, channel) do
    apply_ack_func(ack_data.on_success, ack_data, channel)
  end

  defp apply_ack_func(:failed, ack_data, channel) do
    apply_ack_func(ack_data.on_failure, ack_data, channel)
  end

  defp apply_ack_func(:ack, ack_data, channel) do
    ack_data.client.ack(channel, ack_data.delivery_tag)
  end

  defp apply_ack_func(reject, ack_data, channel)
       when reject in [:reject, :reject_and_requeue, :reject_and_requeue_once] do
    options = [requeue: requeue?(reject, ack_data.redelivered)]
    ack_data.client.reject(channel, ack_data.delivery_tag, options)
  end

  defp requeue?(:reject, _redelivered), do: false
  defp requeue?(:reject_and_requeue, _redelivered), do: true
  defp requeue?(:reject_and_requeue_once, redelivered), do: !redelivered

  defp disconnect(%{channel: channel, client: client} = state) do
    if channel do
      _ = client.close_connection(channel.conn)
      %{state | channel: nil}
    else
      state
    end
  end

  defp connect(state, mode) when mode in [:init_client, :no_init_client] do
    %{client: client, config: config, backoff: backoff, opts: opts} = state

    config =
      if mode == :no_init_client do
        config
      else
        init_client!(client, opts)
      end

    case client.setup_channel(config) do
      {:ok, channel} ->
        # We monitor the channel but link to the connection (in the client, not here).
        channel_ref = Process.monitor(channel.pid)
        backoff = backoff && Backoff.reset(backoff)
        consumer_tag = client.consume(channel, config)

        %{
          state
          | channel: channel,
            config: config,
            consumer_tag: consumer_tag,
            backoff: backoff,
            channel_ref: channel_ref
        }

      {:error, reason} ->
        handle_connection_failure(state, reason)
    end
  end

  defp handle_connection_failure(state, reason) do
    _ = Logger.error("Cannot connect to RabbitMQ broker: #{inspect(reason)}")

    case reason do
      {:auth_failure, 'Disconnected'} ->
        handle_backoff(state)

      {:socket_closed_unexpectedly, :"connection.start"} ->
        handle_backoff(state)

      reason when reason in [:econnrefused, :unknown_host, :not_allowed] ->
        handle_backoff(state)

      _other ->
        _ = Logger.error("Crashing because of unexpected error when connecting to RabbitMQ")
        raise "unexpected error when connecting to RabbitMQ broker"
    end
  end

  defp handle_backoff(%{backoff: backoff} = state) do
    new_backoff =
      if backoff do
        {timeout, backoff} = Backoff.backoff(backoff)
        Process.send_after(self(), {:connect, :init_client}, timeout)
        backoff
      end

    %{
      state
      | channel: nil,
        consumer_tag: nil,
        backoff: new_backoff,
        channel_ref: nil
    }
  end

  defp init_client!(client, opts) do
    case client.init(opts) do
      {:ok, config} ->
        config

      {:error, message} ->
        raise ArgumentError, "invalid options given to #{inspect(client)}.init/1, " <> message
    end
  end

  # TODO: Remove when we remove the default value
  defp maybe_warn_unspecified_on_failure_opt(opts) do
    unless Keyword.has_key?(opts, :on_failure) do
      name = get_in(opts, [:broadway, :name])

      {:ok, vsn} = :application.get_key(:broadway_rabbitmq, :vsn)

      IO.warn(
        ":on_failure should be specified for Broadway topology with name #{inspect(name)}; " <>
          "assuming :reject_and_requeue. See documentation for valid values: " <>
          "https://hexdocs.pm/broadway_rabbitmq/#{vsn}/BroadwayRabbitMQ.Producer.html#module-acking"
      )
    end
  end
end
