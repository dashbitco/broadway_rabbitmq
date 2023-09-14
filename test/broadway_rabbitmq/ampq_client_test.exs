defmodule BroadwayRabbitMQ.AmqpClientTest do
  use ExUnit.Case, async: true

  alias BroadwayRabbitMQ.AmqpClient

  test "default options" do
    assert {:ok,
            %{
              connection: [],
              qos: [prefetch_count: 50],
              metadata: [],
              bindings: [],
              declare_opts: nil,
              queue: "queue",
              after_connect: after_connect
            }} = AmqpClient.init(queue: "queue")

    assert after_connect.(:channel) == :ok
  end

  test "connection name" do
    assert {:ok, %{name: "conn_name"}} = AmqpClient.init(queue: "queue", name: "conn_name")
  end

  describe "validate init options" do
    test "supported options" do
      after_connect = fn _ -> :ok end

      connection = [
        username: nil,
        password: nil,
        virtual_host: nil,
        host: nil,
        port: nil,
        channel_max: nil,
        frame_max: nil,
        heartbeat: nil,
        connection_timeout: nil,
        ssl_options: nil,
        client_properties: nil,
        socket_options: nil
      ]

      qos = [
        prefetch_size: 1,
        prefetch_count: 1
      ]

      options = [
        queue: "queue",
        connection: connection,
        qos: qos,
        after_connect: after_connect,
        consume_options: [no_ack: true, exclusive: false]
      ]

      metadata = []

      assert AmqpClient.init(options) ==
               {:ok,
                %{
                  connection: connection,
                  name: :undefined,
                  qos: qos,
                  metadata: metadata,
                  bindings: [],
                  declare_opts: nil,
                  queue: "queue",
                  after_connect: after_connect,
                  consume_options: [no_ack: true, exclusive: false]
                }}
    end

    test "providing connection via a URI" do
      connection = "amqp://guest:guest@127.0.0.1"

      assert {:ok, %{connection: ^connection, queue: "queue"}} =
               AmqpClient.init(queue: "queue", connection: connection)
    end

    test "invalid URI" do
      assert {:error, message} = AmqpClient.init(queue: "queue", connection: "http://example.com")
      assert message =~ "failed parsing AMQP URI"
    end

    test "custom pool module which implements BroadwayRabbitMQ.ChannelPool behaviour" do
      defmodule ValidPool do
        @behaviour BroadwayRabbitMQ.ChannelPool

        @impl true
        def checkout_channel(_args), do: {:ok, %AMQP.Channel{}}

        @impl true
        def checkin_channel(_args, _channel), do: :ok
      end

      custom_pool = {:custom_pool, ValidPool, []}

      assert {:ok, %{connection: ^custom_pool}} =
               AmqpClient.init(queue: "queue", connection: custom_pool)
    after
      :code.delete(ValidPool)
      :code.purge(ValidPool)
    end

    test "custom pool module which doesn't implement BroadwayRabbitMQ.ChannelPool behaviour" do
      custom_pool = {:custom_pool, URI, []}
      assert {:error, message} = AmqpClient.init(queue: "queue", connection: custom_pool)
      assert message =~ "implements BroadwayRabbitMQ.ChannelPool"
    end

    test "unsupported options for Broadway" do
      assert {:error, message} = AmqpClient.init(queue: "queue", option_1: 1, option_2: 2)
      assert message =~ "unknown options [:option_1, :option_2], valid options are"
    end

    test "unsupported options for :connection" do
      assert {:error, message} =
               AmqpClient.init(queue: "queue", connection: [option_1: 1, option_2: 2])

      assert message =~ "unknown options [:option_1, :option_2], valid options are"
      assert message =~ "in options [:connection]"
    end

    test "unsupported options for :qos" do
      assert {:error, message} = AmqpClient.init(queue: "queue", qos: [option_1: 1, option_2: 2])
      assert message =~ "unknown options [:option_1, :option_2]"
      assert message =~ "in options [:qos]"
    end

    test "unsupported options for :declare" do
      assert {:error, message} =
               AmqpClient.init(queue: "queue", declare: [option_1: 1, option_2: 2])

      assert message =~ "unknown options [:option_1, :option_2]"
      assert message =~ "in options [:declare]"
    end

    test ":queue is required" do
      assert AmqpClient.init([]) ==
               {:error, "required option :queue not found, received options: []"}

      assert AmqpClient.init(queue: nil) ==
               {:error, "expected :queue to be a string, got: nil"}
    end

    test ":queue should be a string" do
      assert AmqpClient.init(queue: :an_atom) ==
               {:error, "expected :queue to be a string, got: :an_atom"}

      {:ok, config} = AmqpClient.init(queue: "my_queue")
      assert config.queue == "my_queue"
    end

    test ":queue shouldn't be an empty string if :declare is not present" do
      assert AmqpClient.init(queue: "") ==
               {:error,
                "can't use \"\" (server autogenerate) as the queue name without the :declare"}

      assert {:ok, config} = AmqpClient.init(queue: "", declare: [])
      assert config.queue == ""
    end

    test ":metadata should be a list of atoms" do
      {:ok, opts} = AmqpClient.init(queue: "queue", metadata: [:routing_key, :headers])
      assert opts[:metadata] == [:routing_key, :headers]

      message =
        ~s(list element at position 0 in :metadata failed validation: expected "list element" ) <>
          ~s(to be an atom, got: "routing_key")

      assert AmqpClient.init(queue: "queue", metadata: ["routing_key", :headers]) ==
               {:error, message}
    end

    test ":bindings should be a list of tuples" do
      {:ok, opts} = AmqpClient.init(queue: "queue", bindings: [{"my-exchange", [arguments: []]}])
      assert opts[:bindings] == [{"my-exchange", [arguments: []]}]

      message =
        ~s(list element at position 0 in :bindings failed validation: expected binding to be ) <>
          ~s(a {exchange, opts} tuple, got: :something)

      assert AmqpClient.init(queue: "queue", bindings: [:something, :else]) ==
               {:error, message}
    end

    test ":bindings with invalid binding options" do
      message =
        "list element at position 0 in :bindings failed validation: unknown options " <>
          "[:invalid], valid options are: [:routing_key, :arguments]"

      assert AmqpClient.init(queue: "queue", bindings: [{"my-exchange", [invalid: true]}]) ==
               {:error, message}
    end

    test ":merge_options options should be merged with normal opts" do
      merge_options_fun = fn index -> [queue: "queue#{index}"] end

      assert {:ok, %{queue: "queue4"}} =
               AmqpClient.init(
                 queue: "queue",
                 broadway: [index: 4],
                 merge_options: merge_options_fun
               )
    end

    test ":merge_options doesn't perform deep merging" do
      merge_options_fun = fn index ->
        [connection: [username: "user#{index}"]]
      end

      assert {:ok, %{connection: connection}} =
               AmqpClient.init(
                 queue: "queue",
                 broadway: [index: 4],
                 connection: [host: "example.com"],
                 merge_options: merge_options_fun
               )

      assert connection == [username: "user4"]
    end

    test ":merge_options should be a 1-arity function" do
      assert AmqpClient.init(queue: "queue", merge_options: :wat) ==
               {:error, ":merge_options must be a function with arity 1, got: :wat"}
    end

    test ":merge_options should return a keyword list" do
      assert AmqpClient.init(
               queue: "queue",
               broadway: [index: 4],
               merge_options: fn _index -> :ok end
             ) ==
               {:error, "The :merge_options function should return a keyword list, got: :ok"}
    end

    test "options returned by :merge_options are still validated" do
      merge_options_fun = fn _index -> [option: 1] end

      assert {:error, message} =
               AmqpClient.init(
                 queue: "queue",
                 merge_options: merge_options_fun,
                 broadway: [index: 4]
               )

      assert message =~ "unknown options [:option], valid options are"
    end

    test ":merge_options raises if there's no Broadway index in the options" do
      merge_options_fun = fn _index -> [option: 1] end

      assert_raise RuntimeError, "missing broadway index", fn ->
        AmqpClient.init(
          queue: "queue",
          merge_options: merge_options_fun,
          broadway: []
        )
      end

      assert_raise RuntimeError, "missing broadway index", fn ->
        AmqpClient.init(
          queue: "queue",
          merge_options: merge_options_fun
        )
      end
    end
  end

  test "ack/2 when the channel is down" do
    {pid, ref} = spawn_monitor(fn -> :ok end)
    assert_receive {:DOWN, ^ref, _, _, _}

    assert {:error, :noproc} = AmqpClient.ack(%AMQP.Channel{pid: pid}, "delivery-tag-1234")
  end

  test "reject/2 when the channel is down" do
    {pid, ref} = spawn_monitor(fn -> :ok end)
    assert_receive {:DOWN, ^ref, _, _, _}

    assert {:error, :noproc} =
             AmqpClient.reject(%AMQP.Channel{pid: pid}, "delivery-tag-1234", requeue: false)
  end

  describe "setup_channel/1" do
    @describetag :integration

    test "returns a real channel" do
      {:ok, config} = AmqpClient.init(queue: "queue", declare: [auto_delete: true])
      assert {:ok, %AMQP.Channel{} = channel} = AmqpClient.setup_channel(config)

      # Make sure that the channel is real by issuing a real AMQP operation.
      assert {:ok, %{queue: "queue"}} = AMQP.Queue.status(channel, "queue")

      AMQP.Channel.close(channel)
      Process.unlink(channel.conn.pid)
      AMQP.Connection.close(channel.conn)
    end

    test "uses an existing queue if :declare is not specified" do
      {:ok, control_channel} = open_channel()
      {:ok, %{queue: queue}} = AMQP.Queue.declare(control_channel, "", auto_delete: true)

      {:ok, config} = AmqpClient.init(queue: queue)
      assert {:ok, %AMQP.Channel{} = channel} = AmqpClient.setup_channel(config)
      assert {:ok, %{queue: ^queue}} = AMQP.Queue.status(channel, queue)

      AMQP.Channel.close(channel)
      Process.unlink(channel.conn.pid)
      AMQP.Connection.close(channel.conn)
    end

    test "can bind the given queue to things" do
      {:ok, control_channel} = open_channel()
      {:ok, %{queue: queue}} = AMQP.Queue.declare(control_channel, "", auto_delete: true)

      {:ok, config} = AmqpClient.init(queue: queue, bindings: [{"amq.direct", []}])
      assert {:ok, %AMQP.Channel{} = channel} = AmqpClient.setup_channel(config)

      # Consume from the queue.
      assert {:ok, consumer_tag} = AMQP.Basic.consume(channel, queue, self())
      assert_receive {:basic_consume_ok, %{consumer_tag: ^consumer_tag}}

      # Check that if we publish to the exchange, we get the message because the binding
      # actually happened.
      :ok = AMQP.Basic.publish(control_channel, "", _routing_key = queue, "hello")
      assert_receive {:basic_deliver, "hello", _meta}, 1000

      AMQP.Channel.close(channel)
      Process.unlink(channel.conn.pid)
      AMQP.Connection.close(channel.conn)
    end

    @tag :capture_log
    test "raises if :after_connect returns a bad value" do
      {:ok, config} =
        AmqpClient.init(
          queue: "",
          declare: [auto_delete: true],
          after_connect: fn _channel -> :bad_return_value end
        )

      message = "unexpected return value from the :after_connect function: :bad_return_value"
      assert_raise RuntimeError, message, fn -> AmqpClient.setup_channel(config) end
    end
  end

  @tag :integration
  test "consume/2 + ack/2 + reject/3 + cancel/2" do
    {:ok, control_channel} = open_channel()
    {:ok, %{queue: queue}} = AMQP.Queue.declare(control_channel, "", auto_delete: true)

    {:ok, config} = AmqpClient.init(queue: queue, bindings: [{"amq.direct", []}])
    assert {:ok, %AMQP.Channel{} = channel} = AmqpClient.setup_channel(config)

    # Consume from the queue.
    consumer_tag = AmqpClient.consume(channel, config)
    assert_receive {:basic_consume_ok, %{consumer_tag: ^consumer_tag}}

    # Publish a message and ack it.
    :ok = AMQP.Basic.publish(control_channel, "", _routing_key = queue, "hello")
    assert_receive {:basic_deliver, "hello", %{delivery_tag: delivery_tag}}, 1000
    assert :ok = AmqpClient.ack(channel, delivery_tag)

    # Publish a message and reject it.
    :ok = AMQP.Basic.publish(control_channel, "", _routing_key = queue, "hello")
    assert_receive {:basic_deliver, "hello", %{delivery_tag: delivery_tag}}, 1000
    assert :ok = AmqpClient.reject(channel, delivery_tag, requeue: false)

    # Cancel.
    assert {:ok, ^consumer_tag} = AmqpClient.cancel(channel, consumer_tag)
    assert_receive {:basic_cancel_ok, %{consumer_tag: ^consumer_tag}}

    AMQP.Channel.close(channel)
    Process.unlink(channel.conn.pid)
    AMQP.Connection.close(channel.conn)
  end

  defp open_channel do
    {:ok, conn} = AMQP.Connection.open("amqp://localhost")
    {:ok, channel} = AMQP.Channel.open(conn)

    on_exit(fn ->
      AMQP.Channel.close(channel)
      AMQP.Connection.close(conn)
    end)

    {:ok, channel}
  end
end
