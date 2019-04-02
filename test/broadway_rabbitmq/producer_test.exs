defmodule BroadwayRabbitmq.ProducerTest do
  use ExUnit.Case

  import ExUnit.CaptureLog
  alias Broadway.Message

  defmodule FakeChannel do
    use GenServer

    def new(test_pid) do
      fake_connection_pid = spawn(fn -> Process.sleep(:infinity) end)
      {:ok, fake_channel_pid} = GenServer.start(__MODULE__, fake_connection_pid)

      %{
        pid: fake_channel_pid,
        conn: %{pid: fake_connection_pid, test_pid: test_pid},
        test_pid: test_pid
      }
    end

    def init(fake_connection_pid) do
      Process.link(fake_connection_pid)
      {:ok, :no_state}
    end

    def handle_call(:fake_basic_ack, _from, state) do
      {:reply, :ok, state}
    end
  end

  defmodule FakeRabbitmqClient do
    @behaviour BroadwayRabbitmq.RabbitmqClient

    @impl true
    def init(opts) do
      {:ok, opts[:queue], opts}
    end

    @impl true
    def setup_channel(_queue, config) do
      test_pid = config[:test_pid]

      status =
        Agent.get_and_update(config[:connection_agent], fn
          [status | rest] ->
            {status, rest}

          _ ->
            {:ok, []}
        end)

      if status == :ok do
        channel = FakeChannel.new(test_pid)
        send(test_pid, {:setup_channel, :ok, channel})
        {:ok, channel}
      else
        send(test_pid, {:setup_channel, :error, nil})
        {:error, :econnrefused}
      end
    end

    @impl true
    def ack(channel, delivery_tag) do
      GenServer.call(channel.pid, :fake_basic_ack)
      send(channel.test_pid, {:ack, delivery_tag})
    end

    @impl true
    def reject(channel, delivery_tag) do
      GenServer.call(channel.pid, :fake_basic_ack)
      send(channel.test_pid, {:reject, delivery_tag})
    end

    @impl true
    def consume(_channel, _queue) do
      :fake_consumer_tag
    end

    @impl true
    def cancel(_channel, :fake_consumer_tag_closing) do
      {:error, :closing}
    end

    @impl true
    def cancel(%{test_pid: test_pid}, consumer_tag) do
      send(test_pid, {:cancel, consumer_tag})
      {:ok, consumer_tag}
    end

    @impl true
    def close_connection(%{test_pid: test_pid}) do
      send(test_pid, :connection_closed)
      :ok
    end
  end

  defmodule Forwarder do
    use Broadway

    def handle_message(_, message, %{test_pid: test_pid}) do
      channel = get_channel(message)
      send(test_pid, {:message_handled, message.data, channel})

      case message.data do
        :fail ->
          Message.failed(message, "failed")

        :break_conn ->
          Process.exit(channel.conn.pid, :shutdown)
          message

        _ ->
          message
      end
    end

    def handle_batch(_, messages, _, %{test_pid: test_pid}) do
      send(test_pid, {:batch_handled, Enum.map(messages, & &1.data)})
      messages
    end

    defp get_channel(%Message{acknowledger: {_, channel, _}}) do
      channel
    end
  end

  test "raise an ArgumentError with proper message when client options are invalid" do
    assert_raise(
      ArgumentError,
      "invalid options given to BroadwayRabbitmq.AmqpClient.init/1, expected :queue to be a non empty string, got: nil",
      fn ->
        BroadwayRabbitmq.Producer.init(queue: nil)
      end
    )
  end

  test "raise an ArgumentError with proper message when backoff options are invalid" do
    assert_raise(
      ArgumentError,
      "unknown type :unknown_type",
      fn ->
        BroadwayRabbitmq.Producer.init(queue: "test", backoff_type: :unknown_type)
      end
    )
  end

  test "producer :buffer_size is :prefetch_count * 5" do
    qos = [prefetch_count: 12]
    {:producer, _, options} = BroadwayRabbitmq.Producer.init(queue: "test", qos: qos)

    assert options[:buffer_size] == 60
  end

  test "forward messages delivered by the channel" do
    {:ok, broadway} = start_broadway()

    deliver_messages(broadway, 1..4)

    assert_receive {:batch_handled, [1, 2]}
    assert_receive {:batch_handled, [3, 4]}

    stop_broadway(broadway)
  end

  test "acknowledge/reject processed messages" do
    {:ok, broadway} = start_broadway()

    deliver_messages(broadway, [1, 2, :fail, 4, 5])

    assert_receive {:ack, 1}
    assert_receive {:ack, 2}
    assert_receive {:reject, :fail}
    assert_receive {:ack, 4}
    assert_receive {:ack, 5}

    stop_broadway(broadway)
  end

  describe "prepare_for_draining" do
    test "cancel consumer" do
      channel = FakeChannel.new(self())
      tag = :fake_consumer_tag
      state = %{client: FakeRabbitmqClient, channel: channel, consumer_tag: tag}

      assert BroadwayRabbitmq.Producer.prepare_for_draining(state) == :ok
      assert_received {:cancel, ^tag}
    end

    test "log unsuccessful cancellation" do
      channel = FakeChannel.new(self())
      tag = :fake_consumer_tag_closing
      state = %{client: FakeRabbitmqClient, channel: channel, consumer_tag: tag}

      assert capture_log(fn ->
               assert BroadwayRabbitmq.Producer.prepare_for_draining(state) == :ok
             end) =~ "[error] Could not cancel producer while draining. Channel is closing"
    end
  end

  describe "handle connection loss" do
    test "producer is not restarted" do
      {:ok, broadway} = start_broadway()
      assert_receive {:setup_channel, :ok, _}
      producer_1 = get_producer(broadway)

      deliver_messages(broadway, [1, :break_conn])
      assert_receive {:setup_channel, :ok, _}
      producer_2 = get_producer(broadway)

      assert producer_1 == producer_2

      stop_broadway(broadway)
    end

    test "open a new connection/channel and keep consuming messages" do
      {:ok, broadway} = start_broadway()
      assert_receive {:setup_channel, :ok, channel_1}

      deliver_messages(broadway, [1, 2])
      assert_receive {:message_handled, 1, ^channel_1}
      assert_receive {:message_handled, 2, ^channel_1}

      deliver_messages(broadway, [:break_conn])
      assert_receive {:setup_channel, :ok, channel_2}

      deliver_messages(broadway, [3, 4])
      assert_receive {:message_handled, 3, ^channel_2}
      assert_receive {:message_handled, 4, ^channel_2}

      assert channel_1.pid != channel_2.pid
      assert channel_1.conn.pid != channel_2.conn.pid

      stop_broadway(broadway)
    end

    test "processed messages delivered by the old connection/channel will not be acknowledged" do
      {:ok, broadway} = start_broadway()
      assert_receive {:setup_channel, :ok, channel}

      deliver_messages(broadway, [1, :break_conn])

      assert_receive {:message_handled, 1, ^channel}
      assert_receive {:message_handled, :break_conn, ^channel}

      refute_receive {:ack, 1}
      refute_receive {:ack, :break_conn}
    end

    test "log error when trying to acknowledge" do
      {:ok, broadway} = start_broadway()
      assert_receive {:setup_channel, :ok, channel}

      assert capture_log(fn ->
               deliver_messages(broadway, [:break_conn])
               refute_receive {:ack, :break_conn}
             end) =~ "(EXIT) no process: the process is not alive"

      stop_broadway(broadway)
    end
  end

  describe "handle connection refused" do
    test "log the error and try to reconnect" do
      assert capture_log(fn ->
               {:ok, broadway} = start_broadway(connect_responses: [:error])
               assert_receive {:setup_channel, :error, _}
               assert_receive {:setup_channel, :ok, _}
               stop_broadway(broadway)
             end) =~ "Cannot connect to RabbitMQ broker"
    end

    test "if backoff_type = :stop, log the error and don't try to reconnect" do
      assert capture_log(fn ->
               {:ok, broadway} = start_broadway(connect_responses: [:error], backoff_type: :stop)
               assert_receive {:setup_channel, :error, _}
               refute_receive {:setup_channel, _, _}
               stop_broadway(broadway)
             end) =~ "Cannot connect to RabbitMQ broker"
    end

    test "keep retrying to connect using the backoff strategy" do
      {:ok, broadway} = start_broadway(connect_responses: [:ok, :error, :error, :error, :ok])
      assert_receive {:setup_channel, :ok, _}

      deliver_messages(broadway, [1, :break_conn])

      assert_receive {:setup_channel, :error, _}
      assert get_backoff_timeout(broadway) == 10
      assert_receive {:setup_channel, :error, _}
      assert get_backoff_timeout(broadway) == 20
      assert_receive {:setup_channel, :error, _}
      assert get_backoff_timeout(broadway) == 40

      assert_receive {:setup_channel, :ok, _}
      refute_receive {:setup_channel, _, _}

      stop_broadway(broadway)
    end

    test "reset backoff timeout after a sucessful connection" do
      {:ok, broadway} = start_broadway(connect_responses: [:error, :ok])

      assert_receive {:setup_channel, :error, _}
      assert get_backoff_timeout(broadway) == 10

      assert_receive {:setup_channel, :ok, _}
      assert get_backoff_timeout(broadway) == nil

      stop_broadway(broadway)
    end
  end

  test "close connection on terminate" do
    {:ok, broadway} = start_broadway()
    assert_receive {:setup_channel, :ok, channel}
    Process.exit(broadway, :shutdown)
    assert_receive :connection_closed
  end

  defp start_broadway(opts \\ []) do
    connect_responses = Keyword.get(opts, :connect_responses, [])
    backoff_type = Keyword.get(opts, :backoff_type, :exp)

    {:ok, connection_agent} = Agent.start_link(fn -> connect_responses end)

    Broadway.start_link(Forwarder,
      name: new_unique_name(),
      context: %{test_pid: self()},
      producers: [
        default: [
          module:
            {BroadwayRabbitmq.Producer,
             client: FakeRabbitmqClient,
             queue: "test",
             test_pid: self(),
             backoff_type: backoff_type,
             backoff_min: 10,
             backoff_max: 100,
             connection_agent: connection_agent,
             qos: [prefetch_count: 10]},
          stages: 1
        ]
      ],
      processors: [
        default: [stages: 1]
      ],
      batchers: [
        default: [
          batch_size: 2,
          batch_timeout: 50,
          stages: 1
        ]
      ]
    )
  end

  defp new_unique_name() do
    :"Broadway#{System.unique_integer([:positive, :monotonic])}"
  end

  defp deliver_messages(broadway, messages) do
    producer = Broadway.Server.get_random_producer(broadway)

    Enum.each(messages, fn msg ->
      send(producer, {:basic_deliver, msg, %{delivery_tag: msg}})
    end)
  end

  defp get_producer(broadway, key \\ :default, index \\ 1) do
    name = Process.info(broadway)[:registered_name]
    :"#{name}.Producer_#{key}_#{index}"
  end

  defp get_backoff_timeout(broadway) do
    producer = get_producer(broadway)
    :sys.get_state(producer).state.module_state.backoff.state
  end

  defp stop_broadway(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :normal)

    receive do
      {:DOWN, ^ref, _, _, _} -> :ok
    end
  end
end
