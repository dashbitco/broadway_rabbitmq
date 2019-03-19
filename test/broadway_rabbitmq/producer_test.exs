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
        conn: %{pid: fake_connection_pid},
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
      {:ok, opts[:queue_name], opts}
    end

    @impl true
    def setup_channel(_queue_name, config) do
      test_pid = config[:test_pid]
      channel = FakeChannel.new(test_pid)
      send(test_pid, {:setup_channel, channel})
      {:ok, channel}
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
    def consume(_channel, _queue_name) do
      :fake_consumer_tag
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

  describe "connection is lost" do
    test "open a new connection/channel and keep consuming messages" do
      {:ok, broadway} = start_broadway()
      assert_receive {:setup_channel, channel_1}

      deliver_messages(broadway, [1, 2])
      assert_receive {:message_handled, 1, ^channel_1}
      assert_receive {:message_handled, 2, ^channel_1}

      deliver_messages(broadway, [:break_conn])
      assert_receive {:setup_channel, channel_2}

      deliver_messages(broadway, [3, 4])
      assert_receive {:message_handled, 3, ^channel_2}
      assert_receive {:message_handled, 4, ^channel_2}

      assert channel_1.pid != channel_2.pid
      assert channel_1.conn.pid != channel_2.conn.pid

      stop_broadway(broadway)
    end

    test "processed messages delivered by the old connection/channel will not be acknowledged" do
      {:ok, broadway} = start_broadway()
      assert_receive {:setup_channel, channel}

      deliver_messages(broadway, [1, :break_conn])

      assert_receive {:message_handled, 1, ^channel}
      assert_receive {:message_handled, :break_conn, ^channel}

      refute_receive {:ack, 1}
      refute_receive {:ack, :break_conn}
    end

    test "log error when trying to acknowledge" do
      {:ok, broadway} = start_broadway()
      assert_receive {:setup_channel, channel}

      assert capture_log(fn ->
               deliver_messages(broadway, [:break_conn])
               refute_receive {:ack, :break_conn}
             end) =~ "(EXIT) no process: the process is not alive"

      stop_broadway(broadway)
    end
  end

  defp start_broadway() do
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

  defp stop_broadway(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :normal)

    receive do
      {:DOWN, ^ref, _, _, _} -> :ok
    end
  end
end
