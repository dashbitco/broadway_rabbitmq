defmodule BroadwayRabbitmq.ProducerTest do
  use ExUnit.Case

  alias Broadway.Message
  alias AMQP.{Channel, Connection}

  defmodule FakeChannel do
    def new(test_pid) do
      fake_connection_pid = spawn_link(fn -> Process.sleep(:infinity) end)
      {:ok, fake_channel_pid} = start_link(test_pid)
      %Channel{pid: fake_channel_pid, conn: %Connection{pid: fake_connection_pid}}
    end

    def get_test_pid(channel) do
      Agent.get(channel.pid, fn %{test_pid: test_pid} -> test_pid end)
    end

    def get_producer_pid(channel) do
      Agent.get(channel.pid, fn %{producer_pid: producer_pid} -> producer_pid end)
    end

    def set_producer_pid(channel, producer_pid) do
      Agent.update(channel.pid, fn state -> %{state | producer_pid: producer_pid} end)
    end

    def deliver_messages(channel, messages) do
      producer_pid = get_producer_pid(channel)

      Enum.each(messages, fn msg ->
        send(producer_pid, {:basic_deliver, msg, %{delivery_tag: msg}})
      end)
    end

    defp start_link(test_pid) do
      Agent.start_link(fn -> %{test_pid: test_pid, producer_pid: nil} end)
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
      channel = config[:fake_channel]
      :ok = FakeChannel.set_producer_pid(channel, self())
      {:ok, channel}
    end

    @impl true
    def ack(channel, delivery_tag) do
      test_pid = FakeChannel.get_test_pid(channel)
      send(test_pid, {:ack, delivery_tag})
    end

    @impl true
    def reject(channel, delivery_tag) do
      test_pid = FakeChannel.get_test_pid(channel)
      send(test_pid, {:reject, delivery_tag})
    end

    @impl true
    def consume(_channel, _queue_name) do
      :fake_consumer_tag
    end
  end

  defmodule Forwarder do
    use Broadway

    def handle_message(_, message, %{test_pid: test_pid}) do
      send(test_pid, {:message_handled, message.data})

      case message.data do
        :fail -> Message.failed(message, "failed")
        _ -> message
      end
    end

    def handle_batch(_, messages, _, %{test_pid: test_pid}) do
      send(test_pid, {:batch_handled, Enum.map(messages, & &1.data)})
      messages
    end
  end

  test "forward messages delivered by the channel" do
    channel = FakeChannel.new(self())
    {:ok, pid} = start_broadway(channel)

    FakeChannel.deliver_messages(channel, 1..4)

    assert_receive {:batch_handled, [1, 2]}
    assert_receive {:batch_handled, [3, 4]}

    stop_broadway(pid)
  end

  test "acknowledge/reject processed messages" do
    channel = FakeChannel.new(self())
    {:ok, pid} = start_broadway(channel)

    FakeChannel.deliver_messages(channel, [1, 2, :fail, 4, 5])

    assert_receive {:ack, 1}
    assert_receive {:ack, 2}
    assert_receive {:reject, :fail}
    assert_receive {:ack, 4}
    assert_receive {:ack, 5}

    stop_broadway(pid)
  end

  defp start_broadway(fake_channel) do
    Broadway.start_link(Forwarder,
      name: new_unique_name(),
      context: %{test_pid: self()},
      producers: [
        default: [
          module:
            {BroadwayRabbitmq.Producer,
             client: FakeRabbitmqClient,
             queue: "test",
             fake_channel: fake_channel,
             qos: [
               prefetch_count: 10
             ]},
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

  defp stop_broadway(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :normal)

    receive do
      {:DOWN, ^ref, _, _, _} -> :ok
    end
  end
end
