defmodule BroadwayRabbitmq.AmqpClientTest do
  use ExUnit.Case

  alias BroadwayRabbitmq.AmqpClient

  test "default options" do
    assert AmqpClient.init(queue: "queue") ==
             {:ok, "queue", %{connection: [], declare: [], qos: [prefetch_count: 50]}}
  end

  describe "validate init options" do
    test "supported options" do
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

      declare = [
        durable: nil,
        auto_delete: nil,
        exclusive: nil,
        passive: nil
      ]

      qos = [
        prefetch_size: nil,
        prefetch_count: nil
      ]

      options = [
        queue: "queue",
        connection: connection,
        declare: declare,
        qos: qos
      ]

      assert AmqpClient.init(options) ==
               {:ok, "queue", %{connection: connection, declare: declare, qos: qos}}
    end

    test "unsupported options for Broadway" do
      assert AmqpClient.init(queue: "queue", option_1: 1, option_2: 2) ==
               {:error, "Unsupported options [:option_1, :option_2] for \"Broadway\""}
    end

    test "unsupported options for :connection" do
      assert AmqpClient.init(queue: "queue", connection: [option_1: 1, option_2: 2]) ==
               {:error, "Unsupported options [:option_1, :option_2] for :connection"}
    end

    test "unsupported options for :declare" do
      assert AmqpClient.init(queue: "queue", declare: [option_1: 1, option_2: 2]) ==
               {:error, "Unsupported options [:option_1, :option_2] for :declare"}
    end

    test "unsupported options for :qos" do
      assert AmqpClient.init(queue: "queue", qos: [option_1: 1, option_2: 2]) ==
               {:error, "Unsupported options [:option_1, :option_2] for :qos"}
    end

    test ":queue is required" do
      assert AmqpClient.init([]) == {:error, "expected :queue to be a non empty string, got: nil"}

      assert AmqpClient.init(queue: nil) ==
               {:error, "expected :queue to be a non empty string, got: nil"}
    end

    test ":queue should be a non empty string" do
      assert AmqpClient.init(queue: "") ==
               {:error, "expected :queue to be a non empty string, got: \"\""}

      assert AmqpClient.init(queue: :an_atom) ==
               {:error, "expected :queue to be a non empty string, got: :an_atom"}

      {:ok, queue, _} = AmqpClient.init(queue: "my_queue")
      assert queue == "my_queue"
    end
  end
end
