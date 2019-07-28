defmodule BroadwayRabbitMQ.AmqpClientTest do
  use ExUnit.Case

  alias BroadwayRabbitMQ.AmqpClient

  test "default options" do
    assert AmqpClient.init(queue: "queue") ==
             {:ok,
              %{
                connection: [],
                qos: [prefetch_count: 50],
                requeue: :always,
                metadata: [],
                bindings: [],
                declare_opts: nil,
                queue: "queue"
              }}
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

      qos = [
        prefetch_size: nil,
        prefetch_count: nil
      ]

      options = [
        queue: "queue",
        requeue: :once,
        connection: connection,
        qos: qos
      ]

      metadata = []

      assert AmqpClient.init(options) ==
               {:ok,
                %{
                  connection: connection,
                  qos: qos,
                  requeue: :once,
                  metadata: metadata,
                  bindings: [],
                  declare_opts: nil,
                  queue: "queue"
                }}
    end

    test "providing connection via a URI" do
      connection = "amqp://guest:guest@127.0.0.1"

      assert {:ok, %{connection: ^connection, queue: "queue"}} =
               AmqpClient.init(queue: "queue", connection: connection)
    end

    test "invalid URI" do
      assert {:error, "Failed parsing AMQP URI: " <> _} =
               AmqpClient.init(queue: "queue", connection: "http://example.com")
    end

    test "unsupported options for Broadway" do
      assert AmqpClient.init(queue: "queue", option_1: 1, option_2: 2) ==
               {:error, "Unsupported options [:option_1, :option_2] for \"Broadway\""}
    end

    test "unsupported options for :connection" do
      assert AmqpClient.init(queue: "queue", connection: [option_1: 1, option_2: 2]) ==
               {:error, "Unsupported options [:option_1, :option_2] for :connection"}
    end

    test "unsupported options for :qos" do
      assert AmqpClient.init(queue: "queue", qos: [option_1: 1, option_2: 2]) ==
               {:error, "Unsupported options [:option_1, :option_2] for :qos"}
    end

    test "unsupported options for :declare" do
      assert AmqpClient.init(queue: "queue", declare: [option_1: 1, option_2: 2]) ==
               {:error, "Unsupported options [:option_1, :option_2] for :declare"}
    end

    test ":queue is required" do
      assert AmqpClient.init([]) == {:error, "expected :queue to be a string, got: nil"}

      assert AmqpClient.init(queue: nil) ==
               {:error, "expected :queue to be a string, got: nil"}
    end

    test ":queue should be a string" do
      assert AmqpClient.init(queue: :an_atom) ==
               {:error, "expected :queue to be a string, got: :an_atom"}

      {:ok, config} = AmqpClient.init(queue: "my_queue")
      assert config.queue == "my_queue"
    end

    test "queue shouldn't be an empty string if :declare is not present" do
      assert AmqpClient.init(queue: "") ==
               {:error,
                "can't use \"\" (server autogenerate) as the queue name without the :declare"}

      assert {:ok, config} = AmqpClient.init(queue: "", declare: [])
      assert config.queue == ""
    end

    test ":requeue is optional" do
      assert {:ok, _config} = AmqpClient.init(queue: "queue")
    end

    test ":requeue should be :never, :always or :once" do
      {:ok, opts} = AmqpClient.init(queue: "queue", requeue: :never)
      assert opts[:requeue] == :never
      {:ok, opts} = AmqpClient.init(queue: "queue", requeue: :always)
      assert opts[:requeue] == :always
      {:ok, opts} = AmqpClient.init(queue: "queue", requeue: :once)
      assert opts[:requeue] == :once
      {:error, reason} = AmqpClient.init(queue: "queue", requeue: :unsupported)

      assert reason ==
               "expected :queue to be any of [:never, :always, :once], got: :unsupported"
    end

    test ":metadata should be a list of atoms" do
      {:ok, opts} = AmqpClient.init(queue: "queue", metadata: [:routing_key, :headers])
      assert opts[:metadata] == [:routing_key, :headers]

      assert AmqpClient.init(queue: "queue", metadata: ["routing_key", :headers]) ==
               {:error,
                "expected :metadata to be a list of atoms, got: [\"routing_key\", :headers]"}
    end

    test ":bindings should be a list of tuples" do
      {:ok, opts} = AmqpClient.init(queue: "queue", bindings: [{"my-exchange", [nowait: true]}])
      assert opts[:bindings] == [{"my-exchange", [nowait: true]}]

      assert AmqpClient.init(queue: "queue", bindings: [:something, :else]) ==
               {:error,
                "expected :bindings to be a list of bindings (keyword lists), got: [:something, :else]"}
    end
  end
end
