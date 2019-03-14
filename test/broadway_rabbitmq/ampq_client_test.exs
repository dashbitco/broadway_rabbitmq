defmodule BroadwayRabbitmq.AmqpClientTest do
  use ExUnit.Case

  alias BroadwayRabbitmq.AmqpClient

  describe "validate init options" do
    test ":queue_name is required" do
      assert AmqpClient.init([]) ==
               {:error, "expected :queue_name to be a non empty string, got: nil"}

      assert AmqpClient.init(queue_name: nil) ==
               {:error, "expected :queue_name to be a non empty string, got: nil"}
    end

    test ":queue_name should be a non empty string" do
      assert AmqpClient.init(queue_name: "") ==
               {:error, "expected :queue_name to be a non empty string, got: \"\""}

      assert AmqpClient.init(queue_name: :an_atom) ==
               {:error, "expected :queue_name to be a non empty string, got: :an_atom"}

      {:ok, queue_name, _} = AmqpClient.init(queue_name: "my_queue")
      assert queue_name == "my_queue"
    end
  end
end
