defmodule BroadwayRabbitmq.Producer do
  use GenStage

  alias Broadway.{Message, Acknowledger}

  @behaviour Acknowledger

  @impl true
  def init(opts) do
    client = opts[:client] || BroadwayRabbitmq.AmqpClient

    case client.init(opts) do
      {:error, message} ->
        raise ArgumentError, "invalid options given to #{inspect(client)}.init/1, " <> message

      {:ok, queue_name, config} ->
        # TODO: Treat channel setup errors properly
        {:ok, channel} = client.setup_channel(queue_name, config)
        consumer_tag = client.consume(channel, queue_name)

        {:producer, %{client: client, channel: channel, consumer_tag: consumer_tag}}
    end
  end

  @impl true
  def handle_demand(_incoming_demand, state) do
    {:noreply, [], state}
  end

  @impl true
  def handle_info({:basic_consume_ok, %{consumer_tag: tag}}, state) do
    {:noreply, [], %{state | consumer_tag: tag}}
  end

  def handle_info({:basic_cancel, _}, state) do
    {:stop, :normal, %{state | consumer_tag: nil}}
  end

  def handle_info({:basic_cancel_ok, _}, state) do
    {:noreply, [], %{state | consumer_tag: nil}}
  end

  def handle_info({:basic_deliver, payload, meta}, state) do
    %{channel: channel, client: client} = state
    %{delivery_tag: tag} = meta

    ack_data = %{
      delivery_tag: tag,
      client: client
    }

    message = %Message{data: payload, acknowledger: {__MODULE__, channel, ack_data}}

    {:noreply, [message], state}
  end

  def handle_info(_, state) do
    {:noreply, [], state}
  end

  @impl true
  def ack(channel, successful, failed) do
    successful
    |> Enum.each(fn msg ->
      {client, delivery_tag} = extract_client_and_delivery_tag(msg)
      client.ack(channel, delivery_tag)
    end)

    failed
    |> Enum.each(fn msg ->
      {client, delivery_tag} = extract_client_and_delivery_tag(msg)
      client.reject(channel, delivery_tag)
    end)
  end

  defp extract_client_and_delivery_tag(message) do
    {_, _, %{client: client, delivery_tag: delivery_tag}} = message.acknowledger
    {client, delivery_tag}
  end
end
