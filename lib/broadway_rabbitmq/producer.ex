defmodule BroadwayRabbitmq.Producer do
  use GenStage

  require Logger

  alias Broadway.{Message, Acknowledger, Producer}
  alias BroadwayRabbitmq.Backoff

  @behaviour Acknowledger
  @behaviour Producer

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    client = opts[:client] || BroadwayRabbitmq.AmqpClient

    case client.init(opts) do
      {:error, message} ->
        raise ArgumentError, "invalid options given to #{inspect(client)}.init/1, " <> message

      {:ok, queue_name, config} ->
        send(self(), :connect)

        prefetch_count = config[:qos][:prefetch_count]
        options = [buffer_size: prefetch_count * 2]

        {:producer,
         %{
           client: client,
           channel: nil,
           consumer_tag: nil,
           queue_name: queue_name,
           config: config,
           backoff: Backoff.new(opts),
           conn_ref: nil
         }, options}
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
    # TODO: Better treat this differently
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

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{conn_ref: ref} = state) do
    {:noreply, [], connect(state)}
  end

  def handle_info(:connect, state) do
    {:noreply, [], connect(state)}
  end

  def handle_info(_, state) do
    {:noreply, [], state}
  end

  @impl true
  def terminate(_reason, state) do
    %{client: client, channel: channel} = state

    if channel do
      client.close_connection(channel.conn)
    end

    :ok
  end

  @impl Acknowledger
  def ack(channel, successful, failed) do
    ack_messages(successful, channel, :ack)
    ack_messages(failed, channel, :reject)
  end

  @impl Producer
  def prepare_for_draining(%{channel: nil}) do
    :ok
  end

  def prepare_for_draining(state) do
    %{client: client, channel: channel, consumer_tag: consumer_tag} = state

    case client.cancel(channel, consumer_tag) do
      {:ok, ^consumer_tag} ->
        :ok

      {:error, error} ->
        Logger.error("Could not cancel producer while draining. Channel is #{error}")
        :ok
    end
  end

  defp ack_messages(messages, channel, ack_func) do
    Enum.each(messages, fn msg ->
      {client, delivery_tag} = extract_client_and_delivery_tag(msg)

      try do
        apply(client, ack_func, [channel, delivery_tag])
      catch
        kind, reason ->
          Logger.error(Exception.format(kind, reason, System.stacktrace()))
      end
    end)
  end

  defp extract_client_and_delivery_tag(message) do
    {_, _, %{client: client, delivery_tag: delivery_tag}} = message.acknowledger
    {client, delivery_tag}
  end

  defp connect(state) do
    %{client: client, queue_name: queue_name, config: config, backoff: backoff} = state
    # TODO: Treat other setup errors properly
    case client.setup_channel(queue_name, config) do
      {:ok, channel} ->
        ref = Process.monitor(channel.conn.pid)
        backoff = backoff && Backoff.reset(backoff)
        consumer_tag = client.consume(channel, queue_name)
        %{state | channel: channel, consumer_tag: consumer_tag, backoff: backoff, conn_ref: ref}

      {:error, :econnrefused} ->
        handle_backoff(state)
    end
  end

  defp handle_backoff(%{backoff: backoff} = state) do
    Logger.error("Cannot connect to RabbitMQ broker")

    new_backoff =
      if backoff do
        {timeout, backoff} = Backoff.backoff(backoff)
        Process.send_after(self(), :connect, timeout)
        backoff
      end

    %{state | channel: nil, consumer_tag: nil, backoff: new_backoff, conn_ref: nil}
  end
end
