defmodule BroadwayRabbitMQ.Telemetry do
  @moduledoc false

  # TODO: remove when telemetry comes out with a release that supports telemetry:span/3. The
  # implementation here is basically taken straigh out of
  # https://github.com/beam-telemetry/telemetry/blob/53fbf8abdebea0ff0d9de9bb09740c578533d479/src/telemetry.erl#L258
  @spec span([atom(), ...], %{optional(atom) => term()}, (() -> result)) :: result
        when result: term()
  def span(prefix, meta \\ %{}, fun)
      when is_list(prefix) and is_map(meta) and is_function(fun, 0) do
    start_time = System.monotonic_time()

    :ok = :telemetry.execute(prefix ++ [:start], %{system_time: System.system_time()}, meta)

    try do
      fun.()
    catch
      kind, error ->
        duration = System.monotonic_time() - start_time
        meta = Map.merge(meta, %{kind: kind, reason: error, stacktrace: __STACKTRACE__})
        :ok = :telemetry.execute(prefix ++ [:exception], %{duration: duration}, meta)
        :erlang.raise(kind, error, __STACKTRACE__)
    else
      result ->
        duration = System.monotonic_time() - start_time
        :ok = :telemetry.execute(prefix ++ [:stop], %{duration: duration}, meta)
        result
    end
  end
end
