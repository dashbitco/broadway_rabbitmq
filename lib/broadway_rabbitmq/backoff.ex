defmodule BroadwayRabbitMQ.Backoff do
  @moduledoc false

  alias BroadwayRabbitMQ.Backoff

  @min 1_000
  @max 30_000

  defstruct [:type, :min, :max, :state]

  def new(opts) do
    case Keyword.fetch!(opts, :backoff_type) do
      :stop ->
        nil

      type ->
        {min, max} = min_max(opts)
        new(type, min, max)
    end
  end

  def backoff(%Backoff{type: :rand, min: min, max: max, state: state} = s) do
    {backoff, state} = rand(state, min, max)
    {backoff, %Backoff{s | state: state}}
  end

  def backoff(%Backoff{type: :exp, min: min, state: nil} = s) do
    {min, %Backoff{s | state: min}}
  end

  def backoff(%Backoff{type: :exp, max: max, state: prev} = s) do
    require Bitwise
    next = min(Bitwise.<<<(prev, 1), max)
    {next, %Backoff{s | state: next}}
  end

  def backoff(%Backoff{type: :rand_exp, max: max, state: state} = s) do
    {prev, lower, rand_state} = state
    next_min = min(prev, lower)
    next_max = min(prev * 3, max)
    {next, rand_state} = rand(rand_state, next_min, next_max)
    {next, %Backoff{s | state: {next, lower, rand_state}}}
  end

  def reset(%Backoff{type: :rand} = s), do: s
  def reset(%Backoff{type: :exp} = s), do: %Backoff{s | state: nil}

  def reset(%Backoff{type: :rand_exp, min: min, state: state} = s) do
    {_, lower, rand_state} = state
    %Backoff{s | state: {min, lower, rand_state}}
  end

  ## Internal

  defp min_max(opts) do
    case {opts[:backoff_min], opts[:backoff_max]} do
      {nil, nil} -> {@min, @max}
      {nil, max} -> {min(@min, max), max}
      {min, nil} -> {min, max(min, @max)}
      {min, max} -> {min, max}
    end
  end

  defp new(_, min, _) when not (is_integer(min) and min >= 0) do
    raise ArgumentError, "minimum #{inspect(min)} not 0 or a positive integer"
  end

  defp new(_, _, max) when not (is_integer(max) and max >= 0) do
    raise ArgumentError, "maximum #{inspect(max)} not 0 or a positive integer"
  end

  defp new(_, min, max) when min > max do
    raise ArgumentError, "minimum #{min} is greater than maximum #{max}"
  end

  defp new(:rand, min, max) do
    %Backoff{type: :rand, min: min, max: max, state: seed()}
  end

  defp new(:exp, min, max) do
    %Backoff{type: :exp, min: min, max: max, state: nil}
  end

  defp new(:rand_exp, min, max) do
    lower = max(min, div(max, 3))
    %Backoff{type: :rand_exp, min: min, max: max, state: {min, lower, seed()}}
  end

  defp new(type, _, _) do
    raise ArgumentError, "unknown type #{inspect(type)}"
  end

  defp seed() do
    :rand.seed_s(:exsplus)
  end

  defp rand(state, min, max) do
    {int, state} = :rand.uniform_s(max - min + 1, state)

    {int + min - 1, state}
  end
end
