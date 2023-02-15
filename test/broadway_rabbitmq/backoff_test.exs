defmodule BroadwayRabbitMQ.BackoffTest do
  use ExUnit.Case, async: true

  alias BroadwayRabbitMQ.Backoff

  @moduletag backoff_min: 1_000
  @moduletag backoff_max: 30_000

  describe "new/1" do
    test "without :backoff_min" do
      assert %{min: 1000, max: 2000} = new(backoff_type: :exp, backoff_max: 2000)
      assert %{min: 500, max: 500} = new(backoff_type: :exp, backoff_max: 500)
    end

    test "without :backoff_max" do
      assert %{min: 20_000, max: 30_000} = new(backoff_type: :exp, backoff_min: 20_000)
      assert %{min: 35_000, max: 35_000} = new(backoff_type: :exp, backoff_min: 35_000)
    end

    test "with :backoff_min and :backoff_max" do
      min = Enum.random(1..100)
      max = Enum.random(101..200)
      assert %{min: ^min, max: ^max} = new(backoff_type: :exp, backoff_min: min, backoff_max: max)
    end

    test "raises an error if :backoff_min is not an integer or a negative integer" do
      assert_raise ArgumentError, "minimum 3.14 not 0 or a positive integer", fn ->
        new(backoff_type: :exp, backoff_min: 3.14)
      end

      assert_raise ArgumentError, "minimum -1 not 0 or a positive integer", fn ->
        new(backoff_type: :exp, backoff_min: -1)
      end
    end

    test "raises an error if :backoff_max is not an integer or a negative integer" do
      assert_raise ArgumentError, "maximum 3.14 not 0 or a positive integer", fn ->
        new(backoff_type: :exp, backoff_min: 0, backoff_max: 3.14)
      end

      assert_raise ArgumentError, "maximum -1 not 0 or a positive integer", fn ->
        new(backoff_type: :exp, backoff_min: 0, backoff_max: -1)
      end
    end

    test "raises an error if :backoff_min is greater than :backoff_max" do
      assert_raise ArgumentError, "minimum 1000 is greater than maximum 500", fn ->
        new(backoff_type: :exp, backoff_min: 1000, backoff_max: 500)
      end
    end

    test "raises if :backoff_type is unknown" do
      assert_raise ArgumentError, "unknown type :unknown", fn ->
        new(backoff_type: :unknown)
      end
    end
  end

  @tag backoff_type: :exp
  test "exponential backoffs always in [min, max]", context do
    backoff = new(context)
    {delays, _} = backoff(backoff, 20)

    assert Enum.all?(delays, fn delay ->
             delay >= context[:backoff_min] and delay <= context[:backoff_max]
           end)
  end

  @tag backoff_type: :exp
  test "exponential backoffs double until max", context do
    backoff = new(context)
    {delays, _} = backoff(backoff, 20)

    Enum.reduce(delays, fn next, prev ->
      assert div(next, 2) == prev or next == context[:backoff_max]
      next
    end)
  end

  @tag backoff_type: :exp
  test "exponential backoffs reset to min", context do
    backoff = new(context)
    {[delay | _], backoff} = backoff(backoff, 20)
    assert delay == context[:backoff_min]

    backoff = Backoff.reset(backoff)
    {[delay], _} = backoff(backoff, 1)
    assert delay == context[:backoff_min]
  end

  @tag backoff_type: :rand
  test "random backoffs always in [min, max]", context do
    backoff = new(context)
    {delays, _} = backoff(backoff, 20)

    assert Enum.all?(delays, fn delay ->
             delay >= context[:backoff_min] and delay <= context[:backoff_max]
           end)
  end

  @tag backoff_type: :rand
  test "random backoffs are not all the same value", context do
    backoff = new(context)
    {delays, _} = backoff(backoff, 20)
    ## If the stars align this test could fail ;)
    refute Enum.all?(delays, &(hd(delays) == &1))
  end

  @tag backoff_type: :rand
  test "random backoffs repeat", context do
    backoff = new(context)
    assert backoff(backoff, 20) == backoff(backoff, 20)
  end

  @tag backoff_type: :rand
  test "random backoffs reset by not changing anything", context do
    backoff = new(context)
    assert Backoff.reset(backoff) == backoff
  end

  @tag backoff_type: :rand_exp
  test "random exponential backoffs always in [min, max]", context do
    backoff = new(context)
    {delays, _} = backoff(backoff, 20)

    assert Enum.all?(delays, fn delay ->
             delay >= context[:backoff_min] and delay <= context[:backoff_max]
           end)
  end

  @tag backoff_type: :rand_exp
  test "random exponential backoffs increase until a third of max", context do
    backoff = new(context)
    {delays, _} = backoff(backoff, 20)

    Enum.reduce(delays, fn next, prev ->
      assert next >= prev or next >= div(context[:backoff_max], 3)
      next
    end)
  end

  @tag backoff_type: :rand_exp
  test "random exponential backoffs repeat", context do
    backoff = new(context)
    assert backoff(backoff, 20) == backoff(backoff, 20)
  end

  @tag backoff_type: :rand_exp
  test "random exponential backoffs reset in [min, min * 3]", context do
    backoff = new(context)
    {[delay | _], backoff} = backoff(backoff, 20)
    assert delay in context[:backoff_min]..(context[:backoff_min] * 3)

    backoff = Backoff.reset(backoff)
    {[delay], _} = backoff(backoff, 1)
    assert delay in context[:backoff_min]..(context[:backoff_min] * 3)
  end

  ## Helpers

  def new(context) do
    Backoff.new(Enum.into(context, []))
  end

  defp backoff(backoff, n) do
    Enum.map_reduce(1..n, backoff, fn _, acc -> Backoff.backoff(acc) end)
  end
end
