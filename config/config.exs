# All of these shenanigans are done to ensure that amqp + lager emit no logs at startup
# when testing. The quoted code is because Elixir 1.13 hard-deprecates Mix.Config,
# but if we call "use Mix.Config" it gets evaluated at compile-time and warns
# if we don't do the eval_quoted/1 dance.

lager_config = [
  error_logger_redirect: false,
  handlers: [level: :critical]
]

config_code =
  cond do
    Mix.env() == :test and Code.ensure_loaded?(Config) ->
      quote do
        import Config
        config :lager, unquote(lager_config)
      end

    Mix.env() == :test ->
      quote do
        use Mix.Config
        config :lager, unquote(lager_config)
      end

    true ->
      :ok
  end

Code.eval_quoted(config_code)
