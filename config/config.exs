use Mix.Config

# ensure_loaded was added in Elixir 1.10
if Application.load(:lager) in [:ok, {:error, {:already_loaded, :lager}}] do
  config :lager,
    error_logger_redirect: false,
    handlers: [level: :critical]
end
