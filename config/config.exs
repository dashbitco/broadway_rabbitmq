use Mix.Config

if :ok == Application.ensure_loaded(:lager) do
  config :lager,
    error_logger_redirect: false,
    handlers: [level: :critical]
end
