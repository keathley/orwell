use Mix.Config

config :logger,
  level: :warn,
  compile_time_purge_matching: [
    [level_lower_than: :info]
  ],
  handle_sasl_reports: true,
  handle_otp_reports: true
