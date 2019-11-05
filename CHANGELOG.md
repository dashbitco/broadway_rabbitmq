# Changelog

## v0.5.0 (2019-11-04)

  * Add support for configuring acking behaviour using `:on_success` and `:on_failure` options
  * Add support for declare options `:no_wait` and `:arguments`
  * Handle `:auth_failure`, `:unknown_host` and `:socket_closed_unexpectedly` errors
  * Add support for a function as the `:connection`
  * Add support for `:merge_options` option
  * Update to Broadway v0.5.0

## v0.4.0 (2019-08-06)

  * Add `:declare` and `:bindings` options to producers
  * Handle consumer cancellation by reconnecting

## v0.3.0 (2019-06-06)

  * Allow overriding `:buffer_size` and `:buffer_keep`
  * Make `:buffer_size` required if `:prefetch_count` is set to `0`
  * Allow passing RabbitMQ connection options via an AMQP URI

## v0.2.0 (2019-05-09)

  * New option `:metadata` that allows users to select which metadata should be retrieved
    and appended to the message struct
  * New option `:requeue` that allows users to define a strategy for requeuing failed messages

## v0.1.0 (2019-04-09)

* Initial release
