# Changelog

## v0.7.1 (2021-11-25)

  * Add support to AMQP 3.0

## v0.7.0 (2021-08-30)

  * Add support to AMQP 2.0
  * Require Broadway 1.0

## v0.6.5 (2020-12-11)

  * Add support for a few Telemetry events. See the "Telemetry" section
    in the docs for `BroadwayRabbitMQ.Producer`.

  * Add support for `:consume_options` when starting a
    `BroadwayRabbitMQ.Producer` to pass options down to `AMQP.Basic.consume/4`.

## v0.6.4 (2020-11-23)

  * Bump nimble_options dependency to 0.3.5 which fixes some deprecation
    warnings.

  * Fix a few potential RabbitMQ issues like possible connection leaking (see
    [#83](https://github.com/dashbitco/broadway_rabbitmq/pull/83)).

## v0.6.3 (2020-11-19)

  * Start using nimble_options for validation. This has no practical
    consequences on the API but introduces a new dependency in broadway_rabbitmq
    (which was already used by Broadway).
  * Raise if acking messages fails. See the discussion in
    [dashbitco/broadway#208](https://github.com/dashbitco/broadway/issues/208).

## v0.6.2 (2020-10-24)

  * Deprecate use of a default `:on_failure` option.
  * Expose always-present `:amqp_channel` metadata containing the `AMQP.Channel`
    struct.

## v0.6.1 (2020-06-05)

  * Add support for the `:after_connect` option.
  * Add `auth_mechanisms` to the supported connection options for RabbitMQ.
  * Support passing in an AMQP connection name.
  * Update Broadway requirement to `~> 0.6.0` (it was exactly `0.6.0`) before.

## v0.6.0 (2020-02-19)

  * Update Broadway requirement to 0.6.0.
  * Re-initialize client options on every reconnect. This means that the `:merge_options`
    function is called on every reconnect, allowing to do things such as round-robin
    on a list of RabbitMQ URLs.
  * Remove support for the deprecated `:requeue` option. Use `:on_success`/`:on_failure`
    instead.
  * Improve logging on RabbitMQ disconnections and reconnections.

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
