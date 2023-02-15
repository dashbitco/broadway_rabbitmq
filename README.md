# BroadwayRabbitMQ

[![hex.pm badge](https://img.shields.io/badge/Package%20on%20hex.pm-informational)](https://hex.pm/packages/broadway_rabbitmq)
[![Documentation badge](https://img.shields.io/badge/Documentation-ff69b4)][docs]
[![CI](https://github.com/dashbitco/broadway_rabbitmq/actions/workflows/ci.yml/badge.svg)](https://github.com/dashbitco/broadway_rabbitmq/actions/workflows/ci.yml)
[![Coverage Status](https://coveralls.io/repos/github/dashbitco/broadway_rabbitmq/badge.svg?branch=master)](https://coveralls.io/github/dashbitco/broadway_rabbitmq?branch=master)

A RabbitMQ connector for [Broadway](https://github.com/dashbitco/broadway).

Documentation can be found at [https://hexdocs.pm/broadway_rabbitmq][docs].
For more details on using Broadway with RabbitMQ, please see the
[RabbitMQ Guide](https://hexdocs.pm/broadway/rabbitmq.html).

## Installation

Add `:broadway_rabbitmq` to the list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:broadway_rabbitmq, "~> 0.7.0"}
  ]
end
```

## Usage

Configure Broadway with one or more producers using `BroadwayRabbitMQ.Producer`:

```elixir
  defmodule MyBroadway do
    use Broadway

    def start_link(_opts) do
      Broadway.start_link(__MODULE__,
        name: __MODULE__,
        producer: [
          module: {BroadwayRabbitMQ.Producer,
            queue: "my_queue",
          },
          concurrency: 1
        ],
        processors: [
          default: [
            concurrency: 10
          ]
        ]
      )
    end

    def handle_message(_, message, _) do
      IO.inspect(message.data, label: "Got message")
      message
    end
  end
```

## License

Copyright 2019 Plataformatec\
Copyright 2020 Dashbit

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

[docs]: https://hexdocs.pm/broadway_rabbitmq
