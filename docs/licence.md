# Licence and warranty

AceMQ for FastAPI is [Apache License
2.0](https://www.apache.org/licenses/LICENSE-2.0). You may use it in production,
commercially, without asking and without paying.

## No warranty

The licence disclaims warranties and limits liability — sections 7 and 8. In plain
terms: this is provided as it is, and if it loses your messages that is your risk to
have taken.

That is not a formality to skim. This is a young package on its own version line,
starting at 0.1.0, wrapping a library that is itself pre-1.0. The API is free to
change. What has been proven against a real broker and what has not is written down in
[testing](testing.md) and in the repository's test suite, which is where to look before
deciding how much to rely on it.

## Its own version line

This repository versions separately from `acemq-amqp`, as the Spring Boot starter does
from `acemq-java-amqp`, and for the same reason: it tracks FastAPI and Starlette's
release train as much as AceMQ's. A FastAPI release that moves the lifespan contract is
a release this package has to answer, and it should not have to wait for the library to
cut a version to do it.

The dependency is declared as `acemq-amqp>=0.6.0,<0.7`, so a library release that
changes the API this wires up cannot arrive silently.

## Trademarks

FastAPI and Starlette are the work of their authors and are used here only to describe
what this package integrates with. This project is not affiliated with, endorsed by or
sponsored by them.

RabbitMQ is a trademark of Broadcom Inc. and/or its subsidiaries. References to
RabbitMQ describe compatibility only.

Python is a trademark of the Python Software Foundation.

## The dependencies

`acemq-amqp` (Apache-2.0), FastAPI (MIT), `pydantic-settings` (MIT), and through them
Starlette, pydantic, aio-pika and aiormq — each under its own licence. `pip show` and
the wheel metadata list what a given install actually pulled.
