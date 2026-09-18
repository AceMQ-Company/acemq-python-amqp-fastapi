# Changelog

All notable changes to this project are documented here.

The format is [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This repository has a version line of its own, separate from `acemq-amqp`, as the
Spring Boot starter has one separate from `acemq-java-amqp`. The reason is the same: it
tracks FastAPI and Starlette's release train as much as AceMQ's. A FastAPI release that
moves the lifespan contract is a release this package has to answer, and waiting for the
library to cut a version to do it would put the two on a schedule neither of them
chose.

## [Unreleased]

## [0.1.0] - 2026-09-18

First release. It wires `acemq-amqp` 0.6.0 into a FastAPI application and does nothing
else.

### Added

- **`AceMQ`** — the object an application holds. Settings, a registry of consumer
  registrations, and the connection once the lifespan has opened it. Built at import
  time and connected to nothing until then, so a module that declares consumers can
  still be imported by a test or by `--help`.
- **`AceMQ.lifespan`** — `FastAPI(lifespan=acemq.lifespan)`. Opens the connection under
  `acemq.connection_timeout`, registers interceptors, applies `acemq.topology`,
  subscribes every registered consumer, and puts itself on `app.state.acemq`. On the
  way out it drains the consumers and releases the connection. Start-up is
  all-or-nothing: a consumer that cannot subscribe unwinds what was already done and
  lets the exception out, because a half-started application and a healthy one look the
  same from outside.
- **`compose_lifespans(*lifespans)`** — nests several lifespans as one, outside in, and
  merges whatever state they yield. AceMQ belongs last, so consumers start after the
  things their handlers use and stop before those go away.
- **`@acemq.consumer(queue, ...)`** — the Python analogue of `@AceListener`. Records a
  registration and returns the function unchanged, so a handler stays an ordinary async
  function a test can call with a `Message`. Per-consumer `name`, `codec`, `retry`,
  `prefetch`, `concurrency`, `declare`, `tag` and `args`; anything left out is resolved
  from `acemq.consumer.*` when the lifespan opens, not when the decorator runs.
  `add_consumer` is the same thing without the decorator.
- **`acemq.consumers`, `start_consumer`, `stop_consumer`** — the registry, by name, for
  scaling, pausing and reading counters. `stop_consumer` drains rather than kills.
  `acemq.consumer.auto_start=false` registers everything and starts nothing.
- **`publishes(routing_key, exchange="")`** — a dependency yielding a `Publisher` for
  one destination, found through `request.app.state.acemq` rather than a module global,
  so two applications in one process reach their own connection. Publishers are cached
  per destination and cleared with the connection.
- **`AceMQConnection` and `AceMQDep`** — the open connection and the integration itself,
  for a route that needs more than a publisher.
- **`AceMQHealth` and `acemq.health_router()`** — a check satisfying the library's
  `HealthCheck` protocol, so it composes into `aggregate_health` beside an
  application's own, and a router that answers on `acemq.health.path`. **A blocked
  connection reports up, with the reason**, matching the Spring starter: restarting into
  a broker that is still blocked helps nobody. Degraded — a consumer that has stopped
  reading — also answers 200; 503 is reserved for a broker that did not answer.
- **`AceMQSettings`** — every `acemq.*` setting, bound from `ACEMQ_*` with `__` between
  levels and from a `.env` file. URL, credentials, virtual host, client name, format,
  prefetch, `max_outstanding_publishes`, confirm and connection timeouts, TLS, topology,
  consumer defaults with a retry ladder, and the health route. Durations accept a bare
  number of seconds as well as ISO 8601, because that is the spelling every environment
  variable actually uses.
- **`AceMQ.drain()` and `DrainReport`** — the shutdown, bounded by
  `acemq.consumer.shutdown_timeout`, reporting whether it finished and how many messages
  were abandoned when it did not.
- **`AceMQ.start()` / `aclose()`** — the same start-up for a worker process that serves
  no HTTP and should not have to construct an ASGI application to get it.
- **Documentation** — eight pages under `docs/`, rendered to a site by
  `.github/scripts/build-docs-site.sh` with the API reference from the docstrings.
- **67 tests.** Unit tests run against a transport that is a dictionary; the ones that
  are about the wire are marked `integration` and run against a real broker in CI.

### Decisions

- **`pydantic-settings` is a dependency, not an extra.** FastAPI already brings
  pydantic, so the marginal cost is one small package that reads environment variables
  into a model. Writing a parser here for durations, nested keys and `.env` files would
  be a worse version of it that this repository would own forever. The Spring starter
  does not write its own binder either.
- **`acemq.publisher_confirms=false` raises rather than being ignored.** The Python
  library publishes with confirms always. A setting that silently did nothing would
  leave a service believing it had traded safety for throughput and having neither.
- **Nothing from `acemq_amqp` is re-exported.** Messages, acknowledgements, codecs and
  retry policies keep the library's names, so an import says which half of the system a
  line is talking about.
- **The health route is mounted explicitly.** A route appearing in an application's
  OpenAPI document without anybody adding it is a route somebody has to go looking for.

### Known limits

- **`parts.blocked` can be `null`.** There is no supported way to ask aio-pika whether
  the broker has blocked a connection: aiormq sets and clears a private event and
  discards the reason RabbitMQ sent with it. The state is read by walking down to that
  event by name; when it is not found the report says `null` rather than `false`,
  because "nobody looked" and "not blocked" are different facts. The reason string is
  genuinely unavailable. A supported accessor belongs on the library's transport,
  beside the `isBlocked()` and `blockedReason()` the Java library already has.
- **Handlers share the event loop with the routes.** A handler that blocks it stalls the
  HTTP server. This package does not hide that; `docs/consumers.md` says what to do
  about it.
- **FastAPI only.** Nothing here is reusable in a WSGI application, and `docs/index.md`
  explains why a Django integration is a different design rather than a missing feature.

[Unreleased]: https://github.com/AceMQ-Company/acemq-python-amqp-fastapi/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/AceMQ-Company/acemq-python-amqp-fastapi/releases/tag/v0.1.0
