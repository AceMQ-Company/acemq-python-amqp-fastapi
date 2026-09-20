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

### Changed

- **The blocked health detail is a different sentence, and an alert rule matching the
  old one stops matching with nothing erroring.** That is the headline because it is the
  only change here that can break something quietly. It was:

  > the broker is applying back pressure and has blocked this connection, usually
  > because it is low on disk or memory. Reported up on purpose: restarting into a
  > broker that is still blocked helps nobody, and this instance is still serving

  and it is now:

  > the broker has blocked this connection; publishing is paused

  Word for word what Go, Ruby and the library itself write, so one alert rule reads a
  blocked broker whatever language the service behind it is in. This package no longer
  has a wording of its own: the sentence comes from `acemq-amqp` and is passed through.

  **What an operator has to change.** A rule matching `detail` on `back pressure`, on
  `usually because it is low on disk or memory`, or on any other phrase from the old
  sentence, matches nothing now and reports nothing — a blocked broker looks like a
  quiet one. Replace it with `parts.blocked == true`, which is the better selector and
  did not change: same key, same three states, same place in the payload. A rule that
  must read the text should match the *prefix* `the broker has blocked this connection`
  rather than the whole string, because a broker reason is appended after a colon where
  a client keeps one, and because a stalled consumer during a block puts its own clause
  in front of it.

  Everything else an operator watches is unchanged. Blocked still answers **200** and
  still reports **up**, `parts.blocked` is still `true`/`false`/`null`, and 503 is still
  reserved for a broker that did not answer at all. The argument that used to be inside
  the string — why blocked is reported up rather than down — is in `docs/health.md`,
  where it does not have to be read at three in the morning.

- **`acemq-amqp` is now `>=0.7.0,<0.8`**, up from `>=0.6.0,<0.7`, which excluded it.
  The floor is 0.7.0 because the health check calls `Connection.blocked`, which 0.6.0
  does not have: a resolver allowed to pick the older release would install something
  that imports cleanly and raises `AttributeError` the first time a readiness probe
  fires. The ceiling stays one minor ahead, because a pre-1.0 library puts its breaking
  changes in minor bumps — 0.7.0 itself rewrote the sentence above — and moving it
  should go on being a decision somebody made.

- **`acemq.health.timeout` is handed to the library's probe instead of wrapped around
  it.** The check used to be an `asyncio.wait_for` around `Connection.health()`, which
  had no deadline of its own; 0.7.0 gave it one, and the wrapper was kept only long
  enough to decide against it. Two deadlines are worse than one: the outer one is the
  cruder, and when a blocked broker stops answering the library recognises it and
  reports *up* where the wrapper firing first would have reported a spurious *down*. It
  would also cancel a request the library deliberately abandons — a broker that is not
  reading cannot be relied on to process a cancellation either. The setting's meaning,
  name and five-second default are unchanged; it is now the probe's deadline rather than
  a limit on the whole check.

- **A blocked connection whose consumer has also stopped reading now reports
  degraded**, where it used to report up. The block was checked first and returned
  immediately, so a stalled consumer went unmentioned for as long as the alarm lasted.
  The connection reports both, with the consumer's failure first, because that one is
  this instance's own and outlives the alarm. Still 200 either way.

### Removed

- **`acemq_fastapi.health.blocked_state`** and the private-attribute walk behind it —
  about 60 lines. It followed `transport → connection → transport → connection` looking
  for aiormq's `_Connection__connection_unblocked` by its name-mangled attribute,
  because until 0.7.0 nothing above it exposed the state and a health check that cannot
  tell a blocked broker from a wedged one is a health check that gets pods restarted
  into an alarm. `Connection.blocked` replaces it exactly, including the three-state
  answer: `None` still means the question could not be asked and is still not folded
  into `false`.

  It was never exported from `acemq_fastapi`, so nothing that imported the package by
  its public name is affected; anything reaching into `acemq_fastapi.health` for it
  should read `acemq.connection.blocked` instead.

- **`acemq_fastapi.health.BLOCKED_DETAIL`.** The library's constant of the same name is
  now the one sentence, for the reason at the top of this entry.

### Notes

- **`blocked_reason` is still `None` on RabbitMQ, confirmed against a real alarm.** The
  broker does send one — aiormq logs `was blocked by: 'low on memory'` — and keeps only
  a flag, so there is nothing to read back. 0.7.0 documents that rather than pretending
  otherwise. Nothing here should be written expecting a reason to appear, and nothing
  here invents one, because an invented reason would read exactly like one the broker
  sent.

- **The blocked path is tested against a broker that is genuinely blocked.** A test that
  only ever sees a fake block proves nothing about this: RabbitMQ sends
  `connection.blocked` only to a connection that publishes while an alarm is on, and
  what makes a probe hang is the broker then stopping reading the socket. So the
  integration suite drops `vm_memory_high_watermark` to `0.0001`, publishes to provoke
  the block, asserts the route answers 200 with the sentence above in under a second,
  and restores the watermark before the connection is closed. It skips, rather than
  pretends, where `ACEMQ_TEST_BROKER_CTL` is not set — the same lever the library's own
  suite uses.

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
