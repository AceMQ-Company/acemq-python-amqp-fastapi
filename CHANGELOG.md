# Changelog

All notable changes to this project are documented here.

The format is [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This repository has a version line of its own, separate from `acemq-amqp`, as the
Spring Boot starter has one separate from `acemq-java-amqp`. The reason is the same: it
tracks FastAPI and Starlette's release train as much as AceMQ's. A FastAPI release that
moves the lifespan contract is a release this package has to answer, and waiting for the
library to cut a version to do it would put the two on a schedule neither of them
chose. How a release is cut is in [RELEASING.md](RELEASING.md).

## [Unreleased]

## [0.1.1] - 2026-09-26

### Added

Seven documentation pages, which take the site from ten to seventeen. The gap they close
is the same one in every case: the library promotes a set of patterns, and nothing here
said where any of them goes in a FastAPI application.

- **[Patterns from FastAPI](docs/patterns.md)** is the hub, and the table at the top of it
  is the part worth having. For every pattern the library carries it says whether there is
  an `acemq.*` setting or whether it is four lines in a lifespan — and for most of them it
  is the latter. That is stated rather than worked around: a pattern needing a database
  handle, a key or a callable cannot be described by an environment variable, and a setting
  that pretended otherwise would be worse than the four lines. The page then names the two
  seams — `connection_factory` for anything on the connection itself, a lifespan composed
  *inside* this one for anything that needs the connection open — and works through the
  idempotent consumer, middleware pipelines, ordering by key, the outbox, the claim check,
  the saga, routing slips, the scheduler, replay and consumer groups.
- **[Security](docs/security.md)**. TLS and why `acemq.tls.*` is only consulted for
  `amqps://`, why naming a certificate authority replaces the machine's trust store rather
  than adding to it, mutual TLS, `nothing-at-all` and what it costs, development
  certificates and the marker every AceMQ library refuses, and encrypting the payload with
  `EncryptedCodec` in the three places it can be wired. Also two things the package does
  *not* do: `credentials_from_file` is not reachable through a setting, and the health
  route carries no authentication.
- **[Streams](docs/streams.md)**. A stream is declarable through `acemq.topology.queues`
  via `args`, because the library's `Topology.queue` keeps a kind that `args` already names
  — but `@acemq.consumer` is the wrong tool for reading one, and the page says so plainly
  rather than showing a registration that half works. `read_stream` forces `no_retry()` and
  refuses a handler that asks for a retry, because retrying on a stream appends a second
  copy to the log; a registration through the decorator would get the offset and none of
  that. So a reader is hand-wired, and the page is explicit that `AceMQ.drain()` does not
  know about it.
- **[Request-reply](docs/request-reply.md)**. A page of its own because it is the one
  pattern where a message and an HTTP request have the same lifetime: three deadlines that
  have to agree, why `acemq.confirm_timeout` shows up in the p99 of a route that never
  mentions it, and what a cancelled request leaves behind.
- **[Serialization](docs/serialization.md)**. What `acemq.format` can and cannot name —
  Protobuf and Avro cannot, because neither format's bytes describe themselves — the
  `register_codec` bridge that gives a constructed codec a name anyway, and the two
  pydantic details that bite: `model_dump(mode="json")` on the way out and
  `model_validate` at the top of a handler on the way in.
- **[Topology](docs/topology.md)**. What the lifespan applies and when, why a durable queue
  is a quorum queue, and an honest section on drift: AMQP cannot enumerate what is there,
  so `Topology.plan()` is a statement of intent and the thing that actually catches drift
  is the broker's `PRECONDITION_FAILED` at start-up.
- **[Observability](docs/observability.md)**. A `/metrics` route from `prometheus_text` with
  nothing installed, or from `PrometheusObserver` for a service that already has a
  registry; the OpenTelemetry interceptors as constructor arguments; every metric the
  library writes and which one to alert on; and why a blocked broker is the failure mode
  this stack hides best.

### Changed

- The version tables in [the overview](docs/index.md#versions) and
  [getting started](docs/getting-started.md) now name **`acemq-amqp` 0.7.1** as the release
  this is tested against, alongside the declared range. The range is unchanged and
  deliberately so: `>=0.7.0,<0.8` names a set, and rewriting it on every 0.7.z release
  would be an exact pin with extra punctuation — and a lie to every application that
  inherits it as a constraint.
- Getting started explains `--extra-index-url` rather than `--index-url` (the latter
  replaces PyPI instead of adding to it, and the install then fails on FastAPI), and shows
  the `requirements.txt` and `pyproject.toml` forms.
- The lifespan page gains *Except for what needs the connection*. "Put AceMQ last" is about
  the things handlers use; something that uses the connection has the opposite requirement,
  and the two pages now agree instead of one of them being silently incomplete.

### Fixed

- `docs/testing.md` built a message with `Envelope.new()`, which does not exist —
  `Envelope()` needs no arguments. The same example compared a handler's answer against
  `reject("no sku")`, which fails whatever the handler does: `Ack` carries the exception,
  and two exceptions built from the same message are not equal. It asserts on
  `decision.action` now, and says why.
- `reject`, `retry` and `park` take a `BaseException`, not a string. Two examples passed a
  string, which works at runtime and is a type error — and loses the reason that would
  otherwise be on the dead-lettered message.
- The module docstrings of `acemq_fastapi` and `acemq_fastapi.dependencies` read
  `result.envelope.id` off a publish. `PublishResult` has no `envelope`; the field is
  `message_id`, which is what every other example already used.
- `tests/test_documented_wiring.py` asserts the recipes the new pages tell people to write:
  an inner lifespan seeing an open connection either side of its `yield`,
  `auto_start=false` with `start_consumer`, `add_consumer` with a wrapped handler,
  `register_codec` reaching `acemq.format`, an unknown format failing the *start* rather
  than the first publish, and a stream declared through the topology setting. None of it is
  new API, which is the point: a documented recipe with nothing asserting it goes stale in
  the same silence as a comment.

## [0.1.0] - 2026-09-20

First release. It wires **`acemq-amqp` 0.7.0** into a FastAPI application and does
nothing else.

The dependency is pinned **`acemq-amqp[rabbitmq]>=0.7.0,<0.8`**. The floor is 0.7.0
because the health check reads `Connection.blocked` and takes its blocked-connection
wording from the library's `BLOCKED_DETAIL`, neither of which exists before it: a
resolver allowed to pick 0.6.0 would install something that imports cleanly and raises
`AttributeError` the first time a readiness probe fires. The ceiling stays one minor
ahead, because a pre-1.0 library puts its breaking changes in minor bumps, and moving it
should be a decision somebody made. `[rabbitmq]` because a FastAPI integration that
cannot reach a broker without a second install is an integration that does not
integrate.

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
  application's own, and a router that answers on `acemq.health.path`. See below for
  what it reports and what it deliberately does not.
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
- **Documentation** — ten pages under `docs/`, rendered to a site by
  `.github/scripts/build-docs-site.sh` with the API reference generated from the
  docstrings.
- **69 tests.** Sixty run against a transport that is a dictionary; the nine that are
  about the wire are marked `integration` and run against a real broker in CI.

### What the health check reports

Worth its own section, because it is the part an operator writes an alert rule against
and the part where this package deliberately does not answer the obvious way.

- **A blocked connection reports *up*, with 200**, matching the Spring starter.
  Restarting into a broker that is still blocked helps nobody, and the instance is still
  serving. The reason is in the payload: `parts.blocked` is `true`, and `detail` reads

  > the broker has blocked this connection; publishing is paused

  Word for word what Go, Ruby and the library itself write, because it comes from the
  library's `BLOCKED_DETAIL` rather than from a wording of this package's own. One alert
  rule then reads a blocked broker whatever language the service behind it is in.

  **Match `parts.blocked == true` rather than the sentence.** A rule that must read the
  text should match the *prefix* `the broker has blocked this connection`, because a
  broker reason is appended after a colon where a client keeps one, and because a
  stalled consumer during a block puts its own clause in front of it.

- **`parts.blocked` is three-state: `true`, `false`, or `null`.** `null` means the
  question could not be asked, and is not folded into `false` — "nobody looked" and "not
  blocked" are different facts. It is read from the library's `Connection.blocked`.

- **`blocked_reason` is `None` on RabbitMQ**, confirmed against a real alarm rather than
  assumed. The broker does send one — aiormq logs `was blocked by: 'low on memory'` —
  and keeps only a flag, so there is nothing to read back. Nothing here invents one,
  because an invented reason would read exactly like one the broker sent.

- **A blocked connection whose consumer has also stopped reading reports *degraded*,**
  with the consumer's failure first: that one is this instance's own and outlives the
  alarm. Still 200.

- **503 is reserved for a broker that did not answer at all.** Degraded answers 200 too.

- **`acemq.health.timeout` is the library probe's deadline**, handed to it rather than
  wrapped around it as an `asyncio.wait_for`. Two deadlines are worse than one: the
  outer one is the cruder, and when a blocked broker stops answering the library
  recognises that and reports *up* where a wrapper firing first would report a spurious
  *down*. It would also cancel a request the library deliberately abandons — a broker
  that is not reading cannot be relied on to process a cancellation either.

- **The route is mounted explicitly.** A route appearing in an application's OpenAPI
  document without anybody adding it is a route somebody has to go looking for.

The argument for reporting blocked as up lives in
[docs/health.md](docs/health.md), where it does not have to be read at three in the
morning.

### Decisions

- **`pydantic-settings` is a dependency, not an extra.** FastAPI already brings
  pydantic, so the marginal cost is one small package that reads environment variables
  into a model. Writing a parser here for durations, nested keys and `.env` files would
  be a worse version of it that this repository would own forever. The Spring starter
  does not write its own binder either; it uses Spring's.
- **`acemq.publisher_confirms=false` raises rather than being ignored.** The Python
  library publishes with confirms always. A setting that silently did nothing would
  leave a service believing it had traded safety for throughput and having neither.
- **Nothing from `acemq_amqp` is re-exported.** Messages, acknowledgements, codecs and
  retry policies keep the library's names, so an import says which half of the system a
  line is talking about.
- **The blocked path is tested against a broker that is genuinely blocked.** A test that
  only ever sees a fake block proves nothing: RabbitMQ sends `connection.blocked` only
  to a connection that publishes while an alarm is on, and what makes a probe hang is
  the broker then stopping reading the socket. So the integration suite drops
  `vm_memory_high_watermark` to `0.0001`, publishes to provoke the block, asserts the
  route answers 200 with the sentence above in under a second, and restores the
  watermark before the connection is closed. It skips, rather than pretends, where
  `ACEMQ_TEST_BROKER_CTL` is not set — the same lever the library's own suite uses, and
  a release refuses to publish if it skipped.
- **Dependencies are installed from the index, never from a sibling checkout.** CI and
  the release both resolve `acemq-amqp` from <https://acemq.org/pypi/simple/> the way a
  consumer would. This repository's whole point is proving the published library works
  behind FastAPI, and a local path would prove something else.

### Known limits

- **Handlers share the event loop with the routes.** A handler that blocks it stalls the
  HTTP server. This package does not hide that; [docs/consumers.md](docs/consumers.md)
  says what to do about it, and `acemq.start()` with no application covers the case
  where the handlers deserve their own process.
- **A publish does not survive a restart.** A publish that must not be lost when the
  process dies wants the library's outbox, and no lifespan is a substitute for one.
- **FastAPI only.** Nothing here is reusable in a WSGI application, and
  [docs/index.md](docs/index.md) explains why a Django integration is a different design
  rather than a missing feature.

[Unreleased]: https://github.com/AceMQ-Company/acemq-python-amqp-fastapi/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/AceMQ-Company/acemq-python-amqp-fastapi/releases/tag/v0.1.0
