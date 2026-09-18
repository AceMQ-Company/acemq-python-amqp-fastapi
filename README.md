# acemq-python-amqp-fastapi

The FastAPI integration for AceMQ: a lifespan that owns the connection and the
consumers, `Depends` for publishing from a route, a decorator for registering handlers,
and a health check that knows a blocked broker is not a reason to restart.

It is the Python analogue of the
[Spring Boot starter](https://github.com/AceMQ-Company/acemq-java-amqp-spring-boot-starter),
and it occupies the same seam. What Spring gives a starter is `SmartLifecycle` —
something that runs after the context is built and before it is torn down. What FastAPI
gives is `lifespan`. They are the same idea, and this package lives in it.

```python
from typing import Annotated

from acemq_amqp import Ack, Message, Publisher, accept
from acemq_fastapi import AceMQ, publishes
from fastapi import Depends, FastAPI

acemq = AceMQ()


@acemq.consumer("orders")
async def handle(message: Message) -> Ack:
    await warehouse.reserve(message.payload)
    return accept()


app = FastAPI(lifespan=acemq.lifespan)
app.include_router(acemq.health_router())

Orders = Annotated[Publisher, Depends(publishes("orders"))]


@app.post("/orders")
async def place(orders: Orders) -> dict[str, str]:
    result = await orders.send({"sku": "A-1"})
    return {"id": result.message_id}
```

## Installing

```bash
pip install --extra-index-url https://acemq.org/pypi/simple/ acemq-amqp-fastapi
```

The extra index is needed because AceMQ is not on PyPI before 1.0. It brings
`acemq-amqp[rabbitmq]`, FastAPI and `pydantic-settings` with it — no second install.

Python 3.10 or newer, which is the library's floor. The dependency on the library is
`>=0.6.0,<0.7`: this repository's whole job is proving the *published* package works,
so it installs it from the index like anybody else.

## What it wires up

| | When | |
|---|---|---|
| The connection | The lifespan opens | Opened under `acemq.connection_timeout`, drained and closed on the way out |
| The codec | Always | `acemq.format`, json by default |
| The topology | `acemq.topology` declares something | Nothing declared means nothing applied, which is a real answer |
| Consumers | Every `@acemq.consumer` | Started on the application's own event loop, after the topology |
| Publishers | Per destination, on first use | Cached, and cleared with the connection |
| `app.state.acemq` | The lifespan opens | How the dependencies find it without a global |
| A health check | `acemq.health_check()` / `acemq.health_router()` | Composes into an aggregate, or answers on its own path |

Every one of them takes the connection rather than creating one, so
`AceMQ(connection_factory=...)` replaces the connection and everything else keeps
working — the same shape as the Spring starter's `@ConditionalOnMissingBean`.

## Three decisions worth knowing about

### The lifespan composes rather than replaces

FastAPI takes one lifespan, and an application that already has one should not have to
give it up:

```python
app = FastAPI(lifespan=compose_lifespans(database_pool, acemq.lifespan))
```

Left to right is outside in. **Put AceMQ last** — consumers should start after the
things their handlers use and stop before those go away.

### A blocked connection reports healthy, with the reason

A blocked connection is the broker protecting itself, usually from disk or memory
pressure. An application that fails its own readiness check for it is one an
orchestrator restarts into the same blocked broker, having thrown away whatever it was
holding. So the check reports **up**, with `parts.blocked` true and the reason in
`detail`, and 503 is reserved for a broker that did not answer at all.

The check is also bounded by `acemq.health.timeout`, which the library's own probe is
not — and a blocked broker is exactly the state in which a round trip does not come
back. A probe that hangs is a pod that never comes back.

### `publisher_confirms=false` is refused, not ignored

The Python library publishes with confirms always. A setting that silently did nothing
would leave a service believing it had traded safety for throughput and having neither,
so the setting exists only to raise with an explanation.

## What a drain finishes

Measured, on this version, against RabbitMQ 4. The full numbers are in
[docs/shutdown.md](docs/shutdown.md).

| at the moment the lifespan exits | what happens |
|---|---|
| a handler is running | it runs to completion, and its decision is carried out |
| a delivery has arrived but no handler has it yet | **it goes back to the broker**, requeued — unlike Go, which handles them all |
| a publish is waiting for its confirm | it is cut off, and raises `PublishError` naming how many were unconfirmed |
| a retry is waiting out a backoff in this process | the drain waits with it, for the whole delay |

Two of those deserve a sentence here. The second means a large prefetch costs
**redeliveries** at shutdown rather than a long drain, which is the opposite of the Go
library's trade — keep handlers idempotent and it stops mattering. The fourth means a
retry ladder without `wait_in_broker_from` set can make a drain take as long as its
longest delay, which is how a deployment starts timing out for reasons nobody connects
to the retry policy.

Bounded by `acemq.consumer.shutdown_timeout`, twenty seconds by default. When it
expires the handlers still running are cancelled and their messages left unsettled — the
broker redelivers them, so work may be done twice and nothing is dropped.

## What it is not

**This is FastAPI, not Django, and that is not an oversight.** The whole design rests
on one event loop the framework owns and hands to the lifespan: consumers are `asyncio`
tasks created on it, handlers await on it, and the drain runs on it while the server is
closing its sockets. Django's synchronous path has no such seam — `AppConfig.ready()`
runs per worker process with no loop and no shutdown hook, and a management command
that starts consumers is a second process with a different lifetime and a different
deployment. A Django integration is a different design with a different set of honest
limits. Pretending one package could be both would produce something good at neither.

**It is not a second messaging library.** Messages, acknowledgements, codecs, retry
policies, topologies, interceptors and patterns are all `acemq-amqp`'s, under their own
names. This package re-exports none of them. What `accept()` does, or where a message
that failed five times ends up, is answered by
[the library's documentation](https://acemq.org/acemq-python-amqp/).

**It does not own your `/health`.** It contributes one check. Whether that becomes the
whole readiness probe or one part of an aggregate beside your database is yours.

**It does not make a publish survive a restart.** A publish that must not be lost when
the process dies wants the library's outbox, and no lifespan is a substitute for one.

**Consumers share the process with your routes.** A handler that blocks the event loop
stalls the HTTP server as surely as it stalls the other handlers. That is the cost of
running an API and a consumer in one process, and this package does not hide it — see
[docs/consumers.md](docs/consumers.md) for the thread pool, and `acemq.start()` with no
application for the case where the handlers deserve their own process.

## Documentation

Eight pages, published at
**<https://acemq.org/acemq-python-amqp-fastapi/>**. They read as markdown in
[docs/](docs/) too, and render with `.github/scripts/build-docs-site.sh`.

| | |
|---|---|
| **Start here** | [docs/index.md](docs/index.md) · [Getting started](docs/getting-started.md) |
| **Reference** | [Configuration](docs/configuration.md) — every `acemq.*` setting |
| **Usage** | [The lifespan](docs/lifespan.md) · [Consumers](docs/consumers.md) · [Publishing from a route](docs/publishing.md) · [Testing](docs/testing.md) |
| **Operations** | [Health](docs/health.md) · [Shutdown](docs/shutdown.md) |
| **Support** | [Enterprise support](https://acemq.com) |

## Its own version line

This repository versions separately from `acemq-amqp`, starting at 0.1.0, as the Spring
Boot starter does from `acemq-java-amqp`. It tracks FastAPI and Starlette's release
train as much as AceMQ's: a FastAPI release that moves the lifespan contract is a
release this package has to answer, and it should not have to wait for the library to
cut a version to do it.

## Developing

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]" --extra-index-url https://acemq.org/pypi/simple/

pytest -m "not integration"   # everything that needs no broker
ruff check . && mypy          # what CI runs

docker run -d --rm --name acemq-test -p 5722:5672 rabbitmq:4-alpine
pytest -m integration
```

## Licence

Apache-2.0. See [LICENSE](LICENSE), and [docs/licence.md](docs/licence.md) for what the
warranty disclaimer means in practice.
