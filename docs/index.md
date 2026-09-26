# AceMQ for FastAPI

`acemq-amqp-fastapi` wires [`acemq-amqp`](https://acemq.org/acemq-python-amqp/) into a
FastAPI application: a lifespan that owns the connection and the consumers, `Depends`
for publishing from a route, a decorator for registering handlers, and a health check
that knows a blocked broker is not a reason to restart.

It is the Python analogue of the
[Spring Boot starter](https://acemq.org/acemq-java-amqp-spring-boot-starter/), and it
occupies the same seam. What Spring gives a starter is `SmartLifecycle` — something
that runs after the context is built and before it is torn down. What FastAPI gives is
`lifespan`. They are the same idea and this package lives in it.

```python
from acemq_amqp import Ack, Message, accept
from acemq_fastapi import AceMQ, publishes
from acemq_amqp import Publisher
from fastapi import Depends, FastAPI
from typing import Annotated

acemq = AceMQ()


@acemq.consumer("orders")
async def handle(message: Message) -> Ack:
    print(message.payload)
    return accept()


app = FastAPI(lifespan=acemq.lifespan)
app.include_router(acemq.health_router())

Orders = Annotated[Publisher, Depends(publishes("orders"))]


@app.post("/orders")
async def place(orders: Orders) -> dict[str, str]:
    result = await orders.send({"sku": "A-1"})
    return {"id": result.message_id}
```

## Where to go next

| | |
|---|---|
| **Start here** | [Getting started](getting-started.md) |
| **Reference** | [Configuration](configuration.md) — every `acemq.*` setting |
| **Usage** | [The lifespan](lifespan.md) · [Consumers](consumers.md) · [Publishing from a route](publishing.md) · [Testing](testing.md) |
| **Patterns** | [Patterns from FastAPI](patterns.md) · [Request-reply](request-reply.md) · [Streams](streams.md) · [Serialization](serialization.md) |
| **Operations** | [Security](security.md) · [Topology](topology.md) · [Health](health.md) · [Observability](observability.md) · [Shutdown](shutdown.md) |
| **Support** | [Enterprise support](https://acemq.com) |

[Patterns from FastAPI](patterns.md) is the page to read second. It lists every pattern the
library carries and says, for each one, whether there is an `acemq.*` setting for it or
whether it is four lines in a lifespan — because for most of them it is the latter, and
knowing which is which saves looking for a setting that was never going to exist.

## Versions

This release wires **`acemq-amqp` 0.7.1**, which is the current library release. The
declared dependency is a range, `acemq-amqp[rabbitmq]>=0.7.0,<0.8`, so pip resolves the
newest release inside it rather than a version pinned here going stale — and the range is
also the constraint your application inherits.

| | |
|---|---|
| `acemq-amqp-fastapi` | 0.1.0 |
| `acemq-amqp` | `>=0.7.0,<0.8`, tested against 0.7.1 |
| Python | 3.10, 3.11, 3.12, 3.13 |
| FastAPI | `>=0.110` |
| `pydantic-settings` | `>=2.2` |

The floor is 0.7.0 because the health check calls `Connection.blocked`, which 0.6.0 does
not have. The ceiling is one minor ahead, because a pre-1.0 library puts its breaking
changes in minor bumps. This package versions separately from the library and starts at
0.1.0, as the Spring Boot starter does from `acemq-java-amqp`: it tracks FastAPI and
Starlette's release train as much as AceMQ's.

## What it is not

**This is FastAPI, not Django.** Nothing here is reusable in a WSGI application, and
that is not an oversight. The whole design rests on one event loop that the framework
owns and hands to the lifespan: consumers are `asyncio` tasks created on it, handlers
await on it, and the drain runs on it while the server is shutting the sockets. Django
has no such seam in its synchronous path — an `AppConfig.ready()` runs per worker
process with no loop and no shutdown hook, and a management command that starts
consumers is a second process with a different lifetime. A Django integration is a
different design with a different set of honest limits, and pretending one package
could be both would produce something that was good at neither.

**It is not a second messaging library.** Every message, acknowledgement, codec, retry
policy and topology is the library's, under the library's own names. This package owns
the wiring and nothing else: if you want to know what `accept()` does or what happens
to a message that fails five times, the answer is in
[the library's documentation](https://acemq.org/acemq-python-amqp/) and not here.

**It does not own your `/health`.** It contributes one check. Whether that becomes the
whole readiness probe, or one part of an aggregate beside your database, is yours to
decide — see [health](health.md).

**It does not survive a restart on your behalf.** A publish that must not be lost when
the process dies wants the library's
[outbox](https://acemq.org/acemq-python-amqp/patterns.html), and no lifespan is a
substitute for it.
