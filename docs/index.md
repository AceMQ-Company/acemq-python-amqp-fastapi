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
| **Operations** | [Health](health.md) · [Shutdown](shutdown.md) |
| **Support** | [Enterprise support](https://acemq.com) |

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
