# Publishing from a route

```python
from typing import Annotated

from acemq_amqp import Publisher
from acemq_fastapi import publishes
from fastapi import Depends

Orders = Annotated[Publisher, Depends(publishes("orders"))]


@app.post("/orders")
async def place(order: Order, orders: Orders) -> dict[str, str]:
    result = await orders.send(order.model_dump())
    return {"id": result.message_id}
```

`publishes(routing_key, exchange="")` builds a dependency for one destination. The
routing key comes first because publishing straight to a queue is the common case: with
no exchange, the broker's default exchange routes to the queue whose name matches the
key. For a topic exchange, name both:

```python
Events = Annotated[Publisher, Depends(publishes("order.placed", "events"))]
```

## Put the alias at module scope

With `from __future__ import annotations` — or on any Python where a route's
annotations are strings — FastAPI resolves them against the *module's* globals. An
`Annotated` alias defined inside a function cannot be found, and the symptom is not an
error: the route decides the parameter is a request body and answers 422 to every
request. Define the aliases beside the imports.

## What the dependency buys over a global

A publisher held in a module-level variable has to be created after the connection is
open and thrown away when it closes, and the code that does both is the code nobody
writes. The dependency resolves per request, after startup, from
`request.app.state.acemq` — so it is impossible to hold one from the previous
deployment, and two applications in one process get their own.

Publishers are cached by destination, so a route called a thousand times a second does
not build a thousand publishers. They are cheap to keep and safe to share; the cache
is cleared when the lifespan closes.

## The connection, for what a publisher does not do

```python
from acemq_fastapi import AceMQConnection

@app.get("/queues/{name}/depth")
async def depth(name: str, connection: AceMQConnection) -> dict[str, int]:
    return {"messages": await connection.message_count(name)}
```

Reading a queue depth, pulling one message, declaring something on the fly — the things
`Publisher` deliberately does not do.

## Batches

```python
@app.post("/orders/bulk")
async def bulk(orders_in: list[Order], orders: Orders) -> dict[str, int]:
    results = await orders.send_all(o.model_dump() for o in orders_in)
    return {"confirmed": sum(1 for r in results if r.confirmed)}
```

`send_all` — added in `acemq-amqp` 0.6.0 — sends every message before awaiting any
confirm, then checks all of them together. Awaiting each `send` in turn is a broker
round trip per message, which is the loop a caller could have written and none of the
throughput a batch exists for.

It raises `PublishError` when any message was not confirmed, which includes the case
where the connection closed underneath it. See [shutdown](shutdown.md).

## Back pressure

`acemq.max_outstanding_publishes` bounds how many publishes may be waiting for the
broker at once — a thousand by default. The thousand-and-first waits for room rather
than queueing in memory, and gives up after `acemq.confirm_timeout`.

That bound matters more in an HTTP process than in a worker. A burst of requests is a
burst of publishes, and without it a slow broker turns into unbounded memory growth
behind a server that is still accepting connections. With it, the requests block on a
publisher that is waiting — which is back pressure reaching the client, which is what
you want.

## Interceptors

Anything the library can wrap a publish in, this can register at startup:

```python
acemq = AceMQ(on_publish=(stamp_the_tenant,), on_consume=(trace_the_handler,))
```

They are registered before any consumer subscribes, so no message goes out or comes in
without them. See
[interceptors](https://acemq.org/acemq-python-amqp/interceptors.html).
