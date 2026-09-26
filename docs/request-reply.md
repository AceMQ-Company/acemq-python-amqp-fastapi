# Request-reply

A route that asks another service a question over the broker and answers the HTTP request
with what came back. It is the one pattern where a message and an HTTP request have the
same lifetime, and that is what makes it worth its own page: the timeouts have to agree,
and a caller who has gone away is a message still in flight.

Both halves are `acemq_amqp.patterns` — a `Requester` that asks and a `serve` that
answers. Neither has an `acemq.*` setting, because both need a connection.

## Asking, from a route

A `Requester` declares its reply queue and starts consuming it, so it is built once and
shared. That makes it an inner lifespan:

```python
# app.py
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any, AsyncIterator

from acemq_amqp.patterns import RequestTimeoutError, Requester, ResponderError
from acemq_fastapi import AceMQ, compose_lifespans
from fastapi import FastAPI, HTTPException, Request

acemq = AceMQ()


@asynccontextmanager
async def prices(app: FastAPI) -> AsyncIterator[dict[str, Any]]:
    requester = await Requester.open(
        acemq.connection, routing_key="prices", timeout=timedelta(seconds=5)
    )
    try:
        yield {"prices": requester}
    finally:
        await requester.close()


app = FastAPI(lifespan=compose_lifespans(acemq.lifespan, prices))
```

`prices` goes to the right of `acemq.lifespan` — inside it — because it needs the open
connection. See [patterns](patterns.md#the-two-seams) for why that is the opposite of
where a database pool goes.

Then a dependency, so the route does not reach into `request.state` by hand:

```python
from typing import Annotated

from fastapi import Depends


def get_prices(request: Request) -> Requester:
    return request.state.prices


Prices = Annotated[Requester, Depends(get_prices)]


@app.get("/quote/{sku}")
async def quote(sku: str, prices: Prices) -> dict[str, object]:
    try:
        reply = await prices.ask({"sku": sku})
    except RequestTimeoutError:
        raise HTTPException(504, "the pricing service did not answer")
    except ResponderError as refused:
        raise HTTPException(502, str(refused))
    return {"sku": sku, "price": reply["price"]}
```

`ask` returns the reply's *payload*, decoded by the connection's codec. It raises
`RequestTimeoutError` when nothing came back in time and `ResponderError` when the
responder said it could not do it — two different failures that deserve two different
status codes, which is the argument for catching them separately rather than letting
either become a 500.

`ask(..., timeout=...)` overrides the requester's default for one call.

## The reply queue

Left unnamed, `Requester.open` generates one: transient, exclusive, auto-deleting and
classic, belonging to this process. That is what you want in an HTTP deployment. A named
reply queue that outlives its requester collects answers nobody is waiting for, and after
a deployment those answers arrive at a process that never asked the questions.

So: do not name one, and in particular do not put the reply queue name in the settings
where two replicas will share it.

## Timeouts have to agree

Three deadlines are in play and they are not the same number:

| | |
|---|---|
| The requester's `timeout` | How long `ask` waits. Five seconds above |
| The HTTP client's | How long the caller waits for the route |
| `acemq.confirm_timeout` | How long the *publish* waits for room, 30s by default |

The requester's timeout must be shorter than the caller's, or the caller gives up first
and the route finishes writing a response to a socket nobody is reading. It must also be
shorter than any proxy in front of it, for the same reason.

`acemq.confirm_timeout` is the one that surprises people. It bounds waiting for
[room to publish](publishing.md#back-pressure), not waiting for a reply — so under a
burst, `ask` can spend its whole budget before the request has even gone out. If the
p99 on `/quote` looks like the confirm timeout rather than the request timeout, that is
what is happening, and the fix is `acemq.max_outstanding_publishes` or fewer concurrent
requests, not a longer deadline.

## A cancelled request

When the HTTP client disconnects, Starlette cancels the route's task, which cancels the
`await ask(...)`. The request message has already been published: the responder will do
the work and publish a reply into a queue whose waiter is gone. The reply is dropped and
`Requester.unmatched` counts it.

That is the correct behaviour and there is nothing to configure, but it is worth knowing
two things about it. The work still happens, so a request that changes something must not
be sent this way without being idempotent on the responder's side. And `unmatched`
climbing is a signal: a few are ordinary client disconnects, a lot of them means the
timeout is too short and the requests are all being abandoned just before their answers
arrive.

`Requester.timed_out` counts the other side of the same story. Both are worth a metric.

## Answering, in the same application

`serve` subscribes a handler that publishes the reply:

```python
from acemq_amqp import Message
from acemq_amqp.patterns import serve


async def price(message: Message) -> dict[str, object]:
    return {"price": await catalogue.price_of(message.payload["sku"])}


@asynccontextmanager
async def pricing(app: FastAPI) -> AsyncIterator[dict[str, Any]]:
    responder = await serve(acemq.connection, "prices", price, concurrency=8)
    try:
        yield {"pricing": responder}
    finally:
        await responder.close()
```

A `Responder` is `Callable[[Message], Any]` — it returns the reply payload rather than an
`Ack`, which is the difference from an ordinary handler. Raising means the requester gets
a `ResponderError`.

`serve` passes anything else through to `Connection.consume`, so `concurrency`, `prefetch`
and `declare` all work. `concurrency` is the one to set: a responder answering one request
at a time is a queue of HTTP requests waiting on each other in a different process.

`ResponderHandle` carries `answered` and `unanswerable` counters, and `in_flight`,
`running` and `closed` like any consumer.

**`serve` is not registered through `@acemq.consumer`**, so — like a
[stream reader](streams.md#the-drain-does-not-know-about-it) — it is not in
`acemq.consumers`, `AceMQ.drain()` does not stop it, and the health check does not see it.
The `finally` above is what closes it, and it runs before `acemq.lifespan` closes the
connection because of the ordering.

## Or don't

Request-reply over a broker puts two extra hops and another service's availability
inside an HTTP request that could have made one call. It earns its place when the
responder is already there for other reasons, when the broker is the only route to it, or
when the request has to queue behind a bounded number of workers. It does not earn its
place as a general remote-call mechanism, and an HTTP client is a shorter answer than this
page.

## Then

- [Patterns](patterns.md) — the rest, and the two seams
- [Publishing from a route](publishing.md) — fire-and-forget, batches, back pressure
- [The library's request-reply page](https://acemq.org/acemq-python-amqp/request-reply.html)
