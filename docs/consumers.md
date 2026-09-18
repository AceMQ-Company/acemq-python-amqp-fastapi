# Consumers

A decorator over an async function is the Python analogue of the Spring starter's
`@AceListener`:

```python
from acemq_amqp import Ack, Message, accept, reject, retry

@acemq.consumer("orders")
async def handle(message: Message) -> Ack:
    order = message.payload
    if not order.get("sku"):
        return reject("no sku")
    await warehouse.reserve(order)
    return accept()
```

The handler takes a `Message` and returns an `Ack`. Both are the library's — this
package adds no message type of its own, and
[consuming](https://acemq.org/acemq-python-amqp/consuming.html) is where the four
decisions are explained.

## Nothing runs at import time

The decorator records a registration. Consumers are subscribed when the lifespan opens,
and that is what puts them on the application's event loop rather than on whichever
loop happened to import the module. There is a test whose only job is to assert that
the loop a handler runs on is the application's.

It also means the decorated function is returned **unchanged**. It is still an ordinary
async function that a test can call with a `Message` and assert on, with no broker, no
lifespan and no event loop belonging to anybody:

```python
async def test_rejects_an_order_with_no_sku() -> None:
    assert await handle(a_message({"qty": 1})) == reject("no sku")
```

Registering after the lifespan has opened is refused, with a message saying so. The
decorator belongs at module scope.

## Per-consumer settings

```python
@acemq.consumer(
    "orders",
    name="orders-main",     # what it is called in acemq.consumers; defaults to the queue
    concurrency=8,          # messages at once; 1 keeps the queue in order
    prefetch=32,            # unacknowledged messages held
    retry=exponential_retry(5, timedelta(seconds=2)),
    codec=my_codec,
    declare=False,          # for a login with no configure permission
    tag="orders-web-1",     # what the broker calls this consumer
)
async def handle(message: Message) -> Ack: ...
```

Anything left out takes the value from `acemq.consumer.*` — and takes it when the
lifespan opens, not when the decorator runs, so settings read from the environment
still reach a consumer registered at import time.

Two consumers on one queue is a reasonable thing to want; give one of them a `name=`,
because the name is how a consumer is addressed and it has to be unique.

## Scaling, pausing and counters

`acemq.consumers` is the running consumers by name. It is the same registry the Spring
starter exposes as `AceListenerRegistry`, and it is what a route uses to pause a
consumer under back pressure:

```python
@app.post("/admin/consumers/{name}/pause")
async def pause(name: str, acemq: AceMQDep) -> dict[str, str]:
    await acemq.stop_consumer(name)     # drains it: handlers in flight still finish
    return {"stopped": name}

@app.post("/admin/consumers/{name}/resume")
async def resume(name: str, acemq: AceMQDep) -> dict[str, str]:
    await acemq.start_consumer(name)
    return {"started": name}
```

`stop_consumer` drains rather than kills: the handlers it is holding finish and their
messages are settled, exactly as at shutdown. The registration stays, so
`start_consumer` brings it back.

Set `acemq.consumer.auto_start=false` to have the lifespan register everything and
start nothing — for a process that serves HTTP and should not also consume, deployed
from the same image as the one that should.

Counters come straight off the library's consumer:

```python
acemq.consumers["orders-main"].in_flight   # messages being worked on right now
acemq.consumers["orders-main"].running     # subscribed, with workers alive
```

`running` is what the health check reads: a consumer whose workers have died without it
being closed is one the broker is still sending messages to and nothing is reading,
which from outside is indistinguishable from a quiet queue.

## Concurrency, and what it costs

`concurrency=1` is the default and keeps a queue's messages in order. Raising it trades
that order for throughput, which is the right trade for handlers that spend their time
awaiting something else — a database, an HTTP call — and the wrong one for a handler
that must see a key's messages in the order they were sent.

In a FastAPI process the consumers share the event loop with the routes. A handler that
blocks the loop — a synchronous database driver, a CPU-bound parse — stalls the HTTP
server as surely as it stalls the other handlers. Put it in a thread:

```python
from starlette.concurrency import run_in_threadpool

@acemq.consumer("reports")
async def handle(message: Message) -> Ack:
    await run_in_threadpool(render_the_pdf, message.payload)
    return accept()
```

This is the cost of sharing a process between an API and a consumer, and it is worth
being explicit that this package does not hide it. If the handlers are heavy enough to
need their own machine, run them in their own process — `acemq.start()` with no app,
from [the lifespan page](lifespan.md), is that process.

## Retries

The ladder is off until it is asked for, matching the Spring starter: a retry policy
declares rung queues on the broker, and a service should say it wants them.

```bash
ACEMQ_CONSUMER__RETRY__ENABLED=true
ACEMQ_CONSUMER__RETRY__MAX_ATTEMPTS=5
ACEMQ_CONSUMER__RETRY__INITIAL_DELAY=2
ACEMQ_CONSUMER__RETRY__MAX_DELAY=300
ACEMQ_CONSUMER__RETRY__WAIT_IN_BROKER_FROM=10
```

`wait_in_broker_from` is the setting to think about, and the reason is in
[shutdown](shutdown.md): a wait held in this process is a wait the drain has to sit
through. Ten seconds is a good threshold. The rung queues those waits need are declared
with the topology when `acemq.topology.declare_retry_rungs` is on, which it is.
