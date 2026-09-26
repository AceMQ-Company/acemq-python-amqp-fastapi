# Patterns from FastAPI

`acemq-amqp` carries the messaging patterns a service ends up needing — an idempotent
consumer, a transactional outbox, a saga, a claim check, request-reply, a scheduler, a
replay. None of them is reimplemented here. What this page is for is the question that
comes next: **where does it go in a FastAPI application, and is there a setting for it?**

For most of them the answer is that there is no setting, and that is the honest answer
rather than a gap. A pattern that needs a database handle, a key, a store or a callable
cannot be described by an environment variable, and a setting that pretended otherwise
would be a worse version of writing four lines in a lifespan.

## What is reachable through `acemq.*`, and what is not

| Pattern | How it is reached here |
|---|---|
| Publish, consume | `publishes(...)` and `@acemq.consumer` — [publishing](publishing.md), [consumers](consumers.md) |
| Retries, dead letters, parking | `acemq.consumer.retry.*` and `dead_letter: true` — [consumers](consumers.md#retries) |
| Topology, and drift | `acemq.topology.*` — [topology](topology.md) |
| Serialization | `acemq.format`, or `codec=` per consumer and publisher — [serialization](serialization.md) |
| TLS, credentials, development certificates | `acemq.tls.*`, `acemq.username`/`password` — [security](security.md) |
| Payload encryption | No setting. A codec — [security](security.md#encrypting-the-payload) |
| Health | `acemq.health.*` — [health](health.md) |
| Back pressure, blocked connections | `acemq.max_outstanding_publishes`, `acemq.confirm_timeout` — [publishing](publishing.md#back-pressure), [observability](observability.md#a-blocked-broker) |
| Interceptors | `AceMQ(on_publish=..., on_consume=...)` — constructor, no setting |
| Metrics, tracing | `AceMQ(observer=...)` and interceptors — [observability](observability.md) |
| Idempotent consumer | No setting. Wrap the handler — [below](#idempotent-consumer) |
| Pipelines, middleware | No setting. Wrap the handler — [below](#pipelines-and-middleware) |
| Ordering by key | No setting. Wrap the handler — [below](#ordering-by-key) |
| Consumer groups | `concurrency=` on a registration, or the pattern — [below](#consumer-groups) |
| Transactional outbox | No setting. A store, and a relay in a lifespan — [below](#transactional-outbox) |
| Request-reply | No setting. A `Requester` in a lifespan — [request-reply](request-reply.md) |
| Scheduling | No setting. A `Scheduler` in a lifespan — [below](#scheduling) |
| Streams | `acemq.topology`, then a reader in a lifespan — [streams](streams.md) |
| Claim check | No setting. A codec — [below](#claim-check) |
| Saga | No setting. A plain object, no connection needed — [below](#saga) |
| Routing slip | No setting. A handler built by `follow_slip` — [below](#routing-slip) |
| Replay | No setting. A call from a route — [below](#replay) |
| Schema evolution | No setting. A registry — [serialization](serialization.md#schema-evolution) |

## The two seams

Everything in the second half of that table goes in one of two places, and which one
depends on a single question: does it have to exist **before the consumers subscribe**?

### `connection_factory`, for anything on the connection itself

`AceMQ.start()` calls the factory, then registers the interceptors, then applies the
topology, then subscribes the consumers. So anything that changes what the connection
*is* — its codec, its security, a transport option — belongs in the factory, because by
the time a lifespan runs the consumers are already reading.

```python
async def open_connection(settings: AceMQSettings) -> Connection:
    return await acemq_amqp.connect(
        settings.broker_url,
        codec=ClaimCheckCodec(settings.build_codec(), store),
        origin=settings.client_name,
        retry=settings.build_retry(),
        prefetch=settings.prefetch,
        max_outstanding_publishes=settings.max_outstanding_publishes,
        confirm_timeout=settings.confirm_timeout,
        security=settings.build_security(),
    )


acemq = AceMQ(connection_factory=open_connection)
```

Copy the argument list rather than writing a short one: every argument left out is a
setting silently dropped, and the symptom is a `confirm_timeout` that is thirty seconds
in the configuration and sixty in the process.

### A lifespan composed *inside*, for anything that needs the open connection

An `OutboxRelay`, a `Requester`, a `Scheduler`, a stream reader — each of these takes a
connection, so none of them can be built before there is one.

```python
@asynccontextmanager
async def relay(app: FastAPI) -> AsyncIterator[dict[str, Any]]:
    running = OutboxRelay(acemq.connection, store)
    running.start()
    try:
        yield {"relay": running}
    finally:
        await running.close()


app = FastAPI(lifespan=compose_lifespans(acemq.lifespan, relay))
```

`acemq.lifespan` comes **first**, which is outermost, which means it opens first and
closes last. That is the opposite of the ordering on
[the lifespan page](lifespan.md#keeping-the-lifespan-you-already-have), and the
distinction is worth holding on to:

- Something the **handlers** use — a database pool, an HTTP client — goes *outside*
  `acemq.lifespan`, so it is open before any handler runs and closed after the last one.
- Something that uses the **connection** goes *inside*, so the connection is open before
  it is built and still open while it closes.

Write the cleanup in `try`/`finally`. A line after the `yield` never runs when a later
lifespan fails on the way up.

### When the handlers need the thing that needs the connection

The two rules above collide exactly once: a handler that asks another service a question
needs a `Requester`, and a `Requester` needs the connection. Nothing can be on both sides
of the same `yield`.

The way through is `acemq.consumer.auto_start=false`, which leaves every consumer
registered and stopped, and starting them at the end of the inner lifespan:

```bash
export ACEMQ_CONSUMER__AUTO_START=false
```

```python
@asynccontextmanager
async def wiring(app: FastAPI) -> AsyncIterator[dict[str, Any]]:
    asked = await Requester.open(acemq.connection, routing_key="prices")
    holder.requester = asked          # what the handlers read
    try:
        for name in ("orders", "refunds"):
            await acemq.start_consumer(name)
        yield {"requester": asked}
    finally:
        await asked.close()


app = FastAPI(lifespan=compose_lifespans(acemq.lifespan, wiring))
```

`start_consumer` subscribes one registration by name and `stop_consumer` drains it, so
this is the same mechanism [consumers](consumers.md#scaling-pausing-and-counters) uses
for pausing under back pressure. The consumers started this way *are* in
`acemq.consumers`, so the drain and the health check see them normally.

`holder` is a small mutable object at module scope. It looks like a global because it is
one; the alternative is handlers resolving `request.app.state` when there is no request,
which is worse. Keep it to the things that genuinely cannot be resolved per call.

## Idempotent consumer

The broker guarantees at-least-once. A handler that charges a card has to make the second
delivery of the same message do nothing, and that is a store the handler consults rather
than anything the transport can do.

```python
import sqlite3

from acemq_amqp import Ack, Message, accept
from acemq_amqp.patterns import SqlIdempotencyStore, idempotent


def connections() -> sqlite3.Connection:
    """How the store gets a database connection: a callable, called per use."""
    return sqlite3.connect("charges.db")


store = SqlIdempotencyStore(connections)


async def charge(message: Message) -> Ack:
    await payments.charge(message.payload)
    return accept()


acemq.add_consumer(
    "charges",
    idempotent(store, charge, key=lambda m: m.payload["payment"]),
    name="charges",
)
```

`add_consumer` rather than the decorator, because the thing being registered is
`idempotent(store, charge, ...)` and not `charge`. The decorator returns the function it
decorated unchanged — which is what keeps a handler testable — so there is nowhere for it
to put a wrapper. `add_consumer` takes the wrapped callable directly and is otherwise the
same registration with the same arguments.

`key` defaults to the envelope's message id, which is right when the producer is another
AceMQ service and a redelivery carries the same id. Naming a business key instead — the
payment reference above — also catches the case where the *producer* published twice.

`SqlIdempotencyStore` imports no driver: it is written against the DB-API 2.0 protocols,
so `sqlite3` from the standard library works with nothing installed and `psycopg` works
if you have it. `acemq_amqp.patterns.create_schema(connections)` writes the tables, for
development; a real deployment gets them from a migration, and
`acemq_amqp.patterns.schema_ddl()` returns the statements to paste into one.
`InMemoryIdempotencyStore` exists and is for tests — it forgets on restart, which is the
one thing this pattern must not do.

## Pipelines and middleware

`chain` composes middleware around a handler, in the same shape and for the same reasons
as ASGI middleware around a route:

```python
from datetime import timedelta

from acemq_amqp.patterns import chain, with_idempotency, with_logging, with_timeout

acemq.add_consumer(
    "charges",
    chain(
        charge,
        with_timeout(timedelta(seconds=30)),
        with_logging(),
        with_idempotency(store, key=lambda m: m.payload["payment"]),
    ),
    name="charges",
)
```

Leftmost is outermost, so the timeout bounds everything inside it including the
idempotency check. `with_timeout` is the one worth adding by default: a handler that
hangs holds a prefetch slot and a drain has to wait for it, and thirty seconds of nothing
is a better failure than an hour.

`with_ordering`, `with_idempotency`, `with_logging` and `with_timeout` are what the
library ships. A middleware is `Callable[[Handler], AsyncHandler]`, so your own is an
ordinary function.

## Ordering by key

`concurrency=1` keeps a queue in order at the cost of throughput. `with_ordering` keeps
each *key* in order and lets unrelated keys run at once:

```python
from acemq_amqp.patterns import by_header, chain, with_ordering

acemq.add_consumer(
    "accounts",
    chain(apply_entry, with_ordering(by_header("x-account"))),
    name="accounts",
    concurrency=8,
)
```

`by_correlation()` partitions on the envelope's correlation id instead, and a
`PartitionKey` is any `Callable[[Message], str]`.

This orders within one process. Two replicas reading the same queue both hold a key's
messages in their own order and nothing coordinates them — for that the key has to reach
the routing, which is what `partitioned_routing_key(base, key, partitions)` and one queue
per partition are for.

## Transactional outbox

A handler that writes to a database and then publishes has two writes and no transaction
across them. The outbox makes it one: the message is written to a table in the same
transaction as the business change, and a relay publishes from that table afterwards.

```python
from acemq_amqp.patterns import OutboxRelay, SqlOutboxStore, record

outbox = SqlOutboxStore(connections)
```

Recording, from a route, inside whatever transaction the route already has:

```python
@app.post("/orders")
async def place(order: Order, connection: AceMQConnection) -> dict[str, str]:
    entry = record(connection, "", "orders", order.model_dump())
    with transaction() as db:
        db.execute("insert into orders ...", ...)
        await outbox.add(entry, connection=db)
    return {"id": entry.id}
```

`record(connection, exchange, routing_key, payload)` encodes the payload with the
connection's codec and returns an `OutboxRecord`; it publishes nothing. `add` takes a
`connection=` so the insert lands in the caller's transaction rather than a second one —
which is the entire point of the pattern, and the argument to get right.

The relay is the inner lifespan from above:

```python
@asynccontextmanager
async def relay(app: FastAPI) -> AsyncIterator[dict[str, Any]]:
    running = OutboxRelay(acemq.connection, outbox)
    running.start()
    try:
        yield {"relay": running}
    finally:
        await running.close()
```

`start()` runs a sweep every second in a task of its own; `sweep()` runs one and returns
how many it published, which is what a test calls instead of waiting. `OutboxRelay(...,
interval=..., batch=...)` changes the two numbers.

**One relay, not one per replica.** Three HTTP processes each sweeping the same table
publish the same rows three times — `mark_published` makes the window small, not zero.
Either run the relay in a single process (`AceMQ.start()` with no app, from
[the lifespan page](lifespan.md#without-an-asgi-application-at-all)) or elect a leader.
An HTTP deployment scaled by a replica count is the wrong home for it, and that is worth
knowing before rather than after.

## Claim check

A large payload does not belong in a broker. The claim check puts the body in a store and
sends the key, transparently, as a codec:

```python
from acemq_amqp.patterns import ClaimCheckCodec, FilesystemClaimCheckStore

store = FilesystemClaimCheckStore("/var/lib/orders/claims")
```

It wraps another codec and only intervenes above a threshold — 64 KiB by default — so
small messages are unchanged and there is nothing to decide per publish. Which makes the
connection the right place for it:

```python
async def open_connection(settings: AceMQSettings) -> Connection:
    return await acemq_amqp.connect(
        settings.broker_url,
        # ... and every other argument from the factory above
        codec=ClaimCheckCodec(settings.build_codec(), store, threshold=64 * 1024),
    )
```

Or `register_codec("claimed", lambda: ClaimCheckCodec(JsonCodec(), store))` and
`ACEMQ_FORMAT=claimed`, which is the same trick as
[encryption](security.md#one-format-for-everything) and needs no factory.

Both ends need the same store. A consumer whose store cannot reach what the publisher
wrote gets a key it cannot resolve, so a filesystem store wants a shared volume and
anything else wants object storage — `ClaimCheckStore` is three methods and S3 is a short
implementation of them.

## Saga

A saga runs steps and, when one fails, runs the compensations for the ones that already
succeeded, in reverse. It needs no broker at all — it is a plain object, and it belongs in
the handler or the route that owns the transaction:

```python
from acemq_amqp.patterns import Saga


def booking() -> Saga[Trip]:
    saga: Saga[Trip] = Saga("booking")
    saga.step("reserve-seat", reserve_seat, release_seat)
    saga.step("charge-card", charge_card, refund_card)
    saga.step("issue-ticket", issue_ticket)
    return saga


@app.post("/trips")
async def book(trip: Trip) -> dict[str, object]:
    result = await booking().run(trip)
    if not result.complete:
        raise HTTPException(409, {"failed_at": result.failed_at})
    return {"completed": list(result.completed)}
```

`SagaResult.complete`, `.compensated` and `.has_unresolved` are the three answers worth
branching on. `has_unresolved` is the one that matters: it means a compensation itself
failed, so something is half-done and no amount of retrying the route will fix it. Log
`unresolved` with the subject and alert on it.

A saga in an HTTP request holds the request open for as long as the steps take. That is
fine for three fast steps and wrong for anything slower — a long saga wants a consumer
driving it, which is the same object with the same steps behind
`@acemq.consumer` instead of a route.

## Routing slip

Where a saga runs the steps in one process, a routing slip carries the itinerary in the
message and each service does one step and forwards:

```python
from acemq_amqp.patterns import follow_slip, route_of, start


@app.post("/fulfil")
async def fulfil(order: Order, connection: AceMQConnection) -> dict[str, str]:
    slip = route_of("fulfilment", "pick", "pack", "ship")
    await start(connection, slip, order.model_dump())
    return {"run": slip.run_id}
```

A new slip per request, not one at module scope: `route_of` generates a `run_id`, and a
single shared slip would file every order in the pipeline under the same run.

`route_of` writes the form Java's `Pipeline` writes — the pipeline's name is the exchange,
each step name is the routing key it is bound on, and the queue behind a step is
`{pipeline}.{step}`. So the consumer is on `fulfilment.pick`, and it needs to be told the
pipeline, because the declared form does not carry it:

```python
acemq.add_consumer(
    "fulfilment.pick",
    follow_slip(acemq.connection, pick_the_items, pipeline="fulfilment"),
    name="pick",
)
```

Note `acemq.connection` in that argument, which raises before the lifespan has opened — so
this registration cannot sit at module scope beside the others. It is one of the cases for
the [inner lifespan](#when-the-handlers-need-the-thing-that-needs-the-connection):
register it there with `auto_start=false` set.

The handler returns the payload to send onwards and raises to fail, and the message is
accepted only once the next one is out — so a failure to publish redelivers *this* step.
A step that changes anything should therefore be idempotent, which is what the
[idempotency middleware](#pipelines-and-middleware) is for.

## Scheduling

A message to be delivered later, without a scheduler process or a cron:

```python
from acemq_amqp.patterns import Scheduler, schedule_topology
```

The rung queues it needs are a topology, applied once — `await acemq.connection.declare(schedule_topology())`
in the inner lifespan, or by whatever owns the broker's shape:

```python
@asynccontextmanager
async def scheduling(app: FastAPI) -> AsyncIterator[dict[str, Any]]:
    await acemq.connection.declare(schedule_topology())
    scheduler = await Scheduler.open(acemq.connection)
    try:
        yield {"scheduler": scheduler}
    finally:
        await scheduler.close()
```

```python
@app.post("/reminders")
async def remind(reminder: Reminder, request: Request) -> dict[str, str]:
    scheduler: Scheduler = request.state.scheduler
    await scheduler.after(timedelta(hours=24), "", "reminders", reminder.model_dump())
    return {"in": "24h"}
```

`after(delay, ...)` and `at(when, ...)` are the two calls. `Scheduler.open` subscribes to
the rungs, so exactly one process should hold an open `Scheduler` per broker — a second
one is a second consumer on the same rung queues, which works but divides the hops
between them for no benefit. `scheduled`, `delivered` and `hops` are counters worth
putting on a route while you convince yourself it works.

Delivery is not to the second: a message hops down rungs of an hour, ten minutes, a
minute, ten seconds and a second, so it arrives at or after the time asked for. For a
reminder that is correct. For an auction closing it is not.

## Replay

Moving messages off a dead-letter queue back to where they came from, once the bug is
fixed. It is a call, so it is a route:

```python
from acemq_amqp.patterns import replay


@app.post("/admin/replay/{queue}")
async def replay_dlq(queue: str, connection: AceMQConnection) -> dict[str, object]:
    result = await replay(connection, f"{queue}.dlq", routing_key=queue, limit=500)
    return {"moved": result.moved, "skipped": result.skipped, "reason": result.reason}
```

`limit` and `deadline` bound it, which matters more here than anywhere else on this page:
this is an HTTP request, and a dead-letter queue with two million messages on it is not a
request that should be allowed to finish. `only=` takes a
`Callable[[Envelope, bytes], bool]` and is how one message, or one day's, is moved rather
than all of them.

`restart` is on by default: the messages go back with their attempt counter reset, so the
retry ladder applies again from the beginning. Off replays them as they were, which is
what to use when the ladder is not what you want re-run.

`ReplayError` carries the partial `ReplayResult`, so a failure half-way still says how
many moved.

## Consumer groups

`ConsumerGroup.start(connection, queue, size, handler)` subscribes several consumers to
one queue. In a FastAPI process it is rarely what you want: `concurrency=8` on one
registration gives the same parallelism on the same event loop, and it stays in
`acemq.consumers` where the drain and the health check can see it.

```python
@acemq.consumer("orders", concurrency=8)
async def handle(message: Message) -> Ack: ...
```

The group is for the case where the consumers need separate channels — separate prefetch
accounting, or a broker-side view of several consumer tags — and then it is hand-wired in
the inner lifespan and closed there, exactly like the [stream reader](streams.md#the-drain-does-not-know-about-it),
with the same caveat: `AceMQ.drain()` does not know about it.

## Then

- [Request-reply](request-reply.md) — a route that asks another service and awaits the answer
- [Streams](streams.md) — reading a log from a chosen offset
- [Serialization](serialization.md) — codecs, formats and schema evolution
- [Observability](observability.md) — metrics, tracing and a blocked broker
- [The library's patterns page](https://acemq.org/acemq-python-amqp/patterns.html) — what each one is for, at length
