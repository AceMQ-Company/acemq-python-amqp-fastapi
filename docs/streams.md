# Streams

A queue forgets a message the moment somebody acknowledges it. A stream keeps it, and
lets a reader say where in the log to start. That is the whole difference, and it is the
reason to reach for one: a projection can be rebuilt from nothing, a second service can
be added later and still see the history, and a consumer that fell behind can resume
from where it stopped rather than from whatever is left.

Everything here is `acemq_amqp.patterns` — the stream helpers, the offsets, the
retention policy. What this page is about is the two things that are different in a
FastAPI application: declaring a stream through `acemq.topology`, and running a reader
under the lifespan when `@acemq.consumer` cannot do it.

```bash
pip install --extra-index-url https://acemq.org/pypi/simple/ acemq-amqp-fastapi
```

Nothing extra to install. Streams are a RabbitMQ queue type, not a second protocol —
the same AMQP connection, the same `aio_pika` transport, the same codec.

## Declaring one

A stream is a queue whose `x-queue-type` is `stream`, so `acemq.topology.queues` can
declare it. The library's `Topology.queue` keeps a kind that `args` already names, which
is exactly what makes this work:

```python
AceMQSettings(
    topology={
        "queues": [
            {
                "name": "readings",
                "args": {
                    "x-queue-type": "stream",
                    "x-max-age": "1h",
                    "x-max-length-bytes": 20 * 1024 * 1024,
                    "x-stream-max-segment-size-bytes": 1024 * 1024,
                },
            }
        ]
    }
)
```

Or from the environment, since `args` is a nested model field:

```bash
export ACEMQ_TOPOLOGY__QUEUES='[{"name":"readings","args":{"x-queue-type":"stream","x-max-age":"1h"}}]'
```

Two things to leave alone. Do not set `quorum` on that entry — writing `x-queue-type`
down *and* passing `quorum` is two answers to one question and the library refuses it.
And do not set `dead_letter: true` unless you want `readings.dlq` and `readings.parked`
declared as ordinary queues beside the stream; a stream consumer can park a copy into
them, but they are not part of the log.

The retention arguments are RabbitMQ's own spelling, and `x-max-age` wants a number with
a unit suffix — `1h`, `7D`, `30m` — because the broker refuses a plain number there. It
is easy to get one of those wrong in JSON, so the alternative is worth knowing:

```python
from datetime import timedelta

from acemq_amqp.patterns import StreamRetention, declare_stream

await declare_stream(
    acemq.connection,
    "readings",
    StreamRetention(
        max_age=timedelta(hours=1),
        max_bytes=20 * 1024 * 1024,
        segment_bytes=1024 * 1024,
    ),
)
```

`StreamRetention` produces the same three arguments with the units formatted for you.
Where to put that call is the next section.

**Retention is not optional in the way it looks.** A queue's messages leave when they
are handled; a stream's leave when the policy says so, and a stream with no policy grows
until the disk is full. `segment_bytes` matters for the same reason: retention happens a
whole segment at a time, so one enormous segment means nothing is ever discarded whatever
`max_age` says.

## Publishing to one

Ordinarily. A stream is a queue name, so the publisher dependency is the publisher
dependency:

```python
Readings = Annotated[Publisher, Depends(publishes("readings"))]


@app.post("/readings")
async def record(reading: Reading, readings: Readings) -> dict[str, str]:
    result = await readings.send(reading.model_dump())
    return {"id": result.message_id}
```

## Reading one

**`@acemq.consumer` cannot read a stream, and this is the one place in these docs where
the decorator is the wrong tool.** The reason is not that the offset argument is missing
— `args={"x-stream-offset": "first"}` would reach the broker perfectly well. It is what
`read_stream` does around the handler:

- it forces `retry=no_retry()` rather than inheriting the connection's policy, and
- it wraps the handler so one returning `retry()` is refused with a `StreamRetryError`.

Both close the same hole from the two directions a retry can arrive from. Retrying on a
stream means republishing to it, which appends a second copy of the message for every
other reader of that log to see as well — and from outside, a stream that is growing
looks exactly like a stream that is busy. A registration through `@acemq.consumer` gets
the offset and none of that, which is a worse position than not supporting streams at
all.

So a reader is hand-wired, in a lifespan composed inside this package's:

```python
# app.py
from contextlib import asynccontextmanager
from typing import Annotated, Any, AsyncIterator

from acemq_amqp import Ack, Message, accept
from acemq_amqp.patterns import from_first, read_stream
from acemq_fastapi import AceMQ, compose_lifespans
from fastapi import Depends, FastAPI, Request

acemq = AceMQ()

average: dict[str, float] = {}


async def project(message: Message) -> Ack:
    reading = message.payload
    average[reading["sensor"]] = reading["celsius"]
    return accept()


@asynccontextmanager
async def readings(app: FastAPI) -> AsyncIterator[dict[str, Any]]:
    consumer = await read_stream(
        acemq.connection,
        "readings",
        project,
        offset=from_first(),
        consumer_name="projection",
        declare=False,
    )
    try:
        yield {"readings": consumer}
    finally:
        await consumer.close()


app = FastAPI(lifespan=compose_lifespans(acemq.lifespan, readings))
```

`readings` goes to the **right** of `acemq.lifespan`, which is the opposite of the advice
on [the lifespan page](lifespan.md#keeping-the-lifespan-you-already-have) and for the
same underlying reason. Left to right is outside in, so `acemq.lifespan` opens first and
closes last — which is what gives `readings` an open `acemq.connection` to read from, and
what keeps the connection alive until after the reader has closed. Anything that needs
the connection goes inside; anything the *handlers* need goes outside.

`declare=False` because the topology already declared the stream. A reader that declares
is a reader that can disagree about the shape of something it did not create, and the
broker answers that disagreement with an error that never mentions streams.

### The drain does not know about it

This is the part worth being careful with. `AceMQ.drain()` stops the consumers *it*
subscribed — the ones registered through `@acemq.consumer` or `add_consumer`. A consumer
returned by `read_stream` is not one of them: it does not appear in `acemq.consumers`, it
is not counted in the `DrainReport`, and nothing closes it but the `finally` above.

Which is why the `finally` is not optional, and why the ordering matters twice over: the
composed lifespan closes `readings` before `acemq.lifespan` drains and closes the
connection, so the reader's handlers finish against a connection that is still open. Get
that order the other way round and the symptom is a handler failing mid-message on a
closed connection during every shutdown.

The same applies to the health check. `acemq.health()` reports on the consumers this
package started; a stalled stream reader is invisible to it. Add it yourself if it
matters:

```python
@app.get("/health/readings")
async def readings_health(request: Request) -> dict[str, object]:
    consumer = request.state.readings
    return {"running": consumer.running, "in_flight": consumer.in_flight}
```

`request.state.readings` is the key the lifespan yielded — `compose_lifespans` merges
every mapping its lifespans yield into the request state, in order.

## Where to start

```python
from acemq_amqp.patterns import from_first, from_last, from_next, from_offset, from_timestamp
```

| | |
|---|---|
| `from_next()` | The next message published. The default, and what a tail wants |
| `from_first()` | The oldest message the stream **still holds** — retention has already dropped whatever is older |
| `from_last()` | The last chunk written, then onwards |
| `from_offset(3)` | Offsets count from zero, so this is the fourth message |
| `from_timestamp(when)` | The first message at or after a moment |

A projection rebuilt at every start-up uses `from_first()` and needs no state of its own,
which is the simplest thing that works and is often enough. Past that, the interesting
one is `consumer_name`: naming a reader is what lets the broker track its offset
server-side, so a restart carries on rather than re-reading. Two processes reading the
same stream want two names, or they share a position neither of them chose.

## What a failing handler does

There is no retry, so there are two outcomes and they are enough.

`accept()` checkpoints past the message and leaves the failure for the handler to record.
That is what most projections want: the message is still on the log, so once whatever
broke is fixed a reader can be pointed at an earlier offset and read it again.

`park()` puts a copy in `readings.parked` — a queue beside the stream rather than part of
it — and leaves the log untouched. For the message that will never work.

`reject()` works too and puts a copy in `readings.dlq`. It is not one of the two because
"this message is bad" is rarely what a stream failure is.

`retry()` raises `StreamRetryError`, as above.

## Running the reader in its own process

A projection reading a stream from the first offset at every deployment is not work an
HTTP process wants, and nothing forces it to be there. `AceMQ.start()` takes no
application:

```python
# projector.py
import asyncio

from acemq_amqp.patterns import from_first, read_stream

from app import acemq, project


async def main() -> None:
    await acemq.start()
    consumer = await read_stream(
        acemq.connection, "readings", project, offset=from_first(), declare=False
    )
    try:
        await asyncio.Event().wait()
    finally:
        await consumer.close()
        await acemq.drain()
        await acemq.aclose()
```

Same settings, same registrations, no ASGI application and no port. See
[the lifespan page](lifespan.md#without-an-asgi-application-at-all) for the shape and for
what to do about signals.

## Then

- [Patterns](patterns.md) — the rest of what the library carries, and how each one is reached
- [Consumers](consumers.md) — ordinary queues, where the decorator is the right tool
- [The library's streams page](https://acemq.org/acemq-python-amqp/streams.html) — offsets, retention and what RabbitMQ does underneath
