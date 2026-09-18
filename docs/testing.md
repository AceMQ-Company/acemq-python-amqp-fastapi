# Testing

Three levels, and most tests want the first.

## The handler, on its own

The decorator returns the function unchanged, so a handler is an ordinary async
function:

```python
from acemq_amqp import Envelope, Message, accept, reject

def a_message(payload: object) -> Message:
    return Message(
        payload=payload,
        envelope=Envelope.new(),
        routing_key="orders",
        content_type="application/json",
        redelivered=False,
        body=b"",
    )

async def test_rejects_an_order_with_no_sku() -> None:
    assert await handle_order(a_message({"qty": 1})) == reject("no sku")
```

No broker, no lifespan, no application. This is where the business logic belongs and
where most of the tests should be.

## The application, on a transport that is a dictionary

`AceMQ(connection_factory=...)` is the seam. A factory returns an
`acemq_amqp.Connection` built on anything satisfying the library's `Transport`
protocol, and the whole package — the lifespan, the registry, the dependencies, the
health check, the drain — runs against it:

```python
async def open_fake(settings: AceMQSettings) -> Connection:
    return Connection(FakeTransport(), prefetch=settings.prefetch)

acemq = AceMQ(AceMQSettings(), connection_factory=open_fake)
app = FastAPI(lifespan=acemq.lifespan)

def test_a_route_publishes() -> None:
    with TestClient(app) as client:
        client.post("/orders")
    assert transport.published == [...]
```

`TestClient` runs the lifespan for you — that is what its `with` block is — so a test
written this way exercises the real start-up and the real drain.

This repository's own `tests/fake_transport.py` is roughly seventy lines and is a
reasonable thing to copy. It records settlements, which matters more than it sounds:
"the handler finished and the message was acknowledged" and "the handler was cancelled
and the message was left" are the two shutdown outcomes a test has to tell apart, and
from outside they look identical.

## Against a real broker

```bash
docker run -d --rm --name acemq-test -p 5722:5672 rabbitmq:4-alpine
```

Mark them, so a laptop with no Docker still runs everything else:

```toml
[tool.pytest.ini_options]
markers = ["integration: needs a running broker"]
asyncio_mode = "auto"
```

```bash
pytest -m "not integration"   # everything that needs nothing
pytest -m integration         # the rest
```

Give the broker a port of its own — 5722 rather than 5672 — so the suite cannot reach
one somebody is using for something else, and give every test a queue named with a
`uuid4` so two runs cannot collide.

### Waiting for a handler from a `TestClient` test

`TestClient` drives the application from a thread of its own, so the handlers are on a
loop the test is not on. Wait on the client's portal rather than sleeping and hoping:

```python
with TestClient(app) as client:
    client.post("/orders")
    client.portal.call(lambda: asyncio.wait_for(arrived.wait(), 10))
```

`asyncio.sleep(0.5)` in the test's own thread waits for wall-clock time and proves
nothing about whether the handler ran.

### Proving the drain

The shutdown guarantee is worth one test of its own, and it is easy to write: publish
something a handler will be slow with, wait until it has started, and leave the
`with` block. Leaving it *is* the ASGI shutdown.

```python
with TestClient(app) as client:
    client.post("/slow")
    client.portal.call(lambda: asyncio.wait_for(started.wait(), 10))
    # leaving the block drains while the handler is mid-sleep

assert finished, "the drain returned before the handler did"
assert await message_count(queue) == 0, "it was acknowledged, not handed back"
```

The second assertion is the one that matters. A drain that returns quickly *and* leaves
the message on the queue is a drain that abandoned it, and only the queue depth can
tell the two apart.
