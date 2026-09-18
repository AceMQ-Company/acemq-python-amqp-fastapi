# The lifespan

`acemq.lifespan` is where everything happens. It opens the connection, applies the
topology, subscribes the registered consumers, hands control back to the application,
and on the way out drains the consumers and releases the connection.

```python
app = FastAPI(lifespan=acemq.lifespan)
```

## What it does, in order

| Startup | Shutdown |
|---|---|
| 1. `acemq_amqp.connect(...)`, under `acemq.connection_timeout` | 1. Every consumer is closed and its handlers allowed to finish |
| 2. Publish and consume interceptors are registered | 2. The connection is released |
| 3. `acemq.topology` is applied, when anything was declared | |
| 4. Every `@acemq.consumer` is subscribed | |
| 5. `app.state.acemq` is set | |

Startup is all-or-nothing. A consumer that cannot subscribe — a queue the login has no
permission on, a broker that refuses a declaration — unwinds what has already been
done and lets the exception out, so the process refuses to start rather than serving
routes with no consumers behind them. A half-started application is worse than one
that did not start: from outside they look the same.

## Keeping the lifespan you already have

FastAPI takes one lifespan. `compose_lifespans` nests several:

```python
from acemq_fastapi import compose_lifespans

@asynccontextmanager
async def database(app: FastAPI) -> AsyncIterator[dict[str, Any]]:
    pool = await open_pool()
    try:
        yield {"pool": pool}
    finally:
        await pool.close()

app = FastAPI(lifespan=compose_lifespans(database, acemq.lifespan))
```

Left to right is outside in. `database` opens first and closes last; `acemq.lifespan`
opens last and closes first.

**Put AceMQ last.** Consumers should start after the things their handlers use and
stop before those go away. A handler reaching for a connection pool that has already
been closed is the bug this ordering exists to prevent, and it only shows up during a
deployment, in a log nobody is reading.

Whatever each lifespan yields as a mapping is merged and reaches the request state, in
order, so a later one can override an earlier one's key. `acemq.lifespan` yields
`{"acemq": acemq}`.

Write the cleanup in a `try`/`finally` rather than on a line after the `yield`. When a
later lifespan fails on the way up, the exception is thrown *into* the earlier ones at
their `yield` — a bare line after it never runs. `acemq.lifespan` is written this way
for exactly that reason, and there is a test that would fail if it stopped being.

## Without an ASGI application at all

A worker process that consumes and serves no HTTP wants the same start-up, and should
not have to construct a FastAPI application to get it:

```python
async def main() -> None:
    await acemq.start()
    try:
        await stop_signal.wait()
    finally:
        await acemq.drain()
        await acemq.aclose()
```

`start()` takes an optional app; without one, nothing is put on `app.state` and the
request dependencies are simply not available — which is correct, because there are no
requests.

## The connection is yours if you want it

`AceMQ(connection_factory=...)` replaces the one thing this package does that
`acemq-amqp` already does better:

```python
async def open_it(settings: AceMQSettings) -> Connection:
    return await acemq_amqp.connect(settings.broker_url, codec=my_codec, heartbeat=15)

acemq = AceMQ(connection_factory=open_it)
```

Everything else keeps working against it, because the registry, the dependencies and
the health check take the connection rather than creating one. It is the same shape as
the Spring starter's `@ConditionalOnMissingBean AceMq`: define your own and the rest
of the wiring still applies.

## Two applications in one process

Each gets its own. The dependencies find the integration through
`request.app.state.acemq`, not through a module-level global, so a main application
and a mounted admin one — or two applications in one test session — reach their own
connection rather than whichever was built last.
