# Shutdown

The Go library's [lifecycle page](https://acemq.org/acemq-go-amqp/lifecycle.html) asks
four questions of a shutdown. They are the right four, and the answers here are not all
the same. Everything below was measured against RabbitMQ 4 on the version of
`acemq-amqp` this package depends on; the script is
`tests/test_shutdown.py` and `tests/test_integration.py`.

| at the moment the lifespan exits | what happens | same as Go? |
|---|---|---|
| a handler is running | it runs to completion, and its decision is carried out | yes |
| a delivery has arrived but no handler has it yet | **it is handed back to the broker**, requeued | **no** |
| a publish is waiting for its confirm | it is cut off, and **raises `PublishError`** | mostly |
| a retry is waiting out a backoff in this process | the drain waits with it, for the whole delay | yes |

## The handler in flight

It finishes. Not "is given a chance to finish" — the drain blocks on it. A handler
sleeping for 250 milliseconds when shutdown starts makes shutdown take 250
milliseconds, and the message is acknowledged rather than abandoned for the broker to
give to somebody else.

That is the guarantee worth having, and it is also why a shutdown needs a deadline:
nothing here will interrupt a handler that never returns.

## The delivery that never reached a handler

**This is where Python and Go differ, and it is the answer a reader of the Go page
will get wrong.** Go's `Close` runs every delivery the transport had already handed
over through a handler, so the work a drain has to get through is bounded by
`Prefetch`. Python's does not: each of those deliveries is nacked with requeue and
goes back to the broker for another consumer to take.

Measured, with a prefetch of 10, one worker and a handler that takes a second:

```
close took 0.81s, handled [0], queue depth afterwards 9
```

Go, with the same numbers, would have taken about ten seconds and handled all ten.

The library's reason is in `Consumer.close`, and it is a good one: a delivery held here
only to be worked through a retry delay would make closing take as long as the retry
schedule. The consequence for an application is the inverse of Go's: **a large prefetch
costs redeliveries here rather than a long drain.** Keep handlers idempotent — which
at-least-once already required — and prefetch stops being a shutdown concern.

## The publish waiting for its confirm

Nothing waits for it. The connection closes and the confirm never arrives.

Python is louder about it than Go: the pending `send` or `send_all` raises
`PublishError` naming how many of its messages were not confirmed. Measured, with 500
messages in flight:

```
close took 0.007s, the publish raised PublishError:
  500 of 500 messages were not confirmed; 0 were.
```

That is the right failure — a publish that says it succeeded when the confirm was cut
off is worse — but it means a route publishing while the process is shutting down sees
an exception rather than a false success. In practice an ASGI server closes the HTTP
server before the lifespan exits, so requests have already finished; a background task
publishing on its own schedule is the case to check. A publish that must survive a
restart wants the library's
[outbox](https://acemq.org/acemq-python-amqp/patterns.html), and no shutdown handling
is a substitute for it.

## The retry waiting out a backoff

A short wait is spent in this process holding the delivery; a long one is spent on a
rung queue in the broker, holding nothing. Which is which is
`acemq.consumer.retry.wait_in_broker_from`.

**The drain waits out an in-process backoff in full.** Measured, with a three-second
delay entered two hundred milliseconds earlier:

```
close took 2.26s
```

A policy on a five-minute schedule that fell back to waiting in this process makes the
drain take five minutes, which is to say it makes the drain fail — nothing grants a pod
five minutes. So the rung queues are a shutdown concern as much as a reliability one.
Set the threshold, let the broker hold the long waits, and a restart does not have to
sit through them:

```bash
ACEMQ_CONSUMER__RETRY__ENABLED=true
ACEMQ_CONSUMER__RETRY__WAIT_IN_BROKER_FROM=10
```

## Bounding it

`acemq.consumer.shutdown_timeout`, twenty seconds by default. When it expires the
consumers still closing are cancelled, which cancels the handlers with them:

```python
report = await acemq.drain()      # the lifespan does this for you
report.finished   # False when the deadline expired
report.abandoned  # how many messages were still in a handler
```

**What is lost when the deadline expires** is worth being exact about, because "the
drain timed out" sounds worse than it is:

- Messages whose handlers had not finished are **not acknowledged**, so the broker
  redelivers them. The work may be done twice — the ordinary at-least-once case
  handlers should already be idempotent against.
- Messages that needed dead-lettering in that window went to the broker's own
  dead-letter exchange without the reason attached, or nowhere.

Nothing is silently dropped. What is lost is the *reasons*.

The deadline has to be comfortably shorter than whatever will kill the process:

| | |
|---|---|
| Kubernetes | `terminationGracePeriodSeconds`, thirty by default |
| systemd | `TimeoutStopSec` |
| Docker | `docker stop -t`, ten by default |

Twenty seconds inside a thirty-second grace period leaves ten for the HTTP server and
for the process to actually exit. Setting the two equal means the orchestrator wins the
race sometimes, and a shutdown that is correct on most deployments is one nobody debugs
until it is not.

The connection is released whether or not the drain finished. A socket left open by a
process that is about to exit is the one outcome with nothing to recommend it.

## Marking the drain

A readiness probe should start failing before the drain begins, so the load balancer
stops sending requests to an instance that is going away. FastAPI gives you the seam:

```python
@asynccontextmanager
async def readiness(app: FastAPI) -> AsyncIterator[None]:
    app.state.ready = True
    try:
        yield
    finally:
        app.state.ready = False      # before AceMQ's drain, because this is outside it

app = FastAPI(lifespan=compose_lifespans(readiness, acemq.lifespan))
```

Outside AceMQ in the composition means it flips before the drain starts — which is the
order you want, and the reason the nesting rule in [the lifespan](lifespan.md) is
worth reading twice.
