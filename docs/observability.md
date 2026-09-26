# Observability

Health has [a page of its own](health.md) — this one is about the other three things:
numbers, traces, and what a blocked broker looks like from outside.

None of them has an `acemq.*` setting, and for once the reason is not that they need a
database handle. An observer is a live object and a tracer needs a configured SDK, so both
reach this package through its constructor rather than through configuration.

## Metrics

The library counts what it does through a three-method `Observer` — `count`, `observe`,
`gauge` — and depends on no metrics client. Give it one:

```python
from acemq_amqp import Metrics
from acemq_fastapi import AceMQ

metrics = Metrics()
acemq = AceMQ(observer=metrics)
```

`Metrics` is the library's own in-process implementation: counters, gauges and duration
summaries in dictionaries, with nothing installed. Which makes a `/metrics` route two
lines:

```python
from acemq_amqp import prometheus_text
from fastapi import Response


@app.get("/metrics", response_class=Response)
async def scrape() -> Response:
    return Response(prometheus_text(metrics), media_type="text/plain; version=0.0.4")
```

`prometheus_text` renders the Prometheus exposition format from a `Metrics`. It is the
cheapest thing that works and it is genuinely enough for a service whose only metrics are
AceMQ's.

For a service that already has a registry — and most do — use the adapter instead, so the
broker's numbers land beside the application's:

```bash
pip install "acemq-amqp[prometheus]"
```

```python
from acemq_amqp.prometheus import PrometheusObserver
from prometheus_client import CollectorRegistry, generate_latest

registry = CollectorRegistry()
acemq = AceMQ(observer=PrometheusObserver(registry))


@app.get("/metrics", response_class=Response)
async def scrape() -> Response:
    return Response(generate_latest(registry), media_type="text/plain; version=0.0.4")
```

`PrometheusObserver(registry, namespace=...)` prefixes the metric names, for a service
whose convention wants that.

The library depends on the OpenTelemetry and Prometheus *APIs* at most, never on a global
registry it opens a port for. Exposing the numbers is the application's decision, which in
a FastAPI application means a route — there is no second HTTP server here and there does
not need to be one.

### What is counted

| | |
|---|---|
| `acemq.publish.total`, `acemq.publish.duration` | Tagged with the outcome, and cut the same way, so one divides into the other |
| `acemq.consume.total`, `acemq.consume.duration` | Counted from the *settlement* rather than from what the handler asked for |
| `acemq.consume.attempts` | Which attempt a delivery was on when it was handled. A rising distribution is a dependency in trouble, and it says so before the dead letters do |
| `acemq.consume.in.flight` | A gauge, bounded by prefetch times concurrency |
| `acemq.messages.retried.total`, `acemq.messages.dead.lettered.total` | |
| `acemq.retry.rung.missing` | **Alert on this one** |
| `acemq.messages.set.aside.failed` | Parking itself failed |
| `acemq.request.total`, `acemq.request.duration` | [Request-reply](request-reply.md) |
| `acemq.outbox.total`, `acemq.outbox.lag` | [The outbox relay](patterns.md#transactional-outbox) |
| `acemq.pipeline.run.total`, `acemq.pipeline.run.duration` | [Routing slips](patterns.md#routing-slip), once per run |

`acemq.retry.rung.missing` is the one worth an alert rather than a dashboard. It means a
retry's rung queue is not there, so the wait is happening in this process instead of in the
broker — which is invisible in every other metric and shows up as a
[drain that will not finish](shutdown.md).

### The numbers this package adds

The health report's `parts` carry what the connection cannot answer: `consumers` is a map
of name to in-flight count, and `registered` lists every registration whether it is running
or not. A consumer in `registered` and missing from `consumers` is one that was stopped —
deliberately by `stop_consumer`, or by `acemq.consumer.auto_start=false`.

Reading them off the integration directly is also fine, and is what an admin route wants:

```python
@app.get("/admin/consumers")
async def consumers(acemq: AceMQDep) -> dict[str, dict[str, object]]:
    return {
        name: {"in_flight": consumer.in_flight, "running": consumer.running}
        for name, consumer in acemq.consumers.items()
    }
```

## Tracing

```bash
pip install "acemq-amqp[opentelemetry]"
```

The adapter is two interceptors, and interceptors are a constructor argument:

```python
from acemq_amqp.tracing import OpenTelemetryTracing

tracing = OpenTelemetryTracing()
acemq = AceMQ(
    on_publish=(tracing.publish_interceptor(),),
    on_consume=(tracing.consume_interceptor(),),
)
```

`AceMQ.start()` registers them before any consumer subscribes and before the first publish
is possible, so nothing goes out or comes in untraced. `tracing.install(connection)` does
both in one call and is what a hand-built `connection_factory` would use instead.

The publish interceptor writes the trace context into the message's headers and the consume
interceptor reads it back out, so a span from a FastAPI route continues in the handler that
picks the message up — in this process or in another service, in another language. That is
the reason to bother: the alternative is two unrelated traces and a human joining them by
timestamp.

`OpenTelemetryTracing` is written against the OpenTelemetry API alone and exports nothing
until the application installs an SDK and configures it. Combined with FastAPI's own
instrumentation, an HTTP request that publishes and a handler that consumes end up on one
trace without either side knowing about the other.

Nothing here configures the SDK. A tracing stack is the application's decision, and a
library that started exporting because it was imported would be the wrong shape.

## A blocked broker

RabbitMQ blocks a connection when it is short of memory or disk: it stops reading the
socket, so a publish waits rather than failing. `Connection.blocked` and
`Connection.blocked_reason` report it, and the health check folds them into the report —
**as up, with the reason**, which is [health's page](health.md#a-blocked-connection-is-up-with-the-reason)
to explain.

What matters here is that a blocked broker is the failure mode this stack hides best. The
routes keep answering, the consumers keep reading, and the publishes queue. Three things
make it visible:

- The health report says so, in a sentence the library owns and every AceMQ language
  writes identically, so one alert rule matches it whatever the service is written in.
- `acemq.max_outstanding_publishes` bounds the queueing, so the thousand-and-first publish
  waits for room instead of growing the heap — and the requests behind it block, which is
  back pressure reaching the client. See [publishing](publishing.md#back-pressure).
- `acemq.confirm_timeout` ends the wait, so a publish eventually raises rather than hanging
  for the length of the incident.

Without the second and third a blocked broker looks like a healthy service until the
process is killed for memory, which is the same symptom as a leak and gets diagnosed as
one.

```python
@app.get("/admin/broker")
async def broker(connection: AceMQConnection) -> dict[str, object]:
    return {
        "blocked": connection.blocked,
        "reason": connection.blocked_reason,
        "outstanding": connection.outstanding_publishes,
    }
```

`blocked` is `None` rather than `False` when the transport cannot answer — a fake transport
in a test, or one that does not implement the library's `BlockedState`. Three states, and
"nobody knows" is one of them.

## Logging

Everything this package logs goes to the `acemq_fastapi` logger, at `INFO` for a connection
opened or closed and `WARNING` for a drain that did not finish or a consumer that failed to
close. Those two warnings are the ones to watch: both mean messages went back to the broker
and will be delivered again.

```python
logging.getLogger("acemq_fastapi").setLevel(logging.INFO)
```

The library logs under its own names, separately, so the two can be turned up
independently.

## Then

- [Health](health.md) — the route, the statuses, and the blocked-connection decision
- [Shutdown](shutdown.md) — what a drain warning actually means
- [The library's observability page](https://acemq.org/acemq-python-amqp/observability.html)
