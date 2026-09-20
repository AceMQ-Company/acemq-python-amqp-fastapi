# Health

Two things, meant to be used separately.

## A route of its own

```python
app.include_router(acemq.health_router())
```

One route at `acemq.health.path`, `/health/acemq` unless it is set.

```json
{
  "status": "up",
  "healthy": true,
  "detail": "",
  "checked": "2026-09-18T09:41:02.118431+00:00",
  "parts": {
    "consumers": {"orders-main": 0},
    "registered": ["orders-main"],
    "in-flight": 0,
    "round-trip": 0.0013,
    "blocked": false
  }
}
```

Mounting it is explicit rather than automatic. A route that appears in an application's
OpenAPI document without anybody adding it is a route somebody has to go looking for.

## A check composed into yours

`AceMQHealth` satisfies the library's `HealthCheck` protocol — a `name` and an
`async check()` — so it drops into `aggregate_health` beside everything else the
service depends on:

```python
from acemq_amqp import aggregate_health

@app.get("/readyz")
async def readyz(acemq: AceMQDep) -> Response:
    report = await aggregate_health(acemq.health_check(), database_check, cache_check)
    return JSONResponse(report_as_json(report), 200 if report.healthy else 503)
```

The combined status is the worst of them, and the combined report names which part was
which.

## What each status means

| | |
|---|---|
| **up** | The broker answered a round trip and every consumer is still reading |
| **degraded** | It answered, but a consumer has stopped reading. Worth an alert; **not** worth a restart, because a replacement would almost certainly stall the same way |
| **down** | The broker did not answer at all |

The status code is the part worth reading twice: 200 for up **and for degraded and
blocked**, 503 only for down. A readiness probe on this route takes the instance out of
rotation when the broker is unreachable and leaves it in when the broker is merely
unhappy. An alert should read the body, not the code.

## A blocked connection is up, with the reason

This is the one place the check disagrees with a naive reading of the broker's state,
and it is deliberate.

RabbitMQ blocks a connection when it is protecting itself — usually low on disk or
memory. It stops reading from the socket, so a health probe that makes a round trip
never gets an answer, and the obvious implementation reports *down*. An orchestrator
then restarts the instance into the same blocked broker, having thrown away whatever it
was holding, and the broker is no better off for it.

So: blocked reports **up, with the reason**, and `parts.blocked` is `true`. It is the
same call the Spring Boot starter's `AceMqHealthIndicator` makes, for the same reason.

```json
{
  "status": "up",
  "healthy": true,
  "detail": "the broker has blocked this connection; publishing is paused",
  "parts": {"blocked": true, "consumers": {"orders-main": 0}, "in-flight": 0}
}
```

### The detail is a contract, not a phrasing

`the broker has blocked this connection; publishing is paused` is the same sentence
every AceMQ library writes — Go's `blockedPrefix`, Ruby's `Health::BLOCKED`, and this
one, word for word. One alert rule matches a blocked broker whatever language the
service behind it is written in, which is the point of fixing the wording. Anything the
broker itself said follows a colon.

**Nothing is appended on RabbitMQ.** The broker does send a reason — "low on memory" —
and aiormq logs it and keeps only a flag, so there is nothing to read back and
`blocked_reason` is `null`. Reporting the block without the reason is the honest half;
a reason invented here would read exactly like one the broker sent. Do not write a
rule that expects one to appear.

This package does not paraphrase that sentence: it comes from the connection and is
passed through. Before acemq-amqp 0.7.0 this package wrote its own, longer wording,
and [the changelog](https://github.com/AceMQ-Company/acemq-python-amqp-fastapi/blob/main/CHANGELOG.md)
says what an alert rule matching the old one has to change to.

### `blocked: null`

`null` is not `false`. It means the question could not be asked — a transport with no
answer to give, or a connection between reconnections — and a report saying `null` is
more use in an incident than one saying `false` because nothing looked.

It is the connection's answer, not this package's. `Connection.blocked` is `true`,
`false` or `null`, and the report carries it through untouched; nothing here has a
`blocked` of its own that could overwrite it. There is an integration test whose only
job is to fail if `blocked` reads `null` against a live broker, because that would mean
the library had quietly lost the ability to tell a blocked broker from a wedged one.

Asking used to take a walk down `transport → connection → transport → connection`
looking for a private aiormq event by its name-mangled attribute — there was no
supported accessor anywhere above it. 0.7.0 added one, and the walk is gone.

### The timeout

`acemq.health.timeout` is handed to the library's probe as its deadline rather than
wrapped around it. The probe has had a deadline of its own since 0.7.0, and a second one
outside it would be the cruder of the two: a blocked broker that stops answering is
recognised as blocked and reported *up*, and an outer timeout firing first would turn
that into a spurious *down*. It would also cancel a request the library deliberately
abandons — a broker that is not reading cannot be relied on to process a cancellation
either.

A blocked broker is not probed at all: the state is read first and the round trip
skipped, so the route answers immediately instead of spending the whole deadline
arriving at an answer it already had.

## Metrics and tracing

They come from the library, not from here:

```python
from acemq_amqp.prometheus import PrometheusObserver

acemq = AceMQ(observer=PrometheusObserver())
```

What is counted, and under what name, is in
[the library's observability page](https://acemq.org/acemq-python-amqp/observability.html).
`acemq.consume.in_flight` and `acemq.retried.total` are the two an operator of a FastAPI
process wants first: the first says whether the loop is saturated, and the second rises
before the dead letters move.
