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

The check is bounded by `acemq.health.timeout`. That matters more here than it looks —
the library's own probe is a round trip with no deadline of its own, and a blocked
broker is exactly the state in which a round trip does not come back. A probe that hangs
is a pod that never comes back.

### `blocked: null`

`null` is not `false`. It means the question could not be asked.

There is no supported way to ask it. RabbitMQ sends `connection.blocked` and
`connection.unblocked` on channel zero; aiormq handles both by setting and clearing an
event it keeps to itself, and neither it nor aio-pika exposes that event — or the reason
string RabbitMQ sent with it. So `blocked_state` walks down the transports looking for
that event by name, and answers `null` when it is not there.

A report saying `blocked: null` is more use in an incident than one saying `false`
because nothing looked. There is an integration test whose only job is to fail when
this starts returning `null` against a live broker.

The reason string is genuinely unavailable — it is logged by aiormq and discarded — so
the detail says what is happening without inventing why. A supported accessor belongs
on the library's transport, beside the `isBlocked()` and `blockedReason()` the Java
library already has; until there is one, this is the honest version of the guess.

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
