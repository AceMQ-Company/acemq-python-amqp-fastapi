# Topology, and drift

The lifespan applies `acemq.topology` between opening the connection and subscribing the
consumers. That order is the whole reason it is a setting rather than a script: the queues
a consumer needs exist by the time it subscribes, in the same process, from the same
configuration, and a broker that refuses a declaration fails the start-up rather than
producing an application whose routes are up and whose consumers are not.

Declaring nothing is the default, and it is a real answer. A service that only consumes
queues somebody else owns should not create them.

## What to write

```python
AceMQSettings(
    topology={
        "exchanges": [{"name": "events", "kind": "topic"}],
        "queues": [{"name": "orders", "dead_letter": True}],
        "bindings": [{"queue": "orders", "exchange": "events", "routing_key": "order.*"}],
    }
)
```

From the environment, each list is JSON — pydantic-settings parses a nested model's value
as JSON, so this is not a special case this package added:

```bash
export ACEMQ_TOPOLOGY__QUEUES='[{"name":"orders","dead_letter":true}]'
export ACEMQ_TOPOLOGY__EXCHANGES='[{"name":"events","kind":"topic"}]'
export ACEMQ_TOPOLOGY__BINDINGS='[{"queue":"orders","exchange":"events","routing_key":"order.*"}]'
```

That is legible in a `.env` and horrible in a Kubernetes manifest. For anything more than
two queues, write the settings down in Python and keep the environment for the URL and the
credentials — `AceMQSettings(topology={...})` accepts the same shape and a nested
`BaseSettings` can carry it too. The point of the setting is not that everything must come
from an environment variable.

The [configuration page](configuration.md#topology--acemqtopology) has every field.

## A durable queue is a quorum queue

That is not a preference. The queue type is one more argument the broker compares, so a
Java service and a Python service that disagree about `orders` cannot both consume it —
the second declaration fails, and the message names the argument rather than the mistake.
Every AceMQ library declares a durable queue as quorum for that reason.

`quorum: false` declares a classic queue, by leaving `x-queue-type` off entirely, which is
the spelling every AceMQ library uses and therefore the only one a broker finds equivalent
to theirs.

`exclusive`, `auto_delete` and `durable: false` all describe a queue belonging to one
connection, and RabbitMQ refuses a quorum queue that is any of those — so such a queue is
declared classic without being asked.

`args` that already name an `x-queue-type` wins, which is how a
[stream](streams.md#declaring-one) is declared through this setting.

## `dead_letter: true`

Brings `{name}.dlq` and `{name}.parked` with it and points the broker at them. Neither
dead-letters itself, because a dead-letter queue that dead-letters is a loop and a loop is
how a poison message becomes an outage.

Both are reached through `acemq.dlx`, bound on their own names, which is what the Java
library has always done — so a broker shared with a Java service already has it.

Leave it off and pass your own arguments in `args` to use a different dead-letter exchange.

## `declare_retry_rungs`

On, so the rung queues the consumer retry policy needs — one `{queue}.retry.{delay}` per
distinct delay at or above the policy's threshold — are declared with each queue. A policy
whose rungs are missing falls back to waiting in this process, which is the slow drain
nobody expects, and it does it silently.

It takes the policy from `acemq.consumer.retry` rather than a second list of delays,
because two copies of the same list drift, and the way that drift shows up is a retry
published to a queue nobody declared, at the moment the service is already failing.

## A login that may not declare

```bash
export ACEMQ_CONSUMER__DECLARE=false
```

A consumer declares the queues a failure needs — `{queue}.dlq`, `{queue}.parked`, the
rungs — before subscribing. A login with no `configure` permission on the vhost cannot,
and the symptom is an `ACCESS_REFUSED` at start-up naming a queue the service never
mentions. Turning `declare` off leaves the consumer to subscribe to what is already there.

Which means somebody else has to have declared it. A consumer that parks messages into a
queue nobody declared has only moved the disappearance somewhere else, so this setting is
a statement that the broker's shape is managed elsewhere, not a way to skip thinking about
it.

`declare=False` on one `@acemq.consumer(...)` registration does the same for one queue.

## Drift

AMQP has no way to ask a broker what is there, short of the management API. So neither
this package nor the library can show you a difference between the configuration and the
broker, and neither pretends to — `Topology.plan()` is a statement of intent rather than a
diff:

```python
from acemq_fastapi import AceMQDep


@app.get("/admin/topology")
async def topology(acemq: AceMQDep) -> dict[str, list[str]]:
    declared = acemq.settings.build_topology()
    if declared is None:
        return {"plan": []}
    return {"plan": [str(action) for action in declared.plan()]}
```

`build_topology()` is synchronous and touches no broker, so this route works whether or not
the connection is healthy. `plan()` calls `validate()` first, which catches the mistake
worth catching without a broker: a binding naming a queue the topology does not declare.
The broker would accept that binding if the queue happened to exist already, and the
service would then depend on something nothing declares — which works until it is deployed
somewhere new.

What *does* catch real drift is the declaration itself. A queue that exists with different
arguments makes the lifespan fail with the broker's `PRECONDITION_FAILED`, naming the
argument that disagrees. That is a start-up failure rather than a warning, and it is the
right one: two services with different ideas of what `orders` is will not both work.

When it happens, the broker is usually right and the configuration is behind. A queue's
arguments cannot be changed in place — delete it and redeclare, which means draining it
first, or declare a new one and move the consumers.

## Checking rather than declaring

```python
@app.get("/admin/queues/{name}")
async def queue(name: str, connection: AceMQConnection) -> dict[str, object]:
    if not await connection.queue_exists(name):
        raise HTTPException(404)
    return {"messages": await connection.message_count(name)}
```

`queue_exists` is a passive declare, so it answers without creating anything. `message_count`
is the depth — worth a route during an incident, and worth remembering that a stream
answers it with zero however much it holds, because a stream has a position rather than a
remainder.

## Who should own it

The service that owns the queue. A queue named `orders` consumed by one service and
declared by three is a queue whose arguments are whichever deployment went out last.

For everything shared — an exchange several services bind to, a queue two of them read —
the honest options are one owning service that declares it, or a migration outside every
application. Either is better than `acemq.topology` in three repositories, which is the
arrangement that produces the `PRECONDITION_FAILED` above at the least convenient moment.

## Then

- [Configuration](configuration.md#topology--acemqtopology) — every field
- [Consumers](consumers.md#retries) — the retry ladder the rungs belong to
- [Streams](streams.md) — a queue type declared through the same setting
- [The library's topology page](https://acemq.org/acemq-python-amqp/topology.html)
