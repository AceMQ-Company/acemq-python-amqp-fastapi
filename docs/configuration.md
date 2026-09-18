# Configuration

Every setting reads from the environment with the prefix `ACEMQ_` and `__` between
levels, and from a `.env` file beside the process. They can also be written down:

```python
acemq = AceMQ()                                        # from the environment
acemq = AceMQ(AceMQSettings(url="amqp://host:5672/"))  # written down
acemq = AceMQ(settings.acemq)                          # nested in the app's own settings
```

`AceMQSettings` is a `pydantic-settings` model. That is a dependency this package took
deliberately: FastAPI already brings pydantic, so what it adds is one small package
whose whole job is reading environment variables into a model — and the alternative is
this repository owning a parser for durations, nested keys and `.env` files forever.
The Spring starter does not write its own binder either.

**Durations** accept a bare number of seconds (`30`), ISO 8601 (`PT30S`) or
`H:MM:SS`. Pydantic refuses a bare number in a string, which is the one spelling every
environment variable uses, so this package coerces it.

## Connection

| Setting | Default | |
|---|---|---|
| `acemq.url` | `amqp://localhost:5672/` | `amqps://` is encrypted, `amqp://` is not |
| `acemq.username` | — | The login, kept out of the URL so a password stays out of a connection string that ends up in a log |
| `acemq.password` | — | A `SecretStr`; it does not appear in a `repr` |
| `acemq.virtual_host` | — | Replaces whatever path the URL carried |
| `acemq.client_name` | `acemq@{hostname}` | What is stamped on published messages |
| `acemq.format` | `json` | One of `acemq_amqp.codec_names()` |
| `acemq.prefetch` | `100` | Unacknowledged messages a consumer holds |
| `acemq.publisher_confirms` | `true` | **Cannot be turned off.** See below |
| `acemq.max_outstanding_publishes` | `1000` | Publishes that may be waiting at once |
| `acemq.confirm_timeout` | `30s` | How long a publish waits for room before raising |
| `acemq.connection_timeout` | `10s` | How long the lifespan gives the broker to answer |

`publisher_confirms=false` is **refused**, with a message saying why, rather than
accepted and ignored. The Python library publishes with confirms always; a setting that
silently did nothing would leave a service believing it had traded safety for
throughput and having neither.

## TLS — `acemq.tls.*`

Only consulted for an `amqps://` URL. The library refuses TLS settings against a
plaintext URL rather than ignoring them, and this passes that refusal through.

| Setting | Default | |
|---|---|---|
| `certificate_authority` | — | A PEM file. Naming one **replaces** the machine's trust store rather than adding to it |
| `client_certificate` | — | A PEM certificate to present |
| `client_key` | the certificate file | The PEM private key |
| `client_key_password` | — | The passphrase, when the key has one |
| `server_name` | the host in the URL | The name to check the certificate against |
| `verification` | `certificate` | Or `nothing-at-all`, spelled that way so a config file cannot hide what it is giving up behind a `verify: false` |
| `allow_development_certificates` | `false` | A laptop's settings copied into a deployment should fail loudly |

## Consumers — `acemq.consumer.*`

The defaults every registered consumer starts with; a registration overrides any of
them per queue.

| Setting | Default | |
|---|---|---|
| `prefetch` | `100` | Also the bound on how many deliveries a shutdown hands back — see [shutdown](shutdown.md) |
| `concurrency` | `1` | One keeps a queue's messages in order |
| `declare` | `true` | Declare `{queue}.dlq`, `{queue}.parked` and the rungs before subscribing |
| `auto_start` | `true` | Off registers everything and starts nothing |
| `shutdown_timeout` | `20s` | How long a drain gets before the handlers are cancelled |

### `acemq.consumer.retry.*`

Off until asked for, matching the Spring starter: a ladder declares rung queues on the
broker.

| Setting | Default | |
|---|---|---|
| `enabled` | `false` | |
| `kind` | `exponential` | Doubling with 20% jitter, or `fixed` |
| `max_attempts` | `3` | Total deliveries, the first included |
| `initial_delay` | `1s` | The wait before the second attempt |
| `max_delay` | `1m` | The ceiling |
| `give_up_after` | — | Give up on a message older than this, whatever attempt it is on |
| `wait_in_broker_from` | — | Waits at or above this live on a rung queue rather than in this process. **Set it** — see [shutdown](shutdown.md) |

## Topology — `acemq.topology.*`

Declaring nothing is the default and is a real answer: a service that only consumes
queues somebody else owns should not create them.

```python
AceMQSettings(
    topology={
        "exchanges": [{"name": "events", "kind": "topic"}],
        "queues": [{"name": "orders", "dead_letter": True}],
        "bindings": [
            {"queue": "orders", "exchange": "events", "routing_key": "order.*"}
        ],
    }
)
```

A queue is `durable` and, being durable, a **quorum** queue unless it is asked to be
something else. That is not a preference: the queue type is an argument the broker
compares, so a Java service and a Python service that disagree about `orders` cannot
both consume it. `quorum: false` declares a classic queue.

`dead_letter: true` brings `{name}.dlq` and `{name}.parked` with it and points the
broker at them.

`declare_retry_rungs` is on, so the rung queues the consumer retry policy needs are
declared with each queue. A policy whose rungs are missing falls back to waiting in
this process, which is the slow drain nobody expects.

## Health — `acemq.health.*`

| Setting | Default | |
|---|---|---|
| `path` | `/health/acemq` | The route `acemq.health_router()` builds |
| `timeout` | `5s` | How long the broker gets to answer. Not optional: the library's own probe has no deadline |
| `unhealthy_status_code` | `503` | Answered only for *down*. Degraded and blocked answer 200 |

See [health](health.md) for why blocked answers 200.
