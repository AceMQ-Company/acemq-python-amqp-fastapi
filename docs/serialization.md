# Serialization

What goes on the wire is a codec's decision, and there are three places to make it: the
`acemq.format` setting for the whole connection, a `codec=` on one consumer or one
publisher, or a codec built in `connection_factory`. This page is about which to use, and
about the one thing that is genuinely FastAPI's problem — pydantic models on both sides of
the wire.

## `acemq.format`

```bash
export ACEMQ_FORMAT=json
```

The setting names a codec in the library's registry. `AceMQSettings.build_codec()`
resolves it inside `AceMQ.start()`, so a name nothing is registered under fails the
start-up with a `KeyError` listing the names that are — rather than at the first publish,
in a route, under load.

Three names are always there, because nothing has to be installed for them:

| | | |
|---|---|---|
| `json` | `application/json` | The default, and what every AceMQ library has |
| `text` | `text/plain` | |
| `bytes` | `application/octet-stream` | Claims **every** content type when reading |

Three more register themselves when their module is imported:

```bash
pip install "acemq-amqp[yaml]"     # and [toml]; xml needs nothing
```

```python
import acemq_amqp.codecs.toml   # noqa: F401 -- registers "toml"
import acemq_amqp.codecs.xml    # noqa: F401 -- registers "xml"
import acemq_amqp.codecs.yaml   # noqa: F401 -- registers "yaml"
```

```bash
export ACEMQ_FORMAT=yaml
```

The import has to happen before the lifespan opens, and it has to be somewhere ruff will
not delete — the `noqa` above, or a `register_codec` call of your own. Importing them from
the module that builds the `AceMQ` object is the simplest place.

**Protobuf and Avro cannot be named by `acemq.format`,** and that is a property of the
formats rather than an omission. Neither format's bytes describe themselves, so a codec
needs a message type or a schema before it can read anything, and there is nothing
sensible for a no-argument factory to hand back. They are constructed —
`acemq_amqp.codecs.protobuf.ProtobufCodec` and `acemq_amqp.codecs.avro.AvroCodec` — and
reach the connection through `connection_factory` or a `codec=` per consumer. The same is
true of the encrypted and claim-check codecs, which wrap another one; the
[`register_codec` trick](security.md#one-format-for-everything) is how those get a name
anyway.

## Reading more than one format

A queue during a migration, or one several producers write to, wants a `CompositeCodec`:

```python
from acemq_amqp import CompositeCodec, JsonCodec
from acemq_amqp.codecs.yaml import YamlCodec

register_codec("json-or-yaml", lambda: CompositeCodec(JsonCodec(), YamlCodec()))
```

```bash
export ACEMQ_FORMAT=json-or-yaml
```

The first codec is what it *writes*; all of them are offered a message to read, in order,
and the first that claims the content type and succeeds wins. Order matters when two
overlap — `BytesCodec` answers for everything, so nothing after it is ever reached.

## Per consumer, per publisher

```python
@acemq.consumer("readings", codec=AvroCodec(reading_schema))
async def handle(message: Message) -> Ack: ...


Readings = Annotated[Publisher, Depends(publishes("readings", codec=AvroCodec(reading_schema)))]
```

Both take a `codec=` and it overrides the connection's for that consumer or that
destination only. Build the codec once at module scope and pass the same object: the
publisher cache keys on the codec's identity, so a fresh `AvroCodec(...)` in the
`Depends(...)` call would be a fresh publisher every time the module is imported — not per
request, but it is still a pointless copy.

## Pydantic on the way out

```python
@app.post("/orders")
async def place(order: Order, orders: Orders) -> dict[str, str]:
    result = await orders.send(order.model_dump())
    return {"id": result.message_id}
```

`model_dump()`, not the model. `JsonCodec` encodes dataclasses through
`dataclasses.asdict` and hands everything else to `json.dumps` unchanged — and
`json.dumps` does not know what a `BaseModel` is. Passing the model raises `TypeError`
from inside the publish, which is a confusing place to read about a serialization problem.

`model_dump(mode="json")` is the one to reach for when the model has a `datetime`, a
`UUID`, a `Decimal` or an `Enum` in it: plain `model_dump()` leaves those as Python
objects and `json.dumps` refuses them too. It costs nothing when there are none, so it is
a reasonable default to write everywhere.

## Pydantic on the way in

`message.payload` is a dict, and deliberately: the wire says nothing about which class was
meant, so the library will not guess. Validate at the edge of the handler, where a bad
message becomes a decision rather than an `AttributeError` four frames in:

```python
from pydantic import ValidationError


@acemq.consumer("orders")
async def handle(message: Message) -> Ack:
    try:
        order = Order.model_validate(message.payload)
    except ValidationError as invalid:
        return reject(invalid)
    await warehouse.reserve(order)
    return accept()
```

`reject` dead-letters without retrying, which is right: a message that does not validate
will not validate on the fourth attempt either. `park()` instead when the message should
be set aside for somebody to look at rather than filed as a failure — the difference is
`{queue}.dlq` versus `{queue}.parked`, and both are declared for you when the queue has
`dead_letter: true`.

`reject` and `park` take a `BaseException`, not a string. Passing the `ValidationError`
itself is what puts the reason on the dead-lettered message where an operator will read
it.

**A body the codec cannot decode at all never reaches the handler.** The library parks it
before then, because a handler cannot be asked to decide about a payload that does not
exist. That is one more reason `declare` is on by default: a consumer that parks into a
queue nobody declared has only moved the disappearance somewhere else.

## Schema evolution

The library carries a registry — `SchemaRegistry`, with `SqlSchemaRegistry` and
`InMemorySchemaRegistry` behind it — for the case where the schema a message was written
against has to be recoverable later. There is no `acemq.*` setting for it: it is an object
with a database handle.

```python
from acemq_amqp.patterns import SqlSchemaRegistry, create_schema

registry = SqlSchemaRegistry(connections)
definition = await registry.register("orders", "avro", schema_text)
```

`register` returns a `SchemaDefinition` carrying an `id`, a `version` and a `fingerprint`,
and registering the same definition twice gives the same version back rather than a new
one. `latest(subject)`, `by_id(id)` and `versions(subject)` read it.

In a FastAPI application the useful shape is a route, so the schemas a service writes are
discoverable without a database client:

```python
@app.get("/schemas/{subject}")
async def schema(subject: str) -> dict[str, object]:
    latest = await registry.latest(subject)
    return {"version": latest.version, "fingerprint": latest.fingerprint,
            "definition": latest.definition}
```

The registry does not enforce compatibility and does not claim to. What it gives is the
ability to answer "which schema was this written against" for a message already on a
queue, which is the question that matters when a consumer starts refusing messages after a
deployment. Whether version 4 may follow version 3 is a decision, and the library leaves
it where decisions belong.

`create_schema(connections)` writes the tables for development; `schema_ddl()` returns the
statements for a migration. Both also cover the idempotency and outbox tables — see
[patterns](patterns.md#idempotent-consumer).

## Then

- [Patterns](patterns.md) — claim check and encryption, which are also codecs
- [Security](security.md#encrypting-the-payload) — encrypting the body
- [The library's serialization page](https://acemq.org/acemq-python-amqp/serialization.html) — every format, and what each one reads
