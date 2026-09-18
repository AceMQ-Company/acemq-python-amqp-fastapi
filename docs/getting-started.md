# Getting started

## Install

```bash
pip install --extra-index-url https://acemq.org/pypi/simple/ acemq-amqp-fastapi
```

The extra index is needed because AceMQ is not on PyPI before 1.0. It brings
`acemq-amqp` with the RabbitMQ transport, FastAPI and `pydantic-settings` with it —
there is no second install to remember.

Python 3.10 or newer, which is the library's floor.

## A broker

```bash
docker run -d --rm --name acemq -p 5672:5672 -p 15672:15672 rabbitmq:4-management
```

## The whole application

```python
# app.py
from typing import Annotated

from acemq_amqp import Ack, Message, Publisher, accept
from acemq_fastapi import AceMQ, AceMQSettings, publishes
from fastapi import Depends, FastAPI

acemq = AceMQ(
    AceMQSettings(
        url="amqp://guest:guest@localhost:5672/",
        topology={"queues": [{"name": "orders", "dead_letter": True}]},
    )
)


@acemq.consumer("orders")
async def handle_order(message: Message) -> Ack:
    print("handling", message.payload)
    return accept()


app = FastAPI(lifespan=acemq.lifespan)
app.include_router(acemq.health_router())

Orders = Annotated[Publisher, Depends(publishes("orders"))]


@app.post("/orders")
async def place(orders: Orders) -> dict[str, str]:
    result = await orders.send({"sku": "A-1"})
    return {"id": result.message_id}
```

```bash
uvicorn app:app
curl -XPOST localhost:8000/orders
curl localhost:8000/health/acemq
```

The `POST` publishes, the consumer picks it up on the same event loop, and the log
shows the handler running before the response has finished being written.

## The four lines that matter

```python
acemq = AceMQ(...)                       # settings, and a registry. Connects to nothing.
@acemq.consumer("orders")                # a registration, held until startup
app = FastAPI(lifespan=acemq.lifespan)   # where the connection is opened and drained
Depends(publishes("orders"))             # a publisher, per request, from app.state
```

Nothing connects at import time. That is the point: `app.py` can be imported by a
test, by `alembic`, or by `--help` without a broker existing.

## Settings from the environment

The settings above can come from the environment instead, which is what a deployment
will do:

```bash
export ACEMQ_URL=amqps://broker.internal:5671/
export ACEMQ_USERNAME=orders-service
export ACEMQ_PASSWORD=...
export ACEMQ_CONSUMER__PREFETCH=32
export ACEMQ_CONSUMER__SHUTDOWN_TIMEOUT=20
```

```python
acemq = AceMQ()   # reads ACEMQ_*, and a .env file if one is beside the process
```

Every setting, and what each one does, is in [configuration](configuration.md).

## Then

- [The lifespan](lifespan.md) — including keeping the one your application already has
- [Consumers](consumers.md) — the decorator, concurrency, retries, scaling
- [Shutdown](shutdown.md) — what a drain finishes, measured
- [Health](health.md) — and why a blocked broker reports up
