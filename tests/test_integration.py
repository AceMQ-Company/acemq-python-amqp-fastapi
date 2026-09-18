# Copyright 2026 AceMQ.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Against a real broker, with the published ``acemq-amqp`` on the wire.

Marked ``integration``, so ``pytest -m "not integration"`` runs everything else on
a laptop with no Docker. They exist because the fake transport proves this package
does what it means to and proves nothing about whether the package it depends on
still behaves that way when it is installed from an index rather than a checkout.

    docker run -d --rm --name acemq-fastapi-test -p 5722:5672 rabbitmq:4-alpine
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Annotated, Any

import pytest
from acemq_amqp import Ack, HealthStatus, Message, Publisher, accept
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from acemq_fastapi import AceMQ, AceMQSettings, publishes
from acemq_fastapi.health import blocked_state
from conftest import BROKER_URL

pytestmark = pytest.mark.integration

Orders = Annotated[Publisher, Depends(publishes("acemq-fastapi-it-orders"))]


def settings(queue: str, **extra: Any) -> AceMQSettings:
    return AceMQSettings.model_validate(
        {
            "url": BROKER_URL,
            "client_name": "acemq-fastapi-tests",
            "topology": {"queues": [{"name": queue, "quorum": False, "dead_letter": True}]},
            **extra,
        }
    )


def portal(client: TestClient) -> Any:
    """The client's own event loop, which is where the handlers are.

    ``TestClient`` drives the application from a thread of its own; waiting for a
    handler from the test's thread means going through this rather than sleeping
    and hoping.
    """
    assert client.portal is not None
    return client.portal


async def test_the_published_package_connects_and_round_trips(queue_name: str) -> None:
    acemq = AceMQ(settings(queue_name))
    seen: list[dict[str, Any]] = []
    arrived = asyncio.Event()

    @acemq.consumer(queue_name)
    async def handle(message: Message) -> Ack:
        seen.append(message.payload)
        arrived.set()
        return accept()

    async with acemq.lifespan(FastAPI()):
        publisher = acemq.publisher("", queue_name)
        result = await publisher.send({"sku": "A-1"})
        assert result.confirmed
        await asyncio.wait_for(arrived.wait(), 10)
        await acemq.connection.delete_queue(queue_name)

    assert seen == [{"sku": "A-1"}]


async def test_send_all_confirms_a_batch(queue_name: str) -> None:
    """0.6.0 added ``send_all``; this is the integration that proves the published
    wheel has it and that it confirms every message."""
    acemq = AceMQ(settings(queue_name))
    arrived = asyncio.Semaphore(0)

    @acemq.consumer(queue_name)
    async def handle(message: Message) -> Ack:
        arrived.release()
        return accept()

    async with acemq.lifespan(FastAPI()):
        publisher = acemq.publisher("", queue_name)
        results = await publisher.send_all([{"n": n} for n in range(20)])
        assert len(results) == 20
        assert all(result.confirmed for result in results)
        for _ in range(20):
            await asyncio.wait_for(arrived.acquire(), 10)
        await acemq.connection.delete_queue(queue_name)


async def test_max_outstanding_publishes_is_honoured(queue_name: str) -> None:
    acemq = AceMQ(settings(queue_name, max_outstanding_publishes=4))
    async with acemq.lifespan(FastAPI()):
        assert acemq.connection.max_outstanding_publishes == 4
        publisher = acemq.publisher("", queue_name)
        await publisher.send_all([{"n": n} for n in range(12)])
        assert acemq.connection.outstanding_publishes == 0
        await acemq.connection.delete_queue(queue_name)


async def test_the_topology_reaches_the_broker(queue_name: str) -> None:
    acemq = AceMQ(settings(queue_name))
    async with acemq.lifespan(FastAPI()):
        assert await acemq.connection.queue_exists(queue_name)
        # dead_letter brought its own two with it.
        assert await acemq.connection.queue_exists(f"{queue_name}.dlq")
        assert await acemq.connection.queue_exists(f"{queue_name}.parked")
        for name in (queue_name, f"{queue_name}.dlq", f"{queue_name}.parked"):
            await acemq.connection.delete_queue(name)


async def test_health_against_a_live_broker(queue_name: str) -> None:
    acemq = AceMQ(settings(queue_name))

    @acemq.consumer(queue_name)
    async def handle(message: Message) -> Ack:
        return accept()  # pragma: no cover

    async with acemq.lifespan(FastAPI()):
        report = await acemq.health()
        await acemq.connection.delete_queue(queue_name)

    assert report.status is HealthStatus.UP
    assert report.parts["round-trip"] >= 0
    # The accessor still finds aiormq's blocked state on the versions installed
    # here. If this starts returning None, aio-pika or aiormq moved it and the
    # health check has quietly stopped being able to tell blocked from wedged.
    assert report.parts["blocked"] is False


async def test_blocked_state_reads_a_real_connection(queue_name: str) -> None:
    acemq = AceMQ(settings(queue_name))
    async with acemq.lifespan(FastAPI()):
        assert blocked_state(acemq.connection) is False
        await acemq.connection.delete_queue(queue_name)


def test_a_route_publishes_and_the_background_consumer_reads_it() -> None:
    """The shape this package exists for: an HTTP request in, a message out, a
    handler on the same loop picking it up, and the whole thing driven by
    FastAPI's own test client."""
    queue = "acemq-fastapi-it-orders"
    acemq = AceMQ(settings(queue))
    handled: list[dict[str, Any]] = []
    arrived = asyncio.Event()

    @acemq.consumer(queue)
    async def handle(message: Message) -> Ack:
        handled.append(message.payload)
        if len(handled) == 1:
            arrived.set()
        return accept()

    app = FastAPI(lifespan=acemq.lifespan)
    app.include_router(acemq.health_router())

    @app.post("/orders")
    async def place(orders: Orders) -> dict[str, str]:
        result = await orders.send({"id": "order-1"})
        return {"id": result.message_id}

    with TestClient(app) as client:
        assert client.get("/health/acemq").json()["status"] == "up"
        response = client.post("/orders")
        assert response.status_code == 200
        assert response.json()["id"]

        # The handler runs on the application's loop, which the test client drives
        # from its own thread. Wait on the portal rather than sleeping here.
        portal(client).call(lambda: asyncio.wait_for(arrived.wait(), 10))
        portal(client).call(lambda: acemq.connection.delete_queue(queue))

    assert handled == [{"id": "order-1"}]


def test_an_in_flight_handler_finishes_when_the_app_shuts_down() -> None:
    """The shutdown guarantee, end to end and against a real broker.

    The handler is still working when the test client leaves its ``with`` block —
    which is what an ASGI shutdown is. The drain waits for it, the message is
    acknowledged, and the queue is empty afterwards rather than holding a
    redelivery.
    """
    queue = f"acemq-fastapi-shutdown-{uuid.uuid4().hex[:8]}"
    acemq = AceMQ(settings(queue))
    started = asyncio.Event()
    finished = False

    @acemq.consumer(queue)
    async def handle(message: Message) -> Ack:
        nonlocal finished
        started.set()
        await asyncio.sleep(0.5)
        finished = True
        return accept()

    app = FastAPI(lifespan=acemq.lifespan)

    @app.post("/slow")
    async def slow(request: Any = None) -> dict[str, str]:
        publisher = acemq.publisher("", queue)
        result = await publisher.send({"work": "slow"})
        return {"id": result.message_id}

    with TestClient(app) as client:
        client.post("/slow")
        portal(client).call(lambda: asyncio.wait_for(started.wait(), 10))
        # Leaving the block runs the shutdown while the handler is mid-sleep.

    assert finished, "the drain returned before the handler did"

    async def count() -> int:
        checker = AceMQ(settings(queue))
        async with checker.lifespan(FastAPI()):
            depth = await checker.connection.message_count(queue)
            for name in (queue, f"{queue}.dlq", f"{queue}.parked"):
                await checker.connection.delete_queue(name)
            return depth

    assert asyncio.run(count()) == 0, "the message was acknowledged, not handed back"
