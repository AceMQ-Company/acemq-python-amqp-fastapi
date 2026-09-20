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

One of them needs to reconfigure that broker, and skips when it cannot::

    export ACEMQ_TEST_BROKER_CTL="docker exec acemq-fastapi-test rabbitmqctl"
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import shlex
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import Annotated, Any

import pytest
from acemq_amqp import Ack, HealthStatus, Message, Publisher, PublishResult, accept
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from acemq_fastapi import AceMQ, AceMQSettings, publishes
from conftest import BROKER_URL

pytestmark = pytest.mark.integration

Orders = Annotated[Publisher, Depends(publishes("acemq-fastapi-it-orders"))]

#: The sentence every AceMQ library writes for a blocked connection. Spelled out
#: rather than imported, because it is an alert-rule contract: a library release
#: that rewords it has to break this and be written down, not be followed quietly.
BLOCKED = "the broker has blocked this connection; publishing is paused"

#: How to run ``rabbitmqctl`` against the broker these tests are pointed at, as
#: the command that fronts it — ``docker exec acemq-fastapi-it rabbitmqctl``, or
#: just ``rabbitmqctl`` where the broker is local. Unset where the broker is not
#: the tests' to reconfigure, and the blocked test skips rather than pretends:
#: nothing on the client side can make a broker block a connection, so a test
#: that never sees a real block is a test of nothing. This is the same lever the
#: library's own suite uses, under the same variable.
BROKER_CONTROL = os.environ.get("ACEMQ_TEST_BROKER_CTL", "")

needs_control_of_the_broker = pytest.mark.skipif(
    not BROKER_CONTROL, reason="ACEMQ_TEST_BROKER_CTL is not set"
)


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
    # ``False`` rather than ``None``: the library could ask the transport and the
    # answer was no. If this ever reads None against a live broker, the supported
    # accessor has stopped working on the installed aio-pika and the health check
    # has quietly lost the ability to tell a blocked broker from a wedged one.
    assert report.parts["blocked"] is False


async def test_the_connection_answers_the_blocked_question(queue_name: str) -> None:
    """The route the health check reads, asked directly.

    ``Connection.blocked`` replaced a walk down private aiormq attributes in
    acemq-amqp 0.7.0. This fails if the published wheel cannot answer it.
    """
    acemq = AceMQ(settings(queue_name))
    async with acemq.lifespan(FastAPI()):
        assert acemq.connection.blocked is False
        assert acemq.connection.blocked_reason is None
        await acemq.connection.delete_queue(queue_name)


@needs_control_of_the_broker
def test_the_route_answers_200_when_the_broker_has_really_blocked_it() -> None:
    """The one that needs a broker under a real alarm.

    A connection nobody has blocked proves nothing here: RabbitMQ only sends
    ``connection.blocked`` to a connection that publishes while an alarm is on,
    and what makes a probe hang is the broker then stopping reading the socket.
    Neither can be faked from this side. So the watermark goes down far enough to
    raise the memory alarm, a publish provokes the block, and the watermark goes
    back before the connection is closed — closing a blocked connection waits on
    a broker that is not reading.
    """
    queue = f"acemq-fastapi-blocked-{uuid.uuid4().hex[:8]}"
    acemq = AceMQ(settings(queue))
    app = FastAPI(lifespan=acemq.lifespan)
    app.include_router(acemq.health_router())

    with TestClient(app) as client:
        call = portal(client).call
        assert client.get("/health/acemq").json()["parts"]["blocked"] is False

        call(lambda: rabbitmqctl("set_vm_memory_high_watermark", "0.0001"))
        # The publish that causes the block is also the first thing the broker
        # will not confirm, so it is left running and cancelled once the alarm
        # is off rather than awaited here.
        publishing = call(lambda: _publish_in_background(acemq, queue))
        try:
            call(lambda: until(lambda: _blocked(acemq), "the broker blocked it"))

            assert acemq.connection.blocked is True
            # Honest rather than invented: RabbitMQ did send a reason and aiormq
            # did not keep it. A string here means a client release started
            # keeping it, and the library should be handing it over.
            assert acemq.connection.blocked_reason is None

            started = time.monotonic()
            response = client.get("/health/acemq")
            answered_in = time.monotonic() - started
        finally:
            call(lambda: rabbitmqctl("set_vm_memory_high_watermark", "0.4"))
            call(lambda: until(lambda: _unblocked(acemq), "the broker unblocked it"))
            call(lambda: _abandon(publishing))
            call(lambda: _delete(acemq, queue))

    # Up, and 200, while genuinely blocked. An orchestrator that took this
    # instance out of rotation would move it to the same blocked broker, having
    # thrown away whatever it was holding.
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "up"
    assert body["healthy"] is True
    assert body["parts"]["blocked"] is True
    # The exact sentence, because it is what an alert rule matches on.
    assert body["detail"] == BLOCKED
    # And it answered rather than hanging on a round trip the broker is not
    # reading: the probe is skipped once the state is known, so this comes back
    # well inside the five seconds acemq.health.timeout would have allowed it.
    assert answered_in < 1.0


async def rabbitmqctl(*arguments: str) -> None:
    """Runs one ``rabbitmqctl`` command against the broker under test.

    Awaited on the application's loop rather than run inline, so the connection
    being tested keeps reading frames while the broker is reconfigured — the
    notification this exists to provoke arrives on it.
    """
    process = await asyncio.create_subprocess_exec(
        *shlex.split(BROKER_CONTROL),
        *arguments,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    output, _ = await process.communicate()
    assert process.returncode == 0, (
        f"{BROKER_CONTROL} {' '.join(arguments)} failed: {output.decode(errors='replace')}"
    )


async def until(
    check: Callable[[], Awaitable[bool]], what: str, timeout: float = 15.0
) -> None:
    """Waits for something the broker does in its own time."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if await check():
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"waited {timeout}s and {what} never happened")


async def _blocked(acemq: AceMQ) -> bool:
    return acemq.connection.blocked is True


async def _unblocked(acemq: AceMQ) -> bool:
    return acemq.connection.blocked is False


async def _publish_in_background(acemq: AceMQ, queue: str) -> asyncio.Task[PublishResult]:
    return asyncio.create_task(acemq.publisher("", queue).send({"id": "7"}))


async def _abandon(publishing: asyncio.Task[PublishResult]) -> None:
    publishing.cancel()
    with contextlib.suppress(asyncio.CancelledError, Exception):
        await publishing


async def _delete(acemq: AceMQ, queue: str) -> None:
    for name in (queue, f"{queue}.dlq", f"{queue}.parked"):
        with contextlib.suppress(Exception):
            await acemq.connection.delete_queue(name)


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
