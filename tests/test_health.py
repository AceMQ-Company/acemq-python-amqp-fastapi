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

"""What a probe sees, and the one case where the obvious answer is wrong."""

from __future__ import annotations

import asyncio
from typing import Any

from acemq_amqp import Ack, HealthReport, HealthStatus, Message, accept, aggregate_health
from fastapi import FastAPI
from fastapi.testclient import TestClient

from acemq_fastapi import AceMQ, AceMQSettings
from fake_transport import FakeConnectionFactory

#: The sentence every AceMQ library writes for a blocked connection, spelled out
#: here rather than imported from ``acemq_amqp``. It is an alert-rule contract, so
#: a library release that changes the wording has to break this test and be
#: written down, not be followed silently.
BLOCKED = "the broker has blocked this connection; publishing is paused"


def build(**settings: Any) -> tuple[AceMQ, FakeConnectionFactory]:
    factory = FakeConnectionFactory()
    return (
        AceMQ(AceMQSettings.model_validate(settings), connection_factory=factory),
        factory,
    )


async def test_a_working_connection_is_up() -> None:
    acemq, _ = build()
    async with acemq.lifespan(FastAPI()):
        report = await acemq.health()
    assert report.status is HealthStatus.UP
    assert report.healthy


async def test_no_connection_is_down_and_says_why() -> None:
    acemq, _ = build()
    report = await acemq.health()
    assert report.status is HealthStatus.DOWN
    assert "lifespan is not running" in report.detail


async def test_a_blocked_connection_is_up_with_the_reason() -> None:
    """The whole point of this check.

    A blocked connection is the broker protecting itself. Reporting it down gets
    the instance restarted into the same blocked broker, having thrown away
    whatever it was holding — which helps nobody. Same call the Spring Boot
    starter makes.
    """
    acemq, factory = build()
    async with acemq.lifespan(FastAPI()):
        factory.transport.block()
        report = await acemq.health()

    assert report.status is HealthStatus.UP
    assert report.healthy
    assert report.detail == BLOCKED
    assert report.parts["blocked"] is True


async def test_a_blocked_broker_that_stops_answering_is_still_up() -> None:
    """Blocked and wedged at once, which is what it actually looks like.

    RabbitMQ stops reading from a blocked connection's socket, so the round trip
    a health probe makes does not come back. The library reads the blocked state
    before it probes and skips the probe entirely, so this answers immediately
    rather than after the timeout — which is why the wedged transport here, whose
    ``queue_exists`` never returns, does not hold the check up.
    """
    acemq, factory = build(health={"timeout": 0.1})
    async with acemq.lifespan(FastAPI()):
        factory.transport.wedged.set()
        factory.transport.block()
        loop = asyncio.get_running_loop()
        started = loop.time()
        report = await acemq.health()
        took = loop.time() - started

    assert report.status is HealthStatus.UP
    assert report.detail == BLOCKED
    assert report.parts["blocked"] is True
    assert took < 0.1, "a blocked broker is not probed at all"


async def test_a_broker_that_does_not_answer_is_down_within_the_timeout() -> None:
    acemq, factory = build(health={"timeout": 0.1})
    async with acemq.lifespan(FastAPI()):
        factory.transport.wedged.set()
        loop = asyncio.get_running_loop()
        started = loop.time()
        report = await acemq.health()
        took = loop.time() - started

    assert report.status is HealthStatus.DOWN
    assert "did not answer within" in report.detail
    assert took < 1.0, "a probe that hangs is a pod that never comes back"


async def test_a_stalled_consumer_is_degraded_not_down() -> None:
    acemq, _ = build()

    @acemq.consumer("orders")
    async def handle(message: Message) -> Ack:
        return accept()  # pragma: no cover

    async with acemq.lifespan(FastAPI()):
        # Kill the workers behind the consumer's back: from outside, a consumer
        # whose workers have died looks exactly like a quiet queue.
        consumer = acemq.consumers["orders"]
        for worker in consumer._workers:
            worker.cancel()
        await asyncio.sleep(0.01)
        report = await acemq.health()

    assert report.status is HealthStatus.DEGRADED
    assert report.healthy, "degraded still passes a readiness probe"


async def test_the_report_names_the_consumers() -> None:
    acemq, _ = build()

    @acemq.consumer("orders", name="orders-main")
    async def handle(message: Message) -> Ack:
        return accept()  # pragma: no cover

    async with acemq.lifespan(FastAPI()):
        report = await acemq.health()
    assert report.parts["registered"] == ["orders-main"]
    assert report.parts["consumers"] == {"orders-main": 0}


async def test_the_check_composes_into_an_aggregate() -> None:
    acemq, _ = build()

    class Database:
        name = "database"

        async def check(self) -> HealthReport:
            return HealthReport(HealthStatus.UP)

    async with acemq.lifespan(FastAPI()):
        combined = await aggregate_health(acemq.health_check(), Database())

    assert combined.status is HealthStatus.UP
    assert set(combined.parts) == {"acemq", "database"}


async def test_the_aggregate_goes_down_when_this_one_does() -> None:
    acemq, _ = build()

    class Database:
        name = "database"

        async def check(self) -> HealthReport:
            return HealthReport(HealthStatus.UP)

    combined = await aggregate_health(acemq.health_check(), Database())
    assert combined.status is HealthStatus.DOWN


def test_the_route_answers_200_when_up() -> None:
    acemq, _ = build()
    app = FastAPI(lifespan=acemq.lifespan)
    app.include_router(acemq.health_router())
    with TestClient(app) as client:
        response = client.get("/health/acemq")
    assert response.status_code == 200
    assert response.json()["status"] == "up"
    assert response.json()["healthy"] is True


def test_the_route_answers_200_when_blocked() -> None:
    acemq, factory = build()
    app = FastAPI(lifespan=acemq.lifespan)
    app.include_router(acemq.health_router())
    with TestClient(app) as client:
        factory.transport.block()
        response = client.get("/health/acemq")
    assert response.status_code == 200
    assert response.json()["parts"]["blocked"] is True


def test_the_route_answers_503_when_down() -> None:
    acemq, factory = build(health={"timeout": 0.1})
    app = FastAPI(lifespan=acemq.lifespan)
    app.include_router(acemq.health_router())
    with TestClient(app) as client:
        factory.transport.wedged.set()
        response = client.get("/health/acemq")
    assert response.status_code == 503
    assert response.json()["healthy"] is False


def test_the_route_takes_the_path_from_the_settings() -> None:
    acemq, _ = build(health={"path": "/readyz/mq"})
    app = FastAPI(lifespan=acemq.lifespan)
    app.include_router(acemq.health_router())
    with TestClient(app) as client:
        assert client.get("/readyz/mq").status_code == 200


async def test_blocked_is_null_when_it_cannot_be_asked() -> None:
    """``None`` is not ``False``: it means nobody could look.

    The connection answers all three, and this package's job is to pass the third
    one through rather than round it down. A report saying ``blocked: null`` is
    more use in an incident than one saying ``false`` because nothing looked, so
    ``_parts`` deliberately has no ``blocked`` key of its own to overwrite it with.
    """
    acemq, factory = build()
    async with acemq.lifespan(FastAPI()):
        factory.transport.mute()
        report = await acemq.health()

    assert report.parts["blocked"] is None
    assert report.status is HealthStatus.UP, "unknown is not a reason to fail a probe"


async def test_the_timeout_is_the_brokers_deadline_not_a_wrapper() -> None:
    """The setting reaches the library's probe.

    It used to be an ``asyncio.wait_for`` around a probe that had no deadline of
    its own. The probe has had one since acemq-amqp 0.7.0, so the setting is handed
    down instead: one deadline, owned by the thing that knows what to do when it
    runs out.
    """
    acemq, factory = build(health={"timeout": 0.25})
    async with acemq.lifespan(FastAPI()):
        factory.transport.wedged.set()
        loop = asyncio.get_running_loop()
        started = loop.time()
        report = await acemq.health()
        took = loop.time() - started

    assert report.status is HealthStatus.DOWN
    assert "within 0.25s" in report.detail, "the library's own deadline, said in its words"
    assert 0.25 <= took < 1.0
