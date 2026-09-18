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

"""The lifespan: what it opens, in what order, and what it composes with."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any

import pytest
from acemq_amqp import AceMQError, Ack, Message, accept
from fastapi import FastAPI

from acemq_fastapi import AceMQ, AceMQSettings, compose_lifespans
from fake_transport import FakeConnectionFactory


def build(**settings: Any) -> tuple[AceMQ, FakeConnectionFactory]:
    factory = FakeConnectionFactory()
    return (
        AceMQ(AceMQSettings.model_validate(settings), connection_factory=factory),
        factory,
    )


async def test_lifespan_opens_and_closes_the_connection() -> None:
    acemq, factory = build()
    app = FastAPI()
    assert not acemq.started
    async with acemq.lifespan(app):
        assert acemq.started
        assert app.state.acemq is acemq
        assert not factory.transport.closed
    assert not acemq.started
    assert factory.transport.closed


async def test_lifespan_applies_the_topology() -> None:
    acemq, factory = build(
        topology={
            "exchanges": [{"name": "orders", "kind": "topic"}],
            "queues": [{"name": "orders.new"}],
            "bindings": [{"queue": "orders.new", "exchange": "orders", "routing_key": "a.*"}],
        }
    )
    async with acemq.lifespan(FastAPI()):
        assert "orders" in factory.transport.exchanges
        assert "orders.new" in factory.transport.queues
        assert ("orders.new", "orders", "a.*") in factory.transport.bindings


async def test_lifespan_declares_nothing_when_nothing_was_declared() -> None:
    acemq, factory = build()
    async with acemq.lifespan(FastAPI()):
        assert factory.transport.exchanges == {}


async def test_consumers_start_with_the_lifespan_and_not_before() -> None:
    acemq, factory = build()

    @acemq.consumer("orders")
    async def handle(message: Message) -> Ack:
        return accept()

    assert [r.name for r in acemq.registrations] == ["orders"]
    assert acemq.consumers == {}
    async with acemq.lifespan(FastAPI()):
        assert set(acemq.consumers) == {"orders"}
        assert "orders" in factory.transport.subscriptions
    assert acemq.consumers == {}


async def test_the_decorator_returns_the_function_unchanged() -> None:
    acemq, _ = build()

    @acemq.consumer("orders")
    async def handle(message: Message) -> Ack:
        return accept()

    # Still an ordinary function: a test can call it with a Message and assert on
    # the Ack without a broker, a lifespan or an event loop belonging to anybody.
    blank = Message(
        payload={},
        envelope=None,  # type: ignore[arg-type]
        routing_key="",
        content_type=None,
        redelivered=False,
        body=b"",
    )
    assert await handle(blank) == accept()


async def test_two_consumers_on_one_queue_need_names() -> None:
    acemq, _ = build()

    @acemq.consumer("orders")
    async def first(message: Message) -> Ack:
        return accept()

    with pytest.raises(AceMQError, match="already a consumer called"):

        @acemq.consumer("orders")
        async def second(message: Message) -> Ack:
            return accept()

    @acemq.consumer("orders", name="orders-audit")
    async def third(message: Message) -> Ack:
        return accept()

    assert [r.name for r in acemq.registrations] == ["orders", "orders-audit"]


async def test_registering_after_the_lifespan_opened_is_refused() -> None:
    acemq, _ = build()
    async with acemq.lifespan(FastAPI()):
        with pytest.raises(AceMQError, match="already opened"):
            acemq.add_consumer("late", lambda message: accept())


async def test_auto_start_off_leaves_consumers_registered_and_stopped() -> None:
    acemq, _ = build(consumer={"auto_start": False})

    @acemq.consumer("orders")
    async def handle(message: Message) -> Ack:
        return accept()

    async with acemq.lifespan(FastAPI()):
        assert acemq.consumers == {}
        await acemq.start_consumer("orders")
        assert set(acemq.consumers) == {"orders"}
        await acemq.stop_consumer("orders")
        assert acemq.consumers == {}


async def test_consumer_settings_are_resolved_at_start_not_at_registration() -> None:
    acemq, _ = build(consumer={"prefetch": 11, "concurrency": 3})

    @acemq.consumer("orders")
    async def handle(message: Message) -> Ack:
        return accept()

    @acemq.consumer("audit", concurrency=1, prefetch=1)
    async def audit(message: Message) -> Ack:
        return accept()

    async with acemq.lifespan(FastAPI()):
        # The registration said nothing about concurrency, so it got the setting.
        assert acemq.consumers["orders"]._concurrency == 3
        assert acemq.consumers["audit"]._concurrency == 1


async def test_a_failure_during_start_leaves_nothing_running() -> None:
    acemq, factory = build()

    async def refuse(message: Message) -> Ack:
        return accept()

    acemq.add_consumer("orders", refuse)

    async def explode(queue: str, spec: Any, deliver: Any) -> Any:
        raise RuntimeError("the broker said no")

    factory.transport.consume = explode  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="the broker said no"):
        async with acemq.lifespan(FastAPI()):
            pass  # pragma: no cover - start raises before the body runs
    assert not acemq.started
    assert factory.transport.closed


async def test_connection_before_start_says_what_is_missing() -> None:
    acemq, _ = build()
    with pytest.raises(AceMQError, match="no connection yet"):
        _ = acemq.connection


async def test_start_gives_up_on_a_broker_that_does_not_answer() -> None:
    factory = FakeConnectionFactory(delay=5.0)
    acemq = AceMQ(
        AceMQSettings(connection_timeout=timedelta(seconds=0.05), url="amqp://nowhere:5672/"),
        connection_factory=factory,
    )
    with pytest.raises(AceMQError, match="did not answer within"):
        await acemq.start()


async def test_compose_lifespans_nests_outside_in() -> None:
    order: list[str] = []
    acemq, _ = build()

    @asynccontextmanager
    async def database(app: Any) -> AsyncIterator[dict[str, str]]:
        order.append("database up")
        yield {"database": "pool"}
        order.append("database down")

    @asynccontextmanager
    async def watching(app: Any) -> AsyncIterator[None]:
        order.append("acemq up" if acemq.started else "acemq not up")
        yield
        order.append("acemq down" if not acemq.started else "acemq still up")

    lifespan = compose_lifespans(database, acemq.lifespan, watching)
    async with lifespan(FastAPI()) as state:
        assert state["database"] == "pool"
        assert state["acemq"] is acemq
    assert order == ["database up", "acemq up", "acemq still up", "database down"]


async def test_compose_unwinds_what_it_opened_when_one_fails() -> None:
    closed: list[str] = []

    # try/finally rather than a line after the yield, because a lifespan that fails
    # on the way up unwinds the ones already open by throwing the exception in at
    # their yield — and a bare line after the yield is skipped when that happens.
    # AceMQ's own lifespan is written this way for exactly this reason.
    @asynccontextmanager
    async def first(app: Any) -> AsyncIterator[None]:
        try:
            yield
        finally:
            closed.append("first")

    @asynccontextmanager
    async def second(app: Any) -> AsyncIterator[None]:
        raise RuntimeError("no")
        yield  # pragma: no cover

    with pytest.raises(RuntimeError):
        async with compose_lifespans(first, second)(FastAPI()):
            pass  # pragma: no cover
    assert closed == ["first"]


async def test_handlers_run_on_the_applications_loop() -> None:
    acemq, factory = build()
    seen: list[asyncio.AbstractEventLoop] = []

    @acemq.consumer("orders")
    async def handle(message: Message) -> Ack:
        seen.append(asyncio.get_running_loop())
        return accept()

    async with acemq.lifespan(FastAPI()):
        await factory.transport.deliver("orders", b'{"id": 1}')
        await asyncio.sleep(0.05)

    assert seen == [asyncio.get_running_loop()]
