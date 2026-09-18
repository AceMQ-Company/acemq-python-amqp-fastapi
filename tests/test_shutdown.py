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

"""What a drain finishes, and what it gives back.

The four questions ``acemq-go-amqp/docs/lifecycle.md`` asks of a Go shutdown,
asked of this one. Three of the four answers are the same and one is not, and the
one that differs — the delivery fetched but never started — is the one worth
having a test for, because it is the one a reader will assume from the Go page.
"""

from __future__ import annotations

import asyncio
from typing import Any

from acemq_amqp import Ack, Message, accept
from fastapi import FastAPI

from acemq_fastapi import AceMQ, AceMQSettings
from fake_transport import FakeConnectionFactory


def build(**settings: Any) -> tuple[AceMQ, FakeConnectionFactory]:
    factory = FakeConnectionFactory()
    return (
        AceMQ(AceMQSettings.model_validate(settings), connection_factory=factory),
        factory,
    )


async def test_a_handler_in_flight_finishes_and_is_acknowledged() -> None:
    """Question one: a handler is running when shutdown starts.

    It finishes. The drain blocks on it, and the message is acknowledged rather
    than abandoned for the broker to give to somebody else.
    """
    acemq, factory = build()
    running = asyncio.Event()
    finished = False

    @acemq.consumer("orders")
    async def handle(message: Message) -> Ack:
        nonlocal finished
        running.set()
        await asyncio.sleep(0.25)
        finished = True
        return accept()

    manager = acemq.lifespan(FastAPI())
    await manager.__aenter__()
    await factory.transport.deliver("orders", b'{"id": 1}', message_id="in-flight")
    await asyncio.wait_for(running.wait(), 1.0)

    loop = asyncio.get_running_loop()
    started = loop.time()
    await manager.__aexit__(None, None, None)
    took = loop.time() - started

    assert finished, "the drain returned before the handler did"
    assert factory.transport.acked() == ["in-flight"]
    assert took >= 0.2, f"the drain did not wait for the handler; it took {took:.3f}s"


async def test_a_delivery_that_never_reached_a_handler_goes_back_to_the_broker() -> None:
    """Question two, and the answer that differs from Go.

    Go's ``Close`` runs every delivery the transport had already handed over
    through a handler, so a drain's work is bounded by prefetch. Python's hands
    them back instead: each is nacked with requeue, which is the broker's to give
    to another consumer. The difference is deliberate in the library — holding a
    delivery through a retry delay would make closing take as long as the
    schedule — and it means a large prefetch costs a redelivery here rather than a
    long drain.
    """
    acemq, factory = build(consumer={"concurrency": 1})
    first_running = asyncio.Event()
    release = asyncio.Event()
    handled: list[str] = []

    @acemq.consumer("orders")
    async def handle(message: Message) -> Ack:
        handled.append(message.envelope.id)
        first_running.set()
        await release.wait()
        return accept()

    manager = acemq.lifespan(FastAPI())
    await manager.__aenter__()

    await factory.transport.deliver("orders", b'{"n": 1}', message_id="first")
    await asyncio.wait_for(first_running.wait(), 1.0)
    # Queued behind the one worker, so it has arrived and no handler has it.
    await factory.transport.deliver("orders", b'{"n": 2}', message_id="second")
    await asyncio.sleep(0)

    draining = asyncio.create_task(manager.__aexit__(None, None, None))
    await asyncio.sleep(0.05)
    release.set()
    await asyncio.wait_for(draining, 2.0)

    assert factory.transport.acked() == ["first"]
    assert factory.transport.nacked() == ["second"]
    assert [s.requeue for s in factory.transport.settled if s.how == "nack"] == [True]
    assert len(handled) == 1, "the second message should never have reached the handler"


async def test_the_deadline_cancels_a_handler_that_will_not_finish() -> None:
    """Question four's consequence: the drain that runs out of time.

    The handler is cancelled and its message left unsettled — the broker will
    redeliver it. Nothing is dropped; some work may be done twice.
    """
    acemq, factory = build(consumer={"shutdown_timeout": 0.15})
    running = asyncio.Event()
    cancelled = False

    @acemq.consumer("orders")
    async def handle(message: Message) -> Ack:
        nonlocal cancelled
        running.set()
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            cancelled = True
            raise
        return accept()  # pragma: no cover

    await acemq.start()
    await factory.transport.deliver("orders", b"{}", message_id="stuck")
    await asyncio.wait_for(running.wait(), 1.0)

    report = await acemq.drain()
    await acemq.aclose()

    assert not report.finished
    assert report.abandoned == 1
    assert report.consumers == 1
    assert 0.1 <= report.waited < 1.0
    assert cancelled
    assert factory.transport.settled == [], "an abandoned message must not be settled"


async def test_a_drain_with_nothing_running_is_free() -> None:
    acemq, _ = build()
    await acemq.start()
    report = await acemq.drain()
    await acemq.aclose()
    assert report.finished
    assert report.consumers == 0
    assert report.waited == 0.0


async def test_stopping_one_consumer_drains_only_that_one() -> None:
    acemq, factory = build()
    finished: list[str] = []

    @acemq.consumer("orders")
    async def orders(message: Message) -> Ack:
        await asyncio.sleep(0.1)
        finished.append("orders")
        return accept()

    @acemq.consumer("audit")
    async def audit(message: Message) -> Ack:
        finished.append("audit")
        return accept()

    await acemq.start()
    await factory.transport.deliver("orders", b"{}", message_id="o1")
    await asyncio.sleep(0)
    await acemq.stop_consumer("orders")

    assert finished == ["orders"]
    assert set(acemq.consumers) == {"audit"}
    await acemq.drain()
    await acemq.aclose()


async def test_the_lifespan_closes_the_connection_even_when_the_drain_expires() -> None:
    acemq, factory = build(consumer={"shutdown_timeout": 0.1})

    @acemq.consumer("orders")
    async def handle(message: Message) -> Ack:
        await asyncio.sleep(30)
        return accept()  # pragma: no cover

    manager = acemq.lifespan(FastAPI())
    await manager.__aenter__()
    await factory.transport.deliver("orders", b"{}", message_id="stuck")
    await asyncio.sleep(0.02)
    await manager.__aexit__(None, None, None)

    assert factory.transport.closed, "the socket must go even when the drain fails"
    assert not acemq.started
