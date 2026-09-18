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

"""A broker that is a dictionary.

Enough of :class:`acemq_amqp.Transport` for the tests that are about *this*
package — the lifespan, the registry, the dependencies, the drain — and no more.
The tests that are about the wire are marked ``integration`` and use a real one.

It records settlements, because "the handler finished and the message was
acknowledged" and "the handler was cancelled and the message was left" are the
two outcomes the shutdown tests have to tell apart, and from outside they look
identical.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

from acemq_amqp import Connection, Delivery, ExchangeSpec, Outbound, PublishResult, QueueSpec
from acemq_amqp.transport import ConsumeSpec

#: What the library hands a transport to push a delivery into.
Deliver = Callable[[Delivery], Awaitable[None]]


@dataclass
class Settled:
    """One settlement, as the test reads it back."""

    message_id: str
    how: str  # "ack" or "nack"
    requeue: bool = False


class FakeSubscription:
    def __init__(self, transport: FakeTransport, queue: str) -> None:
        self._transport = transport
        self._queue = queue
        self.stopped = False
        self.released = False

    async def stop(self) -> None:
        self.stopped = True
        self._transport.subscriptions.pop(self._queue, None)

    async def close(self) -> None:
        self.released = True


class FakeCore:
    """The shape ``acemq_fastapi.health.blocked_state`` reaches through.

    aiormq keeps the blocked state in an event it name-mangles and does not
    expose, so the accessor goes looking for it by that name. This is the same
    shape, so the accessor is exercised here rather than only against a broker.
    """

    def __init__(self) -> None:
        self._Connection__connection_unblocked = asyncio.Event()
        self._Connection__connection_unblocked.set()


class FakeAioPika:
    """What aio-pika puts on the transport: a connection wrapping aiormq's."""

    def __init__(self) -> None:
        self.connection = FakeCore()


class FakeTransport:
    """A transport that keeps everything in memory."""

    def __init__(self) -> None:
        #: The same two-deep shape aio-pika has, for the blocked-state accessor.
        self.connection = FakeAioPika()
        self.queues: dict[str, QueueSpec] = {}
        self.exchanges: dict[str, ExchangeSpec] = {}
        self.bindings: list[tuple[str, str, str]] = []
        self.published: list[tuple[str, str, Outbound]] = []
        self.settled: list[Settled] = []
        self.subscriptions: dict[str, tuple[FakeSubscription, Deliver]] = {}
        self.closed = False
        #: Set to make every ``queue_exists`` hang, which is what a wedged
        #: connection looks like from here.
        self.wedged = asyncio.Event()

    def block(self) -> None:
        """The broker applying back pressure."""
        self.connection.connection._Connection__connection_unblocked.clear()

    def unblock(self) -> None:
        self.connection.connection._Connection__connection_unblocked.set()

    # --- Transport ---------------------------------------------------------

    async def declare_queue(self, name: str, spec: QueueSpec) -> None:
        self.queues[name] = spec

    async def declare_exchange(self, name: str, spec: ExchangeSpec) -> None:
        self.exchanges[name] = spec

    async def bind(self, queue: str, exchange: str, routing_key: str) -> None:
        self.bindings.append((queue, exchange, routing_key))

    async def publish(
        self, exchange: str, routing_key: str, message: Outbound
    ) -> PublishResult:
        self.published.append((exchange, routing_key, message))
        return PublishResult(message_id=message.message_id, confirmed=True, routed=True)

    async def consume(
        self,
        queue: str,
        spec: ConsumeSpec,
        deliver: Deliver,
    ) -> FakeSubscription:
        subscription = FakeSubscription(self, queue)
        self.subscriptions[queue] = (subscription, deliver)
        return subscription

    async def close(self) -> None:
        self.closed = True

    # --- QueueAdmin --------------------------------------------------------

    async def queue_exists(self, name: str) -> bool:
        if self.wedged.is_set():
            await asyncio.Event().wait()  # never returns, like a blocked broker
        return name in self.queues

    async def message_count(self, name: str) -> int:
        return 0

    async def delete_queue(self, name: str) -> None:
        self.queues.pop(name, None)

    # --- the test's own side ----------------------------------------------

    async def deliver(
        self,
        queue: str,
        body: bytes,
        *,
        message_id: str = "m1",
        content_type: str = "application/json",
        headers: Mapping[str, Any] | None = None,
        routing_key: str | None = None,
    ) -> None:
        """Hands one message to whatever is consuming ``queue``."""
        subscription = self.subscriptions.get(queue)
        if subscription is None:
            raise AssertionError(f"nothing is consuming {queue!r}")
        _, deliver = subscription

        async def ack() -> None:
            self.settled.append(Settled(message_id, "ack"))

        async def nack(requeue: bool) -> None:
            self.settled.append(Settled(message_id, "nack", requeue))

        await deliver(
            Delivery(
                body=body,
                content_type=content_type,
                routing_key=routing_key if routing_key is not None else queue,
                message_id=message_id,
                headers=dict(headers or {}),
                redelivered=False,
                ack=ack,
                nack=nack,
            )
        )

    def acked(self) -> list[str]:
        return [s.message_id for s in self.settled if s.how == "ack"]

    def nacked(self) -> list[str]:
        return [s.message_id for s in self.settled if s.how == "nack"]


@dataclass
class FakeConnectionFactory:
    """A connection factory for :class:`acemq_fastapi.AceMQ` that never dials."""

    transport: FakeTransport = field(default_factory=FakeTransport)
    #: How long ``connect`` takes, for the timeout test.
    delay: float = 0.0

    async def __call__(self, settings: Any) -> Connection:
        if self.delay:
            await asyncio.sleep(self.delay)
        return Connection(
            self.transport,
            origin=settings.client_name,
            retry=settings.build_retry(),
            prefetch=settings.prefetch,
            max_outstanding_publishes=settings.max_outstanding_publishes,
            confirm_timeout=settings.confirm_timeout,
        )
