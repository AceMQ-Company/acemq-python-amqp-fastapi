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

"""The recipes docs/patterns.md and docs/streams.md tell people to write.

Every pattern in the library that this package has no setting for is reached one of
four ways: a lifespan composed *inside* this one, ``auto_start=false`` with
``start_consumer``, ``add_consumer`` with a wrapped handler, or ``register_codec``
under a name ``acemq.format`` can name. None of those is new API — which is exactly
why they are worth a test. A documented recipe with nothing asserting it is a recipe
that goes stale in the same silence as a comment.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import pytest
from acemq_amqp import (
    Ack,
    Action,
    AsyncHandler,
    Codec,
    Connection,
    Envelope,
    JsonCodec,
    Message,
    accept,
    codec_by_name,
    register_codec,
    reject,
)
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from acemq_fastapi import AceMQ, AceMQSettings, compose_lifespans
from fake_transport import FakeConnectionFactory


def build(**settings: Any) -> tuple[AceMQ, FakeConnectionFactory]:
    factory = FakeConnectionFactory()
    return (
        AceMQ(AceMQSettings.model_validate(settings), connection_factory=factory),
        factory,
    )


async def test_a_lifespan_composed_inside_reaches_the_open_connection() -> None:
    """docs/patterns.md: anything that needs the connection goes to the right."""
    acemq, _ = build()
    seen: list[Connection] = []

    @asynccontextmanager
    async def inner(app: FastAPI) -> AsyncIterator[dict[str, Any]]:
        # This is the whole point of the ordering: the connection is already open.
        seen.append(acemq.connection)
        try:
            yield {"inner": "open"}
        finally:
            seen.append(acemq.connection)

    app = FastAPI(lifespan=compose_lifespans(acemq.lifespan, inner))

    @app.get("/inner")
    async def read(request: Request) -> dict[str, str]:
        return {"inner": request.state.inner}

    with TestClient(app) as client:
        assert client.get("/inner").json() == {"inner": "open"}

    # Opened before the inner lifespan and still open while it closed, which is
    # what stops a relay or a stream reader failing against a closed connection
    # on every shutdown.
    assert len(seen) == 2
    assert seen[0] is seen[1]
    assert not acemq.started


async def test_auto_start_false_leaves_them_registered_for_start_consumer() -> None:
    """docs/patterns.md: how handlers get at something that needs the connection."""
    acemq, _ = build(consumer={"auto_start": False})

    @acemq.consumer("orders")
    async def handle(message: Message) -> Ack:
        return accept()

    @asynccontextmanager
    async def wiring(app: FastAPI) -> AsyncIterator[dict[str, Any]]:
        await acemq.start_consumer("orders")
        yield {}

    app = FastAPI(lifespan=compose_lifespans(acemq.lifespan, wiring))

    with TestClient(app):
        # Started by the inner lifespan, and in the registry the drain and the
        # health check read — which is the property this recipe is chosen for.
        assert set(acemq.consumers) == {"orders"}
        assert [r.name for r in acemq.registrations] == ["orders"]


async def test_the_lifespan_alone_starts_nothing_when_auto_start_is_off() -> None:
    acemq, _ = build(consumer={"auto_start": False})

    @acemq.consumer("orders")
    async def handle(message: Message) -> Ack:
        return accept()

    async with acemq.lifespan(FastAPI()):
        assert acemq.consumers == {}
        assert [r.name for r in acemq.registrations] == ["orders"]


async def test_add_consumer_registers_a_wrapped_handler() -> None:
    """docs/patterns.md: the decorator has nowhere to put a wrapper, add_consumer does."""
    acemq, factory = build()
    seen: list[str] = []

    async def charge(message: Message) -> Ack:
        seen.append(message.payload["payment"])
        return accept()

    def once(handler: AsyncHandler) -> AsyncHandler:
        """Stands in for patterns.idempotent, which needs a store."""
        done: set[str] = set()

        async def wrapped(message: Message) -> Ack:
            key = message.payload["payment"]
            if key in done:
                return accept()
            done.add(key)
            return await handler(message)

        return wrapped

    acemq.add_consumer("charges", once(charge), name="charges")

    async with acemq.lifespan(FastAPI()):
        assert set(acemq.consumers) == {"charges"}
        for _ in range(3):
            await factory.transport.deliver("charges", b'{"payment": "PAY-42"}')

    assert seen == ["PAY-42"], "the wrapper the registration was given did not run"


async def test_register_codec_gives_acemq_format_a_name_to_use() -> None:
    """docs/security.md and docs/serialization.md: the bridge to a constructed codec."""

    class Shouting:
        """A codec that is not built by a no-argument factory of the library's."""

        def __init__(self, delegate: Codec) -> None:
            self._delegate = delegate

        @property
        def content_type(self) -> str:
            return self._delegate.content_type

        def encode(self, payload: Any) -> bytes:
            return self._delegate.encode(payload).upper()

        def decode(self, body: bytes, content_type: str | None = None) -> Any:
            return self._delegate.decode(body.lower(), content_type)

        def can_decode(self, content_type: str | None) -> bool:
            return self._delegate.can_decode(content_type)

    register_codec("shouting", lambda: Shouting(JsonCodec()))
    try:
        assert isinstance(codec_by_name("shouting"), Shouting)
        # Resolved from the setting, which is what makes ACEMQ_FORMAT=shouting work.
        settings = AceMQSettings(format="shouting")
        assert isinstance(settings.build_codec(), Shouting)
    finally:
        # The registry is process-wide; leaving a test's codec in it would let a
        # later test pass for the wrong reason.
        register_codec("shouting", JsonCodec)


async def test_an_unknown_format_fails_the_start_rather_than_the_first_publish() -> None:
    """docs/serialization.md says the failure is at start-up. It has to be true."""
    acemq = AceMQ(AceMQSettings(format="no-such-codec"))
    with pytest.raises(LookupError):
        async with acemq.lifespan(FastAPI()):
            pass
    assert not acemq.started


async def test_a_stream_is_declarable_through_the_topology_setting() -> None:
    """docs/streams.md: args naming a queue type wins over the quorum default."""
    acemq, factory = build(
        topology={
            "queues": [
                {
                    "name": "readings",
                    "args": {"x-queue-type": "stream", "x-max-age": "1h"},
                }
            ]
        }
    )

    async with acemq.lifespan(FastAPI()):
        declared = factory.transport.queues["readings"]

    assert declared.args["x-queue-type"] == "stream"
    assert declared.args["x-max-age"] == "1h"
    assert declared.durable


def test_envelope_needs_no_arguments() -> None:
    """docs/testing.md builds one for a handler test."""
    envelope = Envelope()
    assert envelope.id
    assert Envelope().id != envelope.id


def test_reject_carries_the_exception_rather_than_comparing_equal() -> None:
    """docs/testing.md: why a handler test asserts on the action."""
    decision = reject(ValueError("no sku"))
    assert decision.action is Action.REJECT
    assert str(decision.error) == "no sku"
    # The reason the documented assertion is `.action` and not `== reject(...)`.
    assert decision != reject(ValueError("no sku"))
