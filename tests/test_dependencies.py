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

"""Publishing from a route, through ``Depends``."""

from __future__ import annotations

import json
from typing import Annotated, Any

import pytest
from acemq_amqp import AceMQError, Connection, Publisher
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from acemq_fastapi import AceMQ, AceMQConnection, AceMQSettings, publishes
from fake_transport import FakeConnectionFactory


def build(**settings: Any) -> tuple[AceMQ, FakeConnectionFactory]:
    factory = FakeConnectionFactory()
    return (
        AceMQ(AceMQSettings.model_validate(settings), connection_factory=factory),
        factory,
    )


# At module level, and that is not incidental. With ``from __future__ import
# annotations`` a route's annotations are strings, and FastAPI resolves them
# against the module's globals — an alias defined inside a test function cannot be
# found and the route ends up asking for a request body it never wanted. The same
# applies to an application: put the aliases beside the imports.
Orders = Annotated[Publisher, Depends(publishes("orders"))]
Audit = Annotated[Publisher, Depends(publishes("audit", "events"))]


def test_a_route_publishes_through_a_dependency() -> None:
    acemq, factory = build()
    app = FastAPI(lifespan=acemq.lifespan)

    @app.post("/orders")
    async def place(orders: Orders) -> dict[str, str]:
        result = await orders.send({"sku": "A-1"})
        return {"id": result.message_id}

    with TestClient(app) as client:
        response = client.post("/orders")

    assert response.status_code == 200
    assert response.json()["id"]
    assert len(factory.transport.published) == 1
    exchange, routing_key, outbound = factory.transport.published[0]
    assert (exchange, routing_key) == ("", "orders")
    assert json.loads(outbound.body) == {"sku": "A-1"}


def test_the_publisher_is_built_once_and_reused() -> None:
    acemq, _ = build()
    app = FastAPI(lifespan=acemq.lifespan)
    seen: list[int] = []

    @app.get("/who")
    async def who(orders: Orders) -> dict[str, int]:
        seen.append(id(orders))
        return {"id": id(orders)}

    with TestClient(app) as client:
        client.get("/who")
        client.get("/who")

    assert len(set(seen)) == 1


def test_different_destinations_get_different_publishers() -> None:
    acemq, factory = build()
    app = FastAPI(lifespan=acemq.lifespan)

    @app.post("/both")
    async def both(
        orders: Orders,
        audit: Audit,
    ) -> dict[str, bool]:
        await orders.send({"n": 1})
        await audit.send({"n": 2})
        return {"ok": orders is not audit}

    with TestClient(app) as client:
        assert client.post("/both").json() == {"ok": True}

    assert [(e, k) for e, k, _ in factory.transport.published] == [
        ("", "orders"),
        ("events", "audit"),
    ]


def test_the_connection_dependency_hands_over_the_live_connection() -> None:
    acemq, _ = build()
    app = FastAPI(lifespan=acemq.lifespan)

    @app.get("/depth")
    async def depth(connection: AceMQConnection) -> dict[str, Any]:
        return {"same": connection is acemq.connection, "closed": connection.closed}

    with TestClient(app) as client:
        assert client.get("/depth").json() == {"same": True, "closed": False}


def test_an_application_without_the_lifespan_says_so() -> None:
    app = FastAPI()  # no lifespan=acemq.lifespan, which is the mistake

    @app.get("/boom")
    async def boom(connection: AceMQConnection) -> dict[str, str]:
        return {"never": "here"}  # pragma: no cover

    with TestClient(app) as client, pytest.raises(AceMQError, match=r"no AceMQ on app\.state"):
        client.get("/boom")


def test_two_applications_in_one_process_keep_their_own() -> None:
    """``app.state`` rather than a module global, which is why this works."""
    first, first_transport = build()
    second, second_transport = build()

    def make(acemq: AceMQ) -> FastAPI:
        app = FastAPI(lifespan=acemq.lifespan)

        @app.post("/send")
        async def send(orders: Orders) -> dict[str, int]:
            await orders.send({"n": 1})
            return {"ok": 1}

        return app

    with TestClient(make(first)) as one, TestClient(make(second)) as two:
        one.post("/send")
        one.post("/send")
        two.post("/send")

    assert len(first_transport.transport.published) == 2
    assert len(second_transport.transport.published) == 1


def test_a_publisher_before_the_lifespan_opened_says_what_is_missing() -> None:
    acemq, _ = build()
    with pytest.raises(AceMQError, match="no connection yet"):
        acemq.publisher("", "orders")


def test_publishers_do_not_survive_the_lifespan() -> None:
    """A publisher holds the connection, and a closed connection's publisher is a
    trap that shows up one deployment later."""
    acemq, _ = build()
    app = FastAPI(lifespan=acemq.lifespan)
    with TestClient(app):
        first = acemq.publisher("", "orders")
    with TestClient(app):
        second = acemq.publisher("", "orders")
    assert first is not second


def test_the_connection_is_the_librarys_own_type() -> None:
    acemq, _ = build()
    app = FastAPI(lifespan=acemq.lifespan)
    with TestClient(app):
        assert isinstance(acemq.connection, Connection)
