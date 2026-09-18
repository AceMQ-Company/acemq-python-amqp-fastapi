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

"""Publishing from a route, without reaching for a global.

::

    Orders = Annotated[Publisher, Depends(publishes("orders"))]

    @app.post("/orders")
    async def place(order: Order, orders: Orders) -> dict[str, str]:
        result = await orders.send(order.model_dump())
        return {"id": result.envelope.id}

Every dependency here finds the integration through ``request.app.state.acemq``,
which the lifespan sets. That is what makes two applications in one process — a
main app and a mounted admin app, or two tests in one session — reach their own
connection rather than whichever was built last.
"""

from __future__ import annotations

from typing import Annotated, Any

from acemq_amqp import AceMQError, Codec, Connection, Publisher
from fastapi import Depends, Request

from .integration import AceMQ

__all__ = [
    "AceMQConnection",
    "AceMQDep",
    "get_acemq",
    "get_connection",
    "publishes",
]


def get_acemq(request: Request) -> AceMQ:
    """The integration for the application handling this request.

    :raises AceMQError: when the application was built without
        ``lifespan=acemq.lifespan``, which is the mistake this is really here to
        name. The alternative is an ``AttributeError`` on ``app.state`` that says
        nothing about what was forgotten
    """
    acemq: AceMQ | None = getattr(request.app.state, "acemq", None)
    if acemq is None:
        raise AceMQError(
            "acemq-fastapi: this application has no AceMQ on app.state. Build it with "
            "FastAPI(lifespan=acemq.lifespan), or compose_lifespans(..., acemq.lifespan) "
            "when it already has a lifespan of its own"
        )
    return acemq


def get_connection(acemq: Annotated[AceMQ, Depends(get_acemq)]) -> Connection:
    """The open connection, for a route that needs more than a publisher.

    Reading a queue depth, declaring something on the fly, pulling one message —
    the things :class:`~acemq_amqp.Publisher` deliberately does not do.
    """
    return acemq.connection


def publishes(
    routing_key: str = "",
    exchange: str = "",
    *,
    codec: Codec | None = None,
    persistent: bool = True,
    mandatory: bool = False,
) -> Any:
    """Builds a dependency yielding a publisher for one destination.

    The routing key comes first because publishing straight to a queue is the
    common case: with no exchange, the broker's default exchange routes to the
    queue whose name matches the key.

    The publisher is built once per destination and reused, so a route that is
    called a thousand times a second does not build a thousand publishers. It is
    a dependency rather than a module-level object because it has to be resolved
    after the lifespan has opened the connection, and because a test that builds
    a second application in the same process must not be handed the first one's.

    :param routing_key: what to publish under, or a queue name
    :param exchange: where to publish, empty for the default exchange
    :param codec: a codec other than the connection's
    :param persistent: ask the broker to write these messages to disk
    :param mandatory: fail when a message reaches no queue at all, rather than
        letting the broker drop it silently
    :returns: something to put in ``Depends(...)``
    """

    def dependency(acemq: Annotated[AceMQ, Depends(get_acemq)]) -> Publisher:
        return acemq.publisher(
            exchange,
            routing_key,
            codec=codec,
            persistent=persistent,
            mandatory=mandatory,
        )

    return dependency


#: The integration itself, for a route that wants the registry or the health check.
AceMQDep = Annotated[AceMQ, Depends(get_acemq)]

#: The open connection.
AceMQConnection = Annotated[Connection, Depends(get_connection)]
