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

"""Whether the connection works, and a route that says so.

Two things live here and they are meant to be used separately. :class:`AceMQHealth`
is a :class:`acemq_amqp.HealthCheck` — a name and an ``async check()`` — so it drops
into :func:`acemq_amqp.aggregate_health` beside an application's own checks and
contributes to one combined answer. :func:`health_router` is the other case: an
application that wants AceMQ to answer on a path of its own and nothing more.
"""

from __future__ import annotations

import asyncio
from enum import Enum
from typing import TYPE_CHECKING, Any

from acemq_amqp import HealthReport, HealthStatus
from fastapi import APIRouter
from fastapi.responses import JSONResponse

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .integration import AceMQ

__all__ = ["AceMQHealth", "health_router", "report_as_json"]

#: Said rather than guessed. RabbitMQ sends a reason with ``connection.blocked``
#: — "low on disk space", usually — and aio-pika's AMQP layer logs it and throws
#: it away rather than keeping it on the connection, so there is nothing to read
#: back. The state is available and the reason is not; saying so is better than
#: inventing one.
BLOCKED_DETAIL = (
    "the broker is applying back pressure and has blocked this connection, usually "
    "because it is low on disk or memory. Reported up on purpose: restarting into a "
    "broker that is still blocked helps nobody, and this instance is still serving"
)


class AceMQHealth:
    """The connection, as something a probe can ask.

    Up means the broker answered a round trip and every consumer is still reading.
    Degraded means it answered but a consumer has stopped reading — worth an alert,
    not worth a restart, because a replacement would almost certainly stall the same
    way. Down means the broker did not answer at all.

    **A blocked connection is up, with the reason.** That is the one place this
    disagrees with a naive reading of the broker's state, and it is deliberate: a
    blocked connection is the broker protecting itself, and an application that
    fails its own readiness check for it is an application an orchestrator restarts
    into the same blocked broker, having thrown away whatever it was holding. It is
    the same call the Spring Boot starter makes, for the same reason.

    The check is bounded by ``acemq.health.timeout``, which matters more here than
    it looks: the library's own probe is a round trip with no deadline, and a
    blocked broker is exactly the state in which a round trip does not come back.
    A probe that hangs is a pod that never comes back.
    """

    def __init__(self, acemq: AceMQ, *, name: str = "acemq") -> None:
        self._acemq = acemq
        self._name = name

    @property
    def name(self) -> str:
        """What this check is called in a combined report."""
        return self._name

    async def check(self) -> HealthReport:
        """The current state. Returns quickly and does not raise."""
        acemq = self._acemq
        if not acemq.started:
            return HealthReport(
                HealthStatus.DOWN,
                "the connection has not been opened; the lifespan is not running",
            )

        connection = acemq.connection
        blocked = blocked_state(connection)
        if blocked:
            return HealthReport(
                HealthStatus.UP,
                BLOCKED_DETAIL,
                parts=self._parts(blocked=True),
            )

        timeout = acemq.settings.health.timeout.total_seconds()
        try:
            report = await asyncio.wait_for(connection.health(), timeout)
        except asyncio.TimeoutError:
            # It may have become blocked while we were waiting, which is the
            # ordinary way a round trip stops coming back.
            if blocked_state(connection):
                return HealthReport(
                    HealthStatus.UP, BLOCKED_DETAIL, parts=self._parts(blocked=True)
                )
            return HealthReport(
                HealthStatus.DOWN,
                f"the broker did not answer within {timeout:g}s",
                parts=self._parts(blocked=False),
            )
        except Exception as failure:
            return HealthReport(
                HealthStatus.DOWN,
                f"the check itself failed: {failure}",
                parts=self._parts(blocked=blocked),
            )

        return HealthReport(
            report.status,
            report.detail,
            report.checked,
            {**dict(report.parts), **self._parts(blocked=blocked)},
        )

    def _parts(self, *, blocked: bool | None) -> dict[str, Any]:
        acemq = self._acemq
        consumers = acemq.consumers
        return {
            "blocked": blocked,
            "consumers": {name: consumer.in_flight for name, consumer in consumers.items()},
            "registered": [registration.name for registration in acemq.registrations],
        }


#: aiormq keeps the state in an event under this name and clears it on
#: ``connection.blocked``. It is name-mangled and private, and there is no
#: supported accessor anywhere above it.
_UNBLOCKED = "_Connection__connection_unblocked"

#: The attributes worth following down to it. On aio-pika 10 the path is
#: ``transport.connection.transport.connection`` — an AceMQ transport, a
#: ``RobustConnection``, an ``UnderlayConnection`` and finally aiormq's own — and
#: naming the links rather than the path means a layer appearing or disappearing
#: does not break this.
_LINKS = ("transport", "connection", "_connection")


def blocked_state(connection: Any, *, depth: int = 5) -> bool | None:
    """Whether the broker has blocked this connection, or ``None`` when unknown.

    There is no supported way to ask. RabbitMQ sends ``connection.blocked`` and
    ``connection.unblocked`` on channel zero; aiormq handles both by setting and
    clearing an event it keeps to itself, and neither it nor aio-pika exposes that
    event or the reason RabbitMQ sent with it. So this walks down the transports
    looking for the event by its mangled name, and answers ``None`` when it is not
    there — which is what a different transport, a future aio-pika, or a fake
    connection in a test will all produce.

    ``None`` is not ``False``: it means the question could not be asked, and a
    report that says ``blocked: null`` is more use in an incident than one that
    says ``false`` because it did not look. There is a test against a live broker
    whose whole job is to fail when this starts returning ``None``.

    A supported accessor belongs on the library's transport, beside the
    ``isBlocked()`` and ``blockedReason()`` the Java library already has. Until
    there is one, this is the honest version of the guess.
    """
    seen: set[int] = set()
    frontier = [connection]
    for _ in range(depth):
        nxt: list[Any] = []
        for node in frontier:
            if node is None or id(node) in seen:
                continue
            seen.add(id(node))
            unblocked = getattr(node, _UNBLOCKED, None)
            if unblocked is not None and hasattr(unblocked, "is_set"):
                return not bool(unblocked.is_set())
            nxt.extend(getattr(node, link, None) for link in _LINKS)
        frontier = nxt
    return None


def report_as_json(report: HealthReport) -> dict[str, Any]:
    """A health report as the shape the route answers with."""
    return {
        "status": report.status.value,
        "healthy": report.healthy,
        "detail": report.detail,
        "checked": report.checked.isoformat(),
        "parts": _plain(dict(report.parts)),
    }


def _plain(parts: dict[str, Any]) -> dict[str, Any]:
    """Nested reports rendered the same way as the outer one."""
    return {
        key: report_as_json(value) if isinstance(value, HealthReport) else value
        for key, value in parts.items()
    }


def health_router(
    acemq: AceMQ, *, path: str | None = None, tags: list[str | Enum] | None = None
) -> APIRouter:
    """A router with one route reporting on the connection.

    ``app.include_router(acemq.health_router())``. The path is
    ``acemq.health.path``, ``/health/acemq`` unless it is set.

    The status code is the part worth reading twice: 200 for up **and for
    degraded and blocked**, ``acemq.health.unhealthy_status_code`` (503) only for
    down. A readiness probe pointed at this route takes the instance out of
    rotation when the broker is unreachable and leaves it in when the broker is
    merely unhappy, which is the behaviour described in
    :class:`AceMQHealth`. An alert should read the body, not the code.
    """
    router = APIRouter(tags=tags if tags is not None else ["acemq"])

    @router.get(
        path or acemq.settings.health.path,
        summary="AceMQ broker connection",
        response_class=JSONResponse,
    )
    async def acemq_health() -> JSONResponse:
        report = await acemq.health()
        code = 200 if report.healthy else acemq.settings.health.unhealthy_status_code
        return JSONResponse(report_as_json(report), status_code=code)

    return router
