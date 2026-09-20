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

from enum import Enum
from typing import TYPE_CHECKING, Any

from acemq_amqp import HealthReport, HealthStatus
from fastapi import APIRouter
from fastapi.responses import JSONResponse

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .integration import AceMQ

__all__ = ["AceMQHealth", "health_router", "report_as_json"]


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

    The wording of that detail comes from the library and is the same sentence in
    every AceMQ language, so one alert rule matches a blocked broker whatever the
    service is written in. This adds ``parts`` and passes the rest through; it does
    not paraphrase.

    ``acemq.health.timeout`` is handed to the library's probe rather than wrapped
    around it. The probe has had a deadline of its own since acemq-amqp 0.7.0, and
    a second one outside it would be the cruder of the two: a blocked broker stops
    reading its socket, and the library answers that case by re-reading the blocked
    state and reporting *up* — an outer timeout firing first would turn that answer
    into a spurious *down*, and would cancel a request the library deliberately
    abandons instead, because a broker that is not reading cannot be relied on to
    process a cancellation either.
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
        timeout = acemq.settings.health.timeout.total_seconds()
        try:
            report = await connection.health(timeout)
        except Exception as failure:
            # A check must not raise; a probe wants an answer, not a 500.
            return HealthReport(
                HealthStatus.DOWN,
                f"the check itself failed: {failure}",
                parts={"blocked": connection.blocked, **self._parts()},
            )

        return HealthReport(
            report.status,
            report.detail,
            report.checked,
            {**dict(report.parts), **self._parts()},
        )

    def _parts(self) -> dict[str, Any]:
        """What this package knows that the connection does not.

        The connection counts its consumers; this names them, because a report
        saying which consumer stalled is the one worth waking up to. ``blocked``
        is not here: it comes from the connection, which is the only thing that
        can answer it.
        """
        acemq = self._acemq
        consumers = acemq.consumers
        return {
            "consumers": {name: consumer.in_flight for name, consumer in consumers.items()},
            "registered": [registration.name for registration in acemq.registrations],
        }


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
