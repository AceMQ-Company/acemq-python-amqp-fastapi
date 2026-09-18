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

"""The object an application holds, and the lifespan that owns the connection.

One :class:`AceMQ` per application. It is built at import time, collects consumer
registrations from decorators, and does nothing at all until its lifespan opens —
which is the point: a module that connects to a broker when it is imported is a
module that cannot be imported by a test, a migration script or ``--help``.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, TypeVar

import acemq_amqp
from acemq_amqp import (
    AceMQError,
    Ack,  # noqa: F401 -- see below
    Codec,
    Connection,
    ConsumeInterceptor,
    Consumer,
    Handler,
    Observer,
    Publisher,
    PublishInterceptor,
    RetryPolicy,
)

from .settings import AceMQSettings

if TYPE_CHECKING:  # pragma: no cover - typing only
    from fastapi import FastAPI

    from .health import AceMQHealth

__all__ = ["AceMQ", "ConsumerRegistration", "DrainReport"]

log = logging.getLogger("acemq_fastapi")

# ``Ack`` is imported but not used here, and that is on purpose. The library's
# ``Handler`` alias is ``Callable[[Message], "Ack | Awaitable[Ack]"]`` — a forward
# reference — and anything resolving the annotations of a function in this module
# evaluates that string against *this* module's globals. Without the name here,
# pdoc renders the registry's signatures as unresolved and a reader of the API
# reference sees a handler type that says nothing.

#: What builds the connection. Replaceable so a test can hand over a connection
#: on a fake transport, and so an application that needs a connection this
#: package does not model — an unusual ``transport_options``, a codec built at
#: runtime — can build its own and keep everything else.
ConnectionFactory = Callable[[AceMQSettings], Awaitable[Connection]]

#: Bound to :data:`acemq_amqp.Handler` so the decorator gives back the function it
#: was handed, with its own signature: a handler stays as callable, and as typed,
#: after decoration as it was before.
HandlerT = TypeVar("HandlerT", bound=Handler)


@dataclass(frozen=True, slots=True)
class ConsumerRegistration:
    """One ``@acemq.consumer`` declaration, before anything is running.

    ``None`` on the per-consumer settings means "whatever ``acemq.consumer.*``
    says", resolved when the lifespan opens rather than when the decorator runs,
    so settings read from the environment still reach a consumer registered at
    import time.
    """

    #: What this consumer is called to :meth:`AceMQ.consumers`. Defaults to the
    #: queue name, and has to be unique because that is how it is addressed.
    name: str
    queue: str
    handler: Handler
    codec: Codec | None = None
    retry: RetryPolicy | None = None
    prefetch: int | None = None
    concurrency: int | None = None
    declare: bool | None = None
    #: What to call this consumer to the broker. Empty lets the broker name it.
    tag: str = ""
    args: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class DrainReport:
    """What a shutdown got through.

    Returned by :meth:`AceMQ.drain` and logged by the lifespan. ``finished``
    false means the deadline expired: the handlers still running were cancelled
    and their messages left unsettled, so the broker will hand them to whoever
    replaces this process. Nothing is lost; some work may be done twice.
    """

    finished: bool
    #: Seconds spent draining.
    waited: float
    #: How many consumers were asked to stop.
    consumers: int
    #: How many messages were still in a handler when the deadline expired.
    abandoned: int = 0


class AceMQ:
    """The connection, the consumers and the settings, for one application.

    ::

        acemq = AceMQ()

        @acemq.consumer("orders")
        async def handle(message: Message) -> Ack:
            ...
            return accept()

        app = FastAPI(lifespan=acemq.lifespan)

    :param settings: what to connect to and how. Read from the environment when
        not given
    :param connection_factory: what opens the connection. Replaceable for tests,
        and for an application that builds its own
    :param on_publish: interceptors wrapping every publish, outermost first
    :param on_consume: interceptors wrapping every handler
    :param observer: where the library's numbers go
    """

    def __init__(
        self,
        settings: AceMQSettings | None = None,
        *,
        connection_factory: ConnectionFactory | None = None,
        on_publish: tuple[PublishInterceptor, ...] = (),
        on_consume: tuple[ConsumeInterceptor, ...] = (),
        observer: Observer | None = None,
    ) -> None:
        self._settings = settings if settings is not None else AceMQSettings()
        self._connect: ConnectionFactory = connection_factory or self._open
        self._on_publish = tuple(on_publish)
        self._on_consume = tuple(on_consume)
        self._observer = observer
        self._registrations: dict[str, ConsumerRegistration] = {}
        self._consumers: dict[str, Consumer] = {}
        self._publishers: dict[tuple[Any, ...], Publisher] = {}
        self._connection: Connection | None = None
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------ state

    @property
    def settings(self) -> AceMQSettings:
        """What this integration was configured with."""
        return self._settings

    @property
    def started(self) -> bool:
        """Whether the connection is open."""
        return self._connection is not None

    @property
    def connection(self) -> Connection:
        """The open connection.

        :raises AceMQError: before the lifespan has opened it, because the
            alternative is a ``None`` that turns into an ``AttributeError``
            three frames away from the mistake
        """
        if self._connection is None:
            raise AceMQError(
                "acemq-fastapi: there is no connection yet. It is opened by the lifespan, "
                "so this is either a call made before the application started or an "
                "application built without lifespan=acemq.lifespan"
            )
        return self._connection

    @property
    def registrations(self) -> tuple[ConsumerRegistration, ...]:
        """Every ``@acemq.consumer`` declaration, in the order they were made."""
        return tuple(self._registrations.values())

    @property
    def consumers(self) -> Mapping[str, Consumer]:
        """The running consumers by name, for scaling, pausing and counters."""
        return dict(self._consumers)

    # -------------------------------------------------------------- consumers

    def consumer(
        self,
        queue: str,
        *,
        name: str | None = None,
        codec: Codec | None = None,
        retry: RetryPolicy | None = None,
        prefetch: int | None = None,
        concurrency: int | None = None,
        declare: bool | None = None,
        tag: str = "",
        args: Mapping[str, Any] | None = None,
    ) -> Callable[[HandlerT], HandlerT]:
        """Registers a handler for a queue. The Python analogue of ``@AceListener``.

        The decorated function is returned unchanged, so it stays an ordinary
        function that a test can call with a :class:`~acemq_amqp.Message` and
        assert on, without a broker anywhere near it.

        Nothing subscribes here. The registration is held until the lifespan
        opens, which is what puts the handler on the application's own event loop
        rather than on whichever loop happened to import the module.

        :param queue: what to read
        :param name: what to call this consumer in :attr:`consumers`. Defaults to
            the queue
        :param codec: a codec other than the connection's
        :param retry: a policy other than ``acemq.consumer.retry``
        :param prefetch: how many unacknowledged messages to hold
        :param concurrency: how many messages to work on at once
        :param declare: declare the queues a failure needs before subscribing
        :param tag: what to call this consumer to the broker
        :param args: broker-specific consumer arguments
        :returns: the decorator
        :raises AceMQError: when the name is already taken, or the lifespan has
            already opened
        """

        def register(handler: HandlerT) -> HandlerT:
            self.add_consumer(
                queue,
                handler,
                name=name,
                codec=codec,
                retry=retry,
                prefetch=prefetch,
                concurrency=concurrency,
                declare=declare,
                tag=tag,
                args=args,
            )
            return handler

        return register

    def add_consumer(
        self,
        queue: str,
        handler: Handler,
        *,
        name: str | None = None,
        codec: Codec | None = None,
        retry: RetryPolicy | None = None,
        prefetch: int | None = None,
        concurrency: int | None = None,
        declare: bool | None = None,
        tag: str = "",
        args: Mapping[str, Any] | None = None,
    ) -> ConsumerRegistration:
        """Registers a handler without the decorator.

        What a factory function or a plugin uses. Same arguments as
        :meth:`consumer`, and the same restriction: before the lifespan opens.
        """
        if self._connection is not None:
            raise AceMQError(
                f"acemq-fastapi: cannot register a consumer for {queue!r} now — the "
                "lifespan has already opened. Register consumers while the module is "
                "being imported, which is what the decorator is for"
            )
        chosen = name or queue
        if chosen in self._registrations:
            raise AceMQError(
                f"acemq-fastapi: there is already a consumer called {chosen!r}. Two "
                "consumers on one queue is a reasonable thing to want; give one of them "
                "a name=... so they can be told apart"
            )
        registration = ConsumerRegistration(
            name=chosen,
            queue=queue,
            handler=handler,
            codec=codec,
            retry=retry,
            prefetch=prefetch,
            concurrency=concurrency,
            declare=declare,
            tag=tag,
            args=dict(args or {}),
        )
        self._registrations[chosen] = registration
        return registration

    async def start_consumer(self, name: str) -> Consumer:
        """Subscribes one registered consumer that is not running.

        What :attr:`consumers` is for in the other direction: a consumer stopped
        for back pressure, or one left registered by
        ``acemq.consumer.auto_start=false``, is started again by name.
        """
        registration = self._registrations.get(name)
        if registration is None:
            known = ", ".join(sorted(self._registrations)) or "none"
            raise AceMQError(
                f"acemq-fastapi: no consumer is registered as {name!r}; registered: {known}"
            )
        if name in self._consumers:
            return self._consumers[name]
        consumer = await self._subscribe(registration)
        self._consumers[name] = consumer
        return consumer

    async def stop_consumer(self, name: str) -> None:
        """Drains and closes one consumer, leaving it registered.

        It finishes what its handlers are holding, exactly as a shutdown does.
        """
        consumer = self._consumers.pop(name, None)
        if consumer is not None:
            await consumer.close()

    # ------------------------------------------------------------- publishing

    def publisher(
        self,
        exchange: str = "",
        routing_key: str = "",
        *,
        codec: Codec | None = None,
        persistent: bool = True,
        mandatory: bool = False,
    ) -> Publisher:
        """A publisher for a destination, built once and reused.

        Publishers are cheap to keep and safe to share, and building one per
        request is a small waste repeated a lot. This caches by every argument
        that changes what a publisher does, so two routes asking for the same
        destination get the same object and a route asking for a different one
        does not quietly get somebody else's.

        :raises AceMQError: before the lifespan has opened the connection
        """
        key = (exchange, routing_key, id(codec) if codec else None, persistent, mandatory)
        publisher = self._publishers.get(key)
        if publisher is None:
            publisher = self.connection.publisher(
                exchange,
                routing_key,
                codec=codec,
                persistent=persistent,
                mandatory=mandatory,
            )
            self._publishers[key] = publisher
        return publisher

    # ----------------------------------------------------------------- health

    def health_check(self) -> AceMQHealth:
        """This connection as a :class:`~acemq_amqp.HealthCheck`.

        Pass it to :func:`acemq_amqp.aggregate_health` beside an application's
        own checks, or mount :meth:`health_router` and let it answer on its own.
        """
        from .health import AceMQHealth

        return AceMQHealth(self)

    async def health(self) -> acemq_amqp.HealthReport:
        """What a readiness probe should see. See :class:`~acemq_fastapi.AceMQHealth`."""
        return await self.health_check().check()

    def health_router(self, *, path: str | None = None, tags: list[Any] | None = None) -> Any:
        """An ``APIRouter`` with one route reporting on the connection.

        ``app.include_router(acemq.health_router())`` and nothing else. Mounting
        it is explicit rather than automatic because a route that appears in an
        application's OpenAPI document without anybody adding it is a route
        somebody has to go looking for.
        """
        from .health import health_router

        return health_router(self, path=path, tags=tags)

    # --------------------------------------------------------------- lifespan

    @asynccontextmanager
    async def lifespan(self, app: FastAPI | None = None) -> AsyncIterator[Mapping[str, Any]]:
        """Opens the connection and the consumers, and drains them on the way out.

        ``FastAPI(lifespan=acemq.lifespan)``. To keep an application's own
        lifespan as well, see :func:`acemq_fastapi.compose_lifespans` — this one
        belongs innermost, so the consumers start after whatever they depend on
        and drain before it goes away.

        The ``app`` is optional so the same context manager can be used in a test
        or a worker with no ASGI application at all. When one is given it gets
        ``app.state.acemq``, which is how the request dependencies find this
        object without a module-level global.
        """
        started = await self.start(app)
        try:
            yield {"acemq": self}
        finally:
            report = await self.drain()
            if not report.finished:
                log.warning(
                    "acemq-fastapi: the drain did not finish in %s; %d message(s) were "
                    "left unsettled and the broker will redeliver them",
                    self._settings.consumer.shutdown_timeout,
                    report.abandoned,
                )
            await self.aclose()
            log.info(
                "acemq-fastapi: closed after %.3fs, %d consumer(s) drained",
                report.waited,
                started,
            )

    async def start(self, app: FastAPI | None = None) -> int:
        """Connects, applies the topology and subscribes the consumers.

        Called by :meth:`lifespan`. Public because a worker process that serves no
        HTTP at all still wants the same start-up, and should not have to
        construct an ASGI application to get it.

        :returns: how many consumers were started
        """
        async with self._lock:
            if self._connection is not None:
                raise AceMQError("acemq-fastapi: this integration is already started")
            timeout = self._settings.connection_timeout.total_seconds()
            try:
                connection = await asyncio.wait_for(self._connect(self._settings), timeout)
            except asyncio.TimeoutError as expired:
                raise AceMQError(
                    f"acemq-fastapi: the broker at {self._settings.broker_url} did not "
                    f"answer within {timeout:g}s"
                ) from expired
            self._connection = connection

        try:
            for publishing in self._on_publish:
                connection.intercept_publish(publishing)
            for consuming in self._on_consume:
                connection.intercept_consume(consuming)

            topology = self._settings.build_topology()
            if topology is not None:
                await connection.declare(topology)

            if self._settings.consumer.auto_start:
                for registration in self._registrations.values():
                    self._consumers[registration.name] = await self._subscribe(registration)
        except BaseException:
            # A half-started application is worse than one that refused to start:
            # the routes would be up, the consumers would not, and nothing outside
            # could tell. Undo what was done and let the failure out.
            await self.drain()
            await self.aclose()
            raise

        if app is not None:
            app.state.acemq = self
        log.info(
            "acemq-fastapi: connected to %s, %d consumer(s) running",
            self._settings.broker_url,
            len(self._consumers),
        )
        return len(self._consumers)

    async def drain(self) -> DrainReport:
        """Stops every consumer, finishing the handlers already running.

        What this finishes, and what it gives back, is written down in the README
        under *What a drain finishes*. In short: a handler in flight runs to
        completion; a delivery fetched but not yet started is handed back to the
        broker; a retry waiting out a short backoff in this process is waited out
        in full; a publish waiting for its confirm is not waited for.

        Bounded by ``acemq.consumer.shutdown_timeout``. When that expires the
        handlers still running are cancelled and their messages left unsettled —
        the broker redelivers them, so the work may be done twice and nothing is
        dropped.
        """
        if self._connection is None or not self._consumers:
            self._consumers.clear()
            return DrainReport(finished=True, waited=0.0, consumers=0)

        consumers = tuple(self._consumers.values())
        self._consumers.clear()
        deadline = self._settings.consumer.shutdown_timeout.total_seconds()
        loop = asyncio.get_running_loop()
        started = loop.time()

        closing = [asyncio.create_task(consumer.close()) for consumer in consumers]
        done, pending = await asyncio.wait(closing, timeout=deadline)
        waited = loop.time() - started

        if not pending:
            for task in done:
                if task.cancelled():
                    # Its workers were already gone — cancelled from outside, or
                    # killed by something the library logged. Nothing here can
                    # finish a handler that is not running any more.
                    log.warning(
                        "acemq-fastapi: a consumer's workers had already stopped; "
                        "whatever they were holding is back with the broker"
                    )
                elif task.exception() is not None:
                    log.warning(
                        "acemq-fastapi: a consumer failed to close: %s", task.exception()
                    )
            return DrainReport(finished=True, waited=waited, consumers=len(consumers))

        # Out of time. Cancelling is the only lever there is, and it is pulled
        # last rather than first: it aborts the handlers still running and leaves
        # their deliveries unsettled for the broker to hand on. That is the price
        # of a deadline, and it is cheaper than being SIGKILLed with the same
        # messages unsettled and the socket gone.
        abandoned = sum(consumer.in_flight for consumer in consumers)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        return DrainReport(
            finished=False,
            waited=loop.time() - started,
            consumers=len(consumers),
            abandoned=abandoned,
        )

    async def aclose(self) -> None:
        """Releases the connection. Idempotent, and safe after a failed start."""
        connection = self._connection
        self._connection = None
        self._publishers.clear()
        self._consumers.clear()
        if connection is not None:
            with suppress(Exception):
                await connection.close()

    # ---------------------------------------------------------------- private

    async def _subscribe(self, registration: ConsumerRegistration) -> Consumer:
        defaults = self._settings.consumer
        return await self.connection.consume(
            registration.queue,
            registration.handler,
            codec=registration.codec,
            retry=registration.retry or self._settings.build_retry(),
            prefetch=registration.prefetch
            if registration.prefetch is not None
            else defaults.prefetch,
            concurrency=registration.concurrency
            if registration.concurrency is not None
            else defaults.concurrency,
            tag=registration.tag,
            args=registration.args,
            declare=(
                registration.declare if registration.declare is not None else defaults.declare
            ),
        )

    async def _open(self, settings: AceMQSettings) -> Connection:
        """The default factory: :func:`acemq_amqp.connect` with the settings applied."""
        return await acemq_amqp.connect(
            settings.broker_url,
            codec=settings.build_codec(),
            origin=settings.client_name,
            retry=settings.build_retry(),
            prefetch=settings.prefetch,
            max_outstanding_publishes=settings.max_outstanding_publishes,
            confirm_timeout=settings.confirm_timeout,
            security=settings.build_security(),
            observer=self._observer,
        )
