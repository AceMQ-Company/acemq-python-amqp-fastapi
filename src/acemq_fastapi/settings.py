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

"""The ``acemq.*`` settings, and what they turn into.

Every setting here has a counterpart in the Spring Boot starter's
``acemq.*`` properties, spelled the way Python spells things. They read from the
environment with the prefix ``ACEMQ_`` and ``__`` between levels — so
``ACEMQ_URL``, ``ACEMQ_CONSUMER__PREFETCH``, ``ACEMQ_TLS__VERIFICATION`` — and
from a ``.env`` file when one is beside the process.

The models are pydantic, and that is a deliberate dependency rather than a
reluctant one. FastAPI already brings pydantic, so what this adds is
``pydantic-settings``: a small package whose whole job is reading environment
variables into a model. Writing a parser here for durations, nested keys and
``.env`` files would be a worse version of it that this repository would then own.

Nothing in this module talks to a broker. It turns strings into the library's own
types — :class:`acemq_amqp.Security`, :class:`acemq_amqp.RetryPolicy`,
:class:`acemq_amqp.Topology` — and hands them over. That separation is what makes
the settings testable without a broker, and it is why every ``build_*`` method
below is synchronous.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Annotated, Any, Literal

from acemq_amqp import (
    Codec,
    Credentials,
    RetryPolicy,
    Security,
    Topology,
    Verification,
    codec_by_name,
    exponential_retry,
    fixed_retry,
    no_retry,
)
from pydantic import BaseModel, BeforeValidator, Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

__all__ = [
    "AceMQSettings",
    "BindingSettings",
    "ConsumerSettings",
    "Duration",
    "ExchangeSettings",
    "HealthSettings",
    "QueueSettings",
    "RetrySettings",
    "TlsSettings",
    "TopologySettings",
]


def _seconds(value: Any) -> Any:
    """``30`` and ``"30"`` mean thirty seconds.

    Pydantic reads ``PT30S`` and ``0:00:30`` and refuses a bare number in a string,
    which is the one spelling every environment variable actually uses. An
    environment that says ``ACEMQ_CONFIRM_TIMEOUT=30`` means thirty seconds and
    should not have to learn ISO 8601 to say it. Everything pydantic already
    understands is passed through untouched.
    """
    if isinstance(value, str):
        try:
            return timedelta(seconds=float(value))
        except ValueError:
            return value
    if isinstance(value, (int, float)):
        return timedelta(seconds=float(value))
    return value


#: A duration that also reads a bare number of seconds. See :func:`_seconds`.
Duration = Annotated[timedelta, BeforeValidator(_seconds)]


class TlsSettings(BaseModel):
    """How to verify the broker and what to present to it.

    Maps onto :class:`acemq_amqp.Security`. It is only consulted for an
    ``amqps://`` URL — the library refuses TLS settings against a plaintext URL
    rather than ignoring them, and this module passes that refusal straight
    through.
    """

    #: A PEM file holding the authority to verify the broker against. Naming one
    #: replaces the machine's trust store rather than adding to it.
    certificate_authority: Path | None = None

    #: A PEM certificate to present, for a broker that authenticates by certificate.
    client_certificate: Path | None = None

    #: The PEM private key for that certificate. Defaults to the certificate file.
    client_key: Path | None = None

    #: The passphrase on that key, when it has one.
    client_key_password: SecretStr | None = None

    #: The name to check the certificate against, when it is not the host in the URL.
    server_name: str | None = None

    #: ``certificate`` checks the chain and the name. ``nothing-at-all`` checks
    #: nothing, and is spelled that way so a configuration file cannot hide what
    #: it is giving up behind a ``verify: false``.
    verification: Literal["certificate", "nothing-at-all"] = "certificate"

    #: Whether to accept a broker certificate carrying the development marker.
    #: Off, so a laptop's settings copied into a deployment fails loudly.
    allow_development_certificates: bool = False

    @property
    def describes_tls(self) -> bool:
        """Whether anything here is worth passing to the library."""
        return any(
            (
                self.certificate_authority,
                self.client_certificate,
                self.client_key,
                self.server_name,
                self.verification != "certificate",
                self.allow_development_certificates,
            )
        )


class RetrySettings(BaseModel):
    """The ladder a consumer follows when a handler asks to be tried again.

    ``enabled`` is off, matching the Spring starter: a retry ladder declares rung
    queues on the broker, and a service should say it wants them rather than
    discover them.
    """

    #: Whether to build a policy at all. Off means :func:`acemq_amqp.no_retry`.
    enabled: bool = False

    #: ``exponential`` doubles with 20% jitter; ``fixed`` waits the same every time.
    kind: Literal["exponential", "fixed"] = "exponential"

    #: Total deliveries, the first one included.
    max_attempts: int = Field(default=3, ge=1)

    #: The wait before the second attempt.
    initial_delay: Duration = timedelta(seconds=1)

    #: The ceiling however many attempts have passed. Zero for none.
    max_delay: Duration = timedelta(minutes=1)

    #: Give up on a message older than this, whatever attempt it is on.
    give_up_after: Duration | None = None

    #: Waits at or above this live on a rung queue in the broker rather than in
    #: this process. It is a shutdown setting as much as a reliability one: a
    #: wait held here is a wait a drain has to sit through.
    wait_in_broker_from: Duration | None = None

    def build(self) -> RetryPolicy:
        """The library policy these settings describe."""
        if not self.enabled:
            return no_retry()
        if self.kind == "fixed":
            policy = fixed_retry(self.max_attempts, self.initial_delay)
        else:
            policy = exponential_retry(self.max_attempts, self.initial_delay, self.max_delay)
        if self.give_up_after is not None:
            policy = policy.give_up_after(self.give_up_after)
        if self.wait_in_broker_from is not None:
            policy = policy.wait_in_broker_from(self.wait_in_broker_from)
        return policy


class ConsumerSettings(BaseModel):
    """The defaults every registered consumer starts with.

    A consumer registered with :meth:`~acemq_fastapi.AceMQ.consumer` overrides
    any of these per queue; what is set here is what the ones that say nothing get.
    """

    #: How many unacknowledged messages a consumer holds. It is also the bound on
    #: how much work a drain has to give back — see the shutdown section of the
    #: README.
    prefetch: int = Field(default=100, ge=1)

    #: How many messages one consumer works on at once. One keeps a queue's
    #: messages in order.
    concurrency: int = Field(default=1, ge=1)

    #: Declare the queues a failure needs — ``{queue}.dlq``, ``{queue}.parked``,
    #: the rungs — before subscribing. Turn it off for a login with no
    #: ``configure`` permission on the vhost.
    declare: bool = True

    #: Start the consumers when the lifespan opens. Off leaves them registered and
    #: stopped, for a process that serves HTTP and should not also consume.
    auto_start: bool = True

    #: How long the lifespan waits for consumers to drain before giving up on
    #: them and closing the connection anyway.
    shutdown_timeout: Duration = timedelta(seconds=20)

    #: The ladder. See :class:`RetrySettings`.
    retry: RetrySettings = Field(default_factory=RetrySettings)


class QueueSettings(BaseModel):
    """One queue in ``acemq.topology.queues``."""

    name: str

    #: Survives a broker restart.
    durable: bool = True

    #: A durable queue is a quorum queue unless it is asked to be something else,
    #: because the queue type is an argument the broker compares and two
    #: languages that disagree about it cannot both consume the queue.
    quorum: bool | None = None

    auto_delete: bool = False
    exclusive: bool = False

    #: Declare ``{name}.dlq`` and ``{name}.parked`` alongside it and point the
    #: broker at them.
    dead_letter: bool = False

    #: Broker-specific arguments.
    args: dict[str, Any] = Field(default_factory=dict)


class ExchangeSettings(BaseModel):
    """One exchange in ``acemq.topology.exchanges``."""

    name: str
    kind: Literal["direct", "topic", "fanout", "headers"] = "topic"
    durable: bool = True
    auto_delete: bool = False
    args: dict[str, Any] = Field(default_factory=dict)


class BindingSettings(BaseModel):
    """One binding in ``acemq.topology.bindings``."""

    queue: str
    exchange: str
    routing_key: str = ""


class TopologySettings(BaseModel):
    """What to declare when the lifespan opens.

    Declaring nothing is the default and is a real answer: a service that only
    consumes queues somebody else owns should not create them.
    """

    queues: list[QueueSettings] = Field(default_factory=list)
    exchanges: list[ExchangeSettings] = Field(default_factory=list)
    bindings: list[BindingSettings] = Field(default_factory=list)

    #: Whether the rung queues of the consumer retry policy are declared with each
    #: queue. On, because a policy whose rungs are missing falls back to waiting in
    #: this process, which is the slow drain nobody expects.
    declare_retry_rungs: bool = True

    @property
    def empty(self) -> bool:
        """Whether there is anything at all to apply."""
        return not (self.queues or self.exchanges or self.bindings)

    def build(self, retry: RetryPolicy | None = None) -> Topology:
        """The library topology these settings describe."""
        topology = Topology()
        for exchange in self.exchanges:
            topology.exchange(
                exchange.name,
                exchange.kind,
                durable=exchange.durable,
                auto_delete=exchange.auto_delete,
                args=exchange.args,
            )
        for queue in self.queues:
            topology.queue(
                queue.name,
                durable=queue.durable,
                auto_delete=queue.auto_delete,
                exclusive=queue.exclusive,
                quorum=queue.quorum,
                dead_letter=queue.dead_letter,
                retry=retry if self.declare_retry_rungs else None,
                args=queue.args,
            )
        for binding in self.bindings:
            topology.binding(binding.queue, binding.exchange, binding.routing_key)
        return topology


class HealthSettings(BaseModel):
    """The health route, when one is mounted."""

    #: Path of the route :meth:`~acemq_fastapi.AceMQ.health_router` builds.
    path: str = "/health/acemq"

    #: How long the broker gets to answer before the check gives up and reports
    #: down. The library's own probe has no deadline, and a probe that hangs is a
    #: pod that never comes back, so this one is not optional.
    timeout: Duration = timedelta(seconds=5)

    #: The status code a report that is not healthy answers with. 503 is what a
    #: readiness probe expects; a degraded or blocked report still answers 200,
    #: because degraded is not a reason to be taken out of rotation.
    unhealthy_status_code: int = 503


class AceMQSettings(BaseSettings):
    """Everything ``acemq.*`` can say.

    Built from the environment by default::

        settings = AceMQSettings()                     # ACEMQ_URL, ACEMQ_TLS__..., ...
        settings = AceMQSettings(url="amqp://host/")   # or written down

    A FastAPI application that already keeps its own ``BaseSettings`` can nest
    this one inside it and pass ``settings.acemq`` to :class:`~acemq_fastapi.AceMQ`.
    """

    model_config = SettingsConfigDict(
        env_prefix="ACEMQ_",
        env_nested_delimiter="__",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    #: Where the broker is. ``amqps://`` is encrypted, ``amqp://`` is not.
    url: str = "amqp://localhost:5672/"

    #: The login, when it is kept out of the URL — which is how a password stays
    #: out of a connection string that ends up in a log.
    username: str | None = None
    password: SecretStr | None = None

    #: The vhost, when it is not in the URL. Given here it replaces the URL's.
    virtual_host: str | None = None

    #: What to stamp on published messages. Defaults to the library's
    #: ``acemq@{hostname}``, which names the machine but does not guess the service.
    client_name: str | None = None

    #: What publishers and consumers encode and decode with unless they say
    #: otherwise. One of :func:`acemq_amqp.codec_names`.
    format: str = "json"

    #: How many unacknowledged messages a consumer holds, unless
    #: ``consumer.prefetch`` or the registration says otherwise.
    prefetch: int = Field(default=100, ge=1)

    #: Whether a publish waits for the broker's confirmation. The library always
    #: publishes with confirms, so turning this off is refused rather than
    #: silently ignored — see the validator below.
    publisher_confirms: bool = True

    #: How many publishes may be waiting for the broker at once. The eleventh
    #: thousand-and-first publish waits for room rather than queueing in memory.
    max_outstanding_publishes: int = Field(default=1000, ge=1)

    #: How long a publish waits for that room before raising.
    confirm_timeout: Duration = timedelta(seconds=30)

    #: How long :func:`acemq_amqp.connect` is given before the lifespan gives up.
    connection_timeout: Duration = timedelta(seconds=10)

    tls: TlsSettings = Field(default_factory=TlsSettings)
    topology: TopologySettings = Field(default_factory=TopologySettings)
    consumer: ConsumerSettings = Field(default_factory=ConsumerSettings)
    health: HealthSettings = Field(default_factory=HealthSettings)

    @field_validator("publisher_confirms")
    @classmethod
    def _confirms_are_not_optional(cls, value: bool) -> bool:
        if not value:
            raise ValueError(
                "acemq-fastapi: acemq.publisher_confirms cannot be turned off. The Python "
                "library publishes with confirms always, and a setting that silently did "
                "nothing would be worse than one that is not there: a service would think "
                "it had traded safety for throughput and have neither. Take the setting off."
            )
        return value

    @property
    def broker_url(self) -> str:
        """The URL to dial, with ``virtual_host`` applied.

        Credentials are *not* applied here: they travel in
        :meth:`build_security`, which is where the library puts them, so a URL
        that ends up in a log or an exception carries no password.
        """
        if self.virtual_host is None:
            return self.url
        base, _, _ = self.url.partition("?")
        query = self.url[len(base) :]
        host = base.rstrip("/")
        # Split off the path the URL already carried: everything after the
        # authority is the vhost, and a setting that names one replaces it.
        scheme, _, rest = host.partition("://")
        authority, _, _ = rest.partition("/")
        vhost = self.virtual_host.lstrip("/")
        return f"{scheme}://{authority}/{vhost}{query}"

    def build_codec(self) -> Codec:
        """The codec ``format`` names.

        :raises LookupError: when no codec is registered under that name, listing
            the ones that are
        """
        return codec_by_name(self.format)

    def build_security(self) -> Security | None:
        """The library's :class:`~acemq_amqp.Security`, or ``None`` when there is
        nothing to say beyond the URL."""
        credentials = None
        if self.username is not None or self.password is not None:
            credentials = Credentials(
                username=self.username or "",
                secret=self.password.get_secret_value() if self.password else "",
            )
        if credentials is None and not self.tls.describes_tls:
            return None
        password = self.tls.client_key_password
        return Security(
            certificate_authority=self.tls.certificate_authority,
            client_certificate=self.tls.client_certificate,
            client_key=self.tls.client_key,
            client_key_password=password.get_secret_value() if password else None,
            server_name=self.tls.server_name,
            credentials=credentials,
            verification=Verification(self.tls.verification),
            allow_development_certificates=self.tls.allow_development_certificates,
        )

    def build_retry(self) -> RetryPolicy:
        """The consumer default retry policy."""
        return self.consumer.retry.build()

    def build_topology(self) -> Topology | None:
        """The topology to apply at startup, or ``None`` when none was declared."""
        if self.topology.empty:
            return None
        return self.topology.build(self.build_retry())
