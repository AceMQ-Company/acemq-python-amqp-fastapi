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

"""FastAPI integration for AceMQ.

A lifespan that owns the connection and the consumers, dependencies for
publishing from a route, a decorator for registering handlers, and a health check
that knows a blocked broker is not a reason to restart.

::

    from acemq_amqp import Ack, Message, accept
    from acemq_fastapi import AceMQ, publishes
    from fastapi import Depends, FastAPI

    acemq = AceMQ()

    @acemq.consumer("orders")
    async def handle(message: Message) -> Ack:
        ...
        return accept()

    app = FastAPI(lifespan=acemq.lifespan)
    app.include_router(acemq.health_router())

    @app.post("/orders")
    async def place(orders=Depends(publishes("orders"))) -> dict[str, str]:
        result = await orders.send({"id": "1"})
        return {"id": result.envelope.id}

This package re-exports nothing from :mod:`acemq_amqp`. Messages, acknowledgements,
codecs and retry policies come from the library under their own names, so the two
imports say which half of the system a line is talking about.
"""

from .dependencies import (
    AceMQConnection,
    AceMQDep,
    get_acemq,
    get_connection,
    publishes,
)
from .health import AceMQHealth, health_router, report_as_json
from .integration import AceMQ, ConsumerRegistration, DrainReport
from .lifespans import Lifespan, compose_lifespans
from .settings import (
    AceMQSettings,
    BindingSettings,
    ConsumerSettings,
    ExchangeSettings,
    HealthSettings,
    QueueSettings,
    RetrySettings,
    TlsSettings,
    TopologySettings,
)

__version__ = "0.1.0"

__all__ = [
    "AceMQ",
    "AceMQConnection",
    "AceMQDep",
    "AceMQHealth",
    "AceMQSettings",
    "BindingSettings",
    "ConsumerRegistration",
    "ConsumerSettings",
    "DrainReport",
    "ExchangeSettings",
    "HealthSettings",
    "Lifespan",
    "QueueSettings",
    "RetrySettings",
    "TlsSettings",
    "TopologySettings",
    "__version__",
    "compose_lifespans",
    "get_acemq",
    "get_connection",
    "health_router",
    "publishes",
    "report_as_json",
]
