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

"""Keeping an application's own lifespan as well as this one.

FastAPI takes one lifespan, and an application that already has one should not
have to give it up or paste this package's start-up into it. :func:`compose_lifespans`
nests them::

    app = FastAPI(lifespan=compose_lifespans(database_pool, acemq.lifespan))

Left to right is outside in: ``database_pool`` opens first and closes last,
``acemq.lifespan`` opens last and closes first.

**Put AceMQ last.** Consumers should start after the things their handlers use
and stop before those go away, and a handler that reaches for a connection pool
which has already been closed is the bug this ordering exists to prevent. The
same argument puts it *before* anything it depends on itself.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from contextlib import AsyncExitStack, asynccontextmanager
from typing import Any, Protocol, runtime_checkable

__all__ = ["Lifespan", "compose_lifespans"]


@runtime_checkable
class Lifespan(Protocol):
    """What Starlette accepts: something that takes the app and is an async
    context manager. It may yield nothing, or a mapping merged into the request
    state."""

    def __call__(self, app: Any) -> Any: ...  # pragma: no cover - a shape, not code


def compose_lifespans(*lifespans: Lifespan) -> Lifespan:
    """Runs several lifespans as one, outermost first.

    The state each one yields is merged, in order, so a later lifespan can
    override an earlier one's key and a route sees all of them. A lifespan that
    yields nothing contributes nothing, which is the common case.

    An exception on the way up unwinds the ones already open, because
    :class:`~contextlib.AsyncExitStack` is doing the work and that is what it is
    for — half an application started is worse than none.

    :param lifespans: the lifespans, outside in
    :returns: one lifespan to pass to ``FastAPI(lifespan=...)``
    """

    @asynccontextmanager
    async def composed(app: Any) -> AsyncIterator[Mapping[str, Any]]:
        state: dict[str, Any] = {}
        async with AsyncExitStack() as stack:
            for lifespan in lifespans:
                yielded = await stack.enter_async_context(lifespan(app))
                if isinstance(yielded, Mapping):
                    state.update(yielded)
            yield state

    return composed
