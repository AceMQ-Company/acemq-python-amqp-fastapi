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

"""Shared fixtures.

The environment is cleared for every test. ``AceMQSettings`` reads ``ACEMQ_*``,
and a developer with ``ACEMQ_URL`` exported for something else should not find
this suite connecting to it.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator

import pytest

#: Where the integration tests expect a broker. A port of its own, so the suite
#: cannot reach a broker somebody is using for something else.
BROKER_URL = os.environ.get("ACEMQ_TEST_BROKER", "amqp://guest:guest@localhost:5722/")


@pytest.fixture(autouse=True)
def clean_environment(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    for name in list(os.environ):
        if name.startswith("ACEMQ_"):
            monkeypatch.delenv(name, raising=False)
    yield


@pytest.fixture
def queue_name() -> str:
    """A queue no other test is using."""
    return f"acemq-fastapi-{uuid.uuid4().hex[:12]}"
