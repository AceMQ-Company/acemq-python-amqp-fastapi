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

"""What ``acemq.*`` binds to, and what it turns into."""

from __future__ import annotations

from datetime import timedelta

import pytest
from acemq_amqp import JsonCodec, Verification
from pydantic import SecretStr, ValidationError

from acemq_fastapi import AceMQSettings


def test_defaults_need_no_environment() -> None:
    settings = AceMQSettings()
    assert settings.url == "amqp://localhost:5672/"
    assert settings.format == "json"
    assert settings.prefetch == 100
    assert settings.consumer.concurrency == 1
    assert settings.consumer.shutdown_timeout == timedelta(seconds=20)
    assert settings.topology.empty


def test_environment_binds_nested_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ACEMQ_URL", "amqp://broker:5672/orders")
    monkeypatch.setenv("ACEMQ_CONSUMER__PREFETCH", "7")
    monkeypatch.setenv("ACEMQ_CONSUMER__RETRY__ENABLED", "true")
    monkeypatch.setenv("ACEMQ_CONSUMER__RETRY__MAX_ATTEMPTS", "4")
    monkeypatch.setenv("ACEMQ_HEALTH__PATH", "/livez/mq")
    settings = AceMQSettings()
    assert settings.url == "amqp://broker:5672/orders"
    assert settings.consumer.prefetch == 7
    assert settings.consumer.retry.max_attempts == 4
    assert settings.health.path == "/livez/mq"


def test_durations_read_as_seconds_and_iso(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ACEMQ_CONFIRM_TIMEOUT", "12")
    monkeypatch.setenv("ACEMQ_CONSUMER__SHUTDOWN_TIMEOUT", "PT45S")
    settings = AceMQSettings()
    assert settings.confirm_timeout == timedelta(seconds=12)
    assert settings.consumer.shutdown_timeout == timedelta(seconds=45)


def test_virtual_host_replaces_the_one_in_the_url() -> None:
    settings = AceMQSettings(url="amqp://host:5672/whatever", virtual_host="orders")
    assert settings.broker_url == "amqp://host:5672/orders"


def test_virtual_host_left_alone_when_unset() -> None:
    settings = AceMQSettings(url="amqp://host:5672/orders")
    assert settings.broker_url == "amqp://host:5672/orders"


def test_credentials_travel_in_security_not_in_the_url() -> None:
    settings = AceMQSettings(
        url="amqp://host:5672/", username="app", password=SecretStr("s3cret")
    )
    assert "s3cret" not in settings.broker_url
    security = settings.build_security()
    assert security is not None
    credentials = security.resolve_credentials()
    assert credentials is not None
    assert credentials.username == "app"
    assert credentials.secret == "s3cret"


def test_security_is_none_when_there_is_nothing_to_say() -> None:
    assert AceMQSettings(url="amqp://host:5672/").build_security() is None


def test_tls_settings_reach_the_library() -> None:
    settings = AceMQSettings.model_validate(
        {
            "url": "amqps://host:5671/",
            "tls": {"verification": "nothing-at-all", "server_name": "broker.internal"},
        }
    )
    security = settings.build_security()
    assert security is not None
    assert security.verification is Verification.NOTHING_AT_ALL
    assert security.server_name == "broker.internal"


def test_format_picks_a_codec() -> None:
    assert isinstance(AceMQSettings(format="json").build_codec(), JsonCodec)


def test_unknown_format_names_the_ones_that_exist() -> None:
    with pytest.raises(LookupError) as raised:
        AceMQSettings(format="hieroglyph").build_codec()
    assert "json" in str(raised.value)


def test_retry_is_off_until_it_is_asked_for() -> None:
    assert AceMQSettings().build_retry().max_attempts == 1


def test_retry_ladder_is_built_from_the_settings() -> None:
    settings = AceMQSettings.model_validate(
        {
            "consumer": {
                "retry": {
                    "enabled": True,
                    "max_attempts": 5,
                    "initial_delay": 2,
                    "max_delay": 60,
                    "wait_in_broker_from": 10,
                }
            }
        }
    )
    policy = settings.build_retry()
    assert policy.max_attempts == 5
    assert policy.waits_in_broker(timedelta(seconds=30))
    assert not policy.waits_in_broker(timedelta(seconds=1))


def test_topology_is_none_when_nothing_is_declared() -> None:
    assert AceMQSettings().build_topology() is None


def test_topology_declares_what_it_was_given() -> None:
    settings = AceMQSettings.model_validate(
        {
            "topology": {
                "exchanges": [{"name": "orders", "kind": "topic"}],
                "queues": [{"name": "orders.new", "dead_letter": True}],
                "bindings": [
                    {"queue": "orders.new", "exchange": "orders", "routing_key": "order.*"}
                ],
            }
        }
    )
    topology = settings.build_topology()
    assert topology is not None
    assert "orders" in topology.exchanges
    assert "orders.new" in topology.queues
    # dead_letter brings its own queues with it, which is the point of the flag.
    assert "orders.new.dlq" in topology.queues
    assert "orders.new.parked" in topology.queues
    assert len(topology.bindings) >= 1


def test_publisher_confirms_cannot_be_turned_off() -> None:
    with pytest.raises(ValidationError) as raised:
        AceMQSettings.model_validate({"publisher_confirms": False})
    assert "cannot be turned off" in str(raised.value)
