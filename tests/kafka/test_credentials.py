import sys
from types import ModuleType, SimpleNamespace
from typing import Any, Dict, List, Tuple

import pytest
from confluent_kafka import Consumer
from dlt.common.exceptions import MissingDependencyException

from sources.kafka import KafkaCredentials, KafkaMskIamCredentials, kafka_consumer

from tests.kafka.utils import FakeConsumer, make_partition

DEFAULTS = {
    "bootstrap.servers": "localhost:1",
    "group.id": "dlt",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
    "enable.partition.eof": True,
}


def test_consumer_config() -> None:
    # every field is the librdkafka property with periods instead of underscores
    fields = {
        name: f"value_of_{name}"
        for name in KafkaCredentials.get_resolvable_fields()
        if name
        not in KafkaCredentials.NOT_PROPERTIES | {"bootstrap_servers", "group_id"}
    }
    credentials = KafkaCredentials(
        bootstrap_servers="localhost:1",
        consumer_config={"enable.partition.eof": False},
        **fields,
    )
    assert credentials.to_native_representation() == {
        **DEFAULTS,
        **{name.replace("_", "."): value for name, value in fields.items()},
        "enable.partition.eof": False,
    }


@pytest.mark.parametrize(
    "fields",
    [
        {
            "security_protocol": "SASL_SSL",
            "sasl_mechanisms": "SCRAM-SHA-512",
            "sasl_username": "u",
            "sasl_password": "p",
            "ssl_endpoint_identification_algorithm": "none",
        },
        {
            "security_protocol": "SASL_SSL",
            "sasl_mechanisms": "OAUTHBEARER",
            "sasl_oauthbearer_method": "oidc",
            "sasl_oauthbearer_token_endpoint_url": "https://localhost:1/token",
            "sasl_oauthbearer_client_id": "id",
            "sasl_oauthbearer_client_secret": "secret",
            "sasl_oauthbearer_scope": "kafka",
            "sasl_oauthbearer_extensions": "logicalCluster=lkc-1",
        },
    ],
    ids=["scram", "oidc"],
)
def test_librdkafka_accepts_properties(fields: Dict[str, Any]) -> None:
    # librdkafka rejects unknown properties and values
    credentials = KafkaCredentials(bootstrap_servers="localhost:1", **fields)
    Consumer(credentials.to_native_representation()).close()


@pytest.mark.parametrize(
    "environ,expected_type",
    [
        ({"BOOTSTRAP_SERVERS": "localhost:1"}, KafkaCredentials),
        (
            {"BOOTSTRAP_SERVERS": "localhost:1", "AWS_REGION": "eu-central-1"},
            KafkaMskIamCredentials,
        ),
    ],
    ids=["kafka", "msk_iam"],
)
def test_credentials_from_config(
    monkeypatch: pytest.MonkeyPatch, environ: Dict[str, str], expected_type: type
) -> None:
    resolved: List[KafkaCredentials] = []

    def init_consumer(self: KafkaCredentials) -> FakeConsumer:
        resolved.append(self)
        return FakeConsumer({"orders": [make_partition("orders", 0, 1)]})

    monkeypatch.setattr(KafkaCredentials, "init_consumer", init_consumer)
    for name, value in environ.items():
        monkeypatch.setenv(f"SOURCES__KAFKA__CREDENTIALS__{name}", value)
    monkeypatch.setenv(
        "SOURCES__KAFKA__CREDENTIALS__CONSUMER_CONFIG",
        '{"isolation.level": "read_uncommitted"}',
    )
    assert len(list(kafka_consumer("orders"))) == 1
    assert type(resolved[0]) is expected_type
    assert resolved[0].consumer_config == {"isolation.level": "read_uncommitted"}


@pytest.fixture
def msk_signer(monkeypatch: pytest.MonkeyPatch) -> List[Tuple[str, Tuple[str, ...]]]:
    """Replaces `aws_msk_iam_sasl_signer` and records the calls of its token provider"""
    calls: List[Tuple[str, Tuple[str, ...]]] = []

    def provider_method(name: str) -> Any:
        def generate(*args: str) -> Tuple[str, int]:
            calls.append((name, args))
            return f"token_{name}", 1_700_000_000_500

        return generate

    module = ModuleType("aws_msk_iam_sasl_signer")
    module.MSKAuthTokenProvider = SimpleNamespace(  # type: ignore[attr-defined]
        generate_auth_token=provider_method("default"),
        generate_auth_token_from_profile=provider_method("profile"),
        generate_auth_token_from_role_arn=provider_method("role"),
    )
    monkeypatch.setitem(sys.modules, "aws_msk_iam_sasl_signer", module)
    return calls


@pytest.mark.parametrize(
    "fields,expected_call",
    [
        ({}, ("default", ("eu-central-1",))),
        ({"aws_profile": "dev"}, ("profile", ("eu-central-1", "dev"))),
        (
            {"aws_role_arn": "arn:aws:iam::1:role/r"},
            ("role", ("eu-central-1", "arn:aws:iam::1:role/r")),
        ),
    ],
    ids=["default_chain", "profile", "role"],
)
def test_msk_iam_token(
    msk_signer: List[Tuple[str, Tuple[str, ...]]],
    fields: Dict[str, str],
    expected_call: Tuple[str, Tuple[str, ...]],
) -> None:
    credentials = KafkaMskIamCredentials(
        bootstrap_servers="b-1.msk:9098", aws_region="eu-central-1", **fields
    )
    config = credentials.to_native_representation()
    assert config["security.protocol"] == "SASL_SSL"
    assert config["sasl.mechanisms"] == "OAUTHBEARER"
    assert config["oauth_cb"]("ignored") == (
        f"token_{expected_call[0]}",
        1_700_000_000.5,
    )
    assert msk_signer == [expected_call]


def test_msk_iam_without_signer(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "aws_msk_iam_sasl_signer", None)
    credentials = KafkaMskIamCredentials(
        bootstrap_servers="b-1.msk:9098", aws_region="eu-central-1"
    )
    with pytest.raises(MissingDependencyException):
        credentials.auth_token()
