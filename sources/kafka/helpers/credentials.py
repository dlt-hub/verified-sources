from typing import Any, ClassVar, Dict, FrozenSet, Optional, Tuple

from confluent_kafka import Consumer
from dlt.common.configuration import configspec
from dlt.common.configuration.specs import CredentialsConfiguration
from dlt.common.exceptions import MissingDependencyException
from dlt.common.typing import TSecretStrValue


@configspec
class KafkaCredentials(CredentialsConfiguration):
    """Kafka source credentials.

    The fields are librdkafka configuration properties written with underscores instead of
    periods, for example `bootstrap_servers` is `bootstrap.servers`. Any other property goes to
    `consumer_config` with its librdkafka name and overrides the fields.
    """

    bootstrap_servers: str = None
    group_id: str = "dlt"
    """Consumer group, offsets are kept in the pipeline state and never committed to it"""
    security_protocol: Optional[str] = None
    """PLAINTEXT, SSL, SASL_PLAINTEXT or SASL_SSL"""

    sasl_mechanisms: Optional[str] = None
    """PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER or GSSAPI"""
    sasl_username: Optional[str] = None
    sasl_password: Optional[TSecretStrValue] = None

    sasl_oauthbearer_method: Optional[str] = None
    """`oidc` to get tokens from `sasl_oauthbearer_token_endpoint_url`"""
    sasl_oauthbearer_token_endpoint_url: Optional[str] = None
    sasl_oauthbearer_client_id: Optional[str] = None
    sasl_oauthbearer_client_secret: Optional[TSecretStrValue] = None
    sasl_oauthbearer_scope: Optional[str] = None
    sasl_oauthbearer_extensions: Optional[str] = None

    ssl_ca_location: Optional[str] = None
    ssl_ca_pem: Optional[str] = None
    ssl_certificate_location: Optional[str] = None
    ssl_certificate_pem: Optional[str] = None
    ssl_key_location: Optional[str] = None
    ssl_key_pem: Optional[TSecretStrValue] = None
    ssl_key_password: Optional[TSecretStrValue] = None
    ssl_endpoint_identification_algorithm: Optional[str] = None
    """`https` (librdkafka default) verifies the broker host name, `none` skips it"""

    consumer_config: Optional[Dict[str, Any]] = None
    """librdkafka properties by their names, for example `{"isolation.level": "read_uncommitted"}`"""

    NOT_PROPERTIES: ClassVar[FrozenSet[str]] = frozenset({"consumer_config"})

    def to_native_representation(self) -> Dict[str, Any]:
        """librdkafka configuration of the consumer"""
        config: Dict[str, Any] = {
            "auto.offset.reset": "earliest",
            # offsets are kept in the pipeline state, committed offsets get ahead of loaded data
            "enable.auto.commit": False,
            "enable.partition.eof": True,
        }
        config.update(client_properties(self, self.NOT_PROPERTIES))
        config.update(self.consumer_config or {})
        return config

    def init_consumer(self) -> Consumer:
        """Init a Kafka consumer from this credentials.

        Returns:
            confluent_kafka.Consumer: an initiated consumer.
        """
        return Consumer(self.to_native_representation())


@configspec
class KafkaMskIamCredentials(KafkaCredentials):
    """Amazon MSK credentials with IAM access control.

    Tokens are signed with `aws-msk-iam-sasl-signer-python`, using the AWS profile, the role or
    the default AWS credentials chain, for example `AWS_ACCESS_KEY_ID` and
    `AWS_SECRET_ACCESS_KEY`.
    """

    security_protocol: Optional[str] = "SASL_SSL"
    sasl_mechanisms: Optional[str] = "OAUTHBEARER"
    aws_region: str = None
    aws_profile: Optional[str] = None
    aws_role_arn: Optional[str] = None

    NOT_PROPERTIES: ClassVar[FrozenSet[str]] = KafkaCredentials.NOT_PROPERTIES | {
        "aws_region",
        "aws_profile",
        "aws_role_arn",
    }

    def auth_token(self, oauthbearer_config: Optional[str] = None) -> Tuple[str, float]:
        """Signed token and its expiry in seconds since the epoch, the librdkafka `oauth_cb`"""
        try:
            from aws_msk_iam_sasl_signer import MSKAuthTokenProvider  # type: ignore[import-not-found]
        except ModuleNotFoundError:
            raise MissingDependencyException(
                "Kafka MSK IAM credentials", ["aws-msk-iam-sasl-signer-python"]
            )
        if self.aws_role_arn:
            token, expiry_ms = MSKAuthTokenProvider.generate_auth_token_from_role_arn(
                self.aws_region, self.aws_role_arn
            )
        elif self.aws_profile:
            token, expiry_ms = MSKAuthTokenProvider.generate_auth_token_from_profile(
                self.aws_region, self.aws_profile
            )
        else:
            token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(self.aws_region)
        return token, expiry_ms / 1000

    def to_native_representation(self) -> Dict[str, Any]:
        config = super().to_native_representation()
        config["oauth_cb"] = self.auth_token
        return config


@configspec
class SchemaRegistryCredentials(CredentialsConfiguration):
    """Schema Registry with the Confluent API.

    The fields are client properties written with underscores instead of periods. Any other
    property goes to `client_config` with its name, for example `bearer.auth.credentials.source`.
    """

    url: str = None
    basic_auth_user_info: Optional[TSecretStrValue] = None
    """`<key>:<secret>`, for example a Confluent Cloud API key and secret"""
    ssl_ca_location: Optional[str] = None
    ssl_certificate_location: Optional[str] = None
    ssl_key_location: Optional[str] = None
    client_config: Optional[Dict[str, Any]] = None

    def to_native_representation(self) -> Dict[str, Any]:
        """Configuration of `confluent_kafka.schema_registry.SchemaRegistryClient`"""
        config = client_properties(self, frozenset({"client_config"}))
        config.update(self.client_config or {})
        return config

    def init_client(self) -> Any:
        """A `confluent_kafka.schema_registry.SchemaRegistryClient`"""
        try:
            from confluent_kafka.schema_registry import SchemaRegistryClient
        except ImportError:
            raise MissingDependencyException(
                "Kafka Schema Registry", ["confluent-kafka[schemaregistry]"]
            )
        return SchemaRegistryClient(self.to_native_representation())


def client_properties(
    credentials: CredentialsConfiguration, not_properties: FrozenSet[str]
) -> Dict[str, Any]:
    """The fields that are set, named like client properties: periods instead of underscores"""
    return {
        name.replace("_", "."): getattr(credentials, name)
        for name in credentials.get_resolvable_fields()
        if name not in not_properties and getattr(credentials, name) is not None
    }
