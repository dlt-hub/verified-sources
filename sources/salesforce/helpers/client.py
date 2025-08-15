from typing import Optional, Union, cast

import json

from dlt.sources.helpers.requests import Session
from simple_salesforce import Salesforce
from simple_salesforce.util import Proxies
from simple_salesforce.api import DEFAULT_API_VERSION
from dlt.common.typing import TSecretStrValue
from dlt.common.configuration.specs import (
    CredentialsConfiguration,
    configspec,
    BaseConfiguration,
)
from dlt.common.configuration import with_config
from dlt.common.configuration.exceptions import ConfigurationValueError


@configspec
class SalesforceClientConfiguration(BaseConfiguration):
    domain: Optional[str] = None
    version: Optional[str] = DEFAULT_API_VERSION
    proxies: Optional[str] = None
    client_id: Optional[str] = None

    def get_proxies(self) -> Optional[Proxies]:
        if self.proxies is None:
            return None
        return cast(Proxies, json.loads(self.proxies))


@configspec
class SalesforceCredentialsBase(CredentialsConfiguration):
    """
    The base version of all the SalesforceCredential classes.
    """


@configspec
class SecurityTokenAuth(SalesforceCredentialsBase):
    """
    This class is used to store 'OAuth 2.0 Username Password Flow Credentials' based on a security token.
    """

    user_name: str = None
    password: TSecretStrValue = None
    security_token: TSecretStrValue = None


@configspec
class OrganizationIdAuth(SalesforceCredentialsBase):
    """
    This class is used to store credentials based on `Trusted IP Ranges` in Salesforce.
    """

    user_name: str = None
    password: TSecretStrValue = None
    organization_id: TSecretStrValue = None


@configspec
class InstanceAuth(SalesforceCredentialsBase):
    """
    This class is used to store credentials for direct session access.
    """

    session_id: str = None
    instance: Optional[TSecretStrValue] = None
    instance_url: Optional[TSecretStrValue] = None

    def on_resolved(self) -> None:
        if not self.instance and not self.instance_url:
            raise ConfigurationValueError(
                "InstanceAuth requires either 'instance' or 'instance_url' to be configured. "
                "Please provide one of these fields."
            )


@configspec
class ConsumerKeySecretAuth(SalesforceCredentialsBase):
    """
    This class is used to store 'OAuth 2.0 Username Password Flow Credentials' based on a connected app.
    """

    user_name: str = None
    password: TSecretStrValue = None
    consumer_key: TSecretStrValue = None
    consumer_secret: TSecretStrValue = None


@configspec
class JWTAuth(SalesforceCredentialsBase):
    """
    This class is used to store 'OAuth 2.0 JWT Bearer Flow Credentials'.
    """

    user_name: str = None
    consumer_key: TSecretStrValue = None
    privatekey_file: Optional[TSecretStrValue] = None
    privatekey: Optional[TSecretStrValue] = None
    instance_url: Optional[TSecretStrValue] = None

    def on_resolved(self) -> None:
        if not self.privatekey_file and not self.privatekey:
            raise ConfigurationValueError(
                "JWTAuth requires either 'privatekey_file' or 'privatekey' to be configured. "
                "Please provide one of these fields."
            )


@configspec
class ConsumerKeySecretDomainAuth(SalesforceCredentialsBase):
    """
    This class is used to store 'OAuth 2.0 Client Credentials Flow'.
    """

    consumer_key: TSecretStrValue = None
    consumer_secret: TSecretStrValue = None
    domain: str = None


SalesforceAuth = Union[
    SecurityTokenAuth,
    OrganizationIdAuth,
    ConsumerKeySecretAuth,
    JWTAuth,
    ConsumerKeySecretDomainAuth,
    InstanceAuth,
]


@with_config(spec=SalesforceClientConfiguration)
def make_salesforce_client(
    credentials: SalesforceAuth,
    session: Optional[Session] = None,
    config: SalesforceClientConfiguration = None,
) -> Salesforce:
    """This function passes only the necessary arguments to Salesforce depending on the authentication type.
    Note that version, domain, session and proxies are universal kwargs used for all authentication types in
    the Salesforce object.
    """
    if isinstance(credentials, SecurityTokenAuth):
        return Salesforce(
            version=config.version,
            domain=config.domain,
            session=session,
            proxies=config.get_proxies(),
            username=credentials.user_name,
            password=credentials.password,
            security_token=credentials.security_token,
            client_id=config.client_id,
        )

    elif isinstance(credentials, InstanceAuth):
        return Salesforce(
            version=config.version,
            domain=config.domain,
            session=session,
            proxies=config.get_proxies(),
            session_id=credentials.session_id,
            instance=credentials.instance,
            instance_url=credentials.instance_url,
        )

    elif isinstance(credentials, OrganizationIdAuth):
        return Salesforce(
            version=config.version,
            domain=config.domain,
            session=session,
            proxies=config.get_proxies(),
            username=credentials.user_name,
            password=credentials.password,
            organizationId=credentials.organization_id,
            client_id=config.client_id,
        )

    elif isinstance(credentials, ConsumerKeySecretAuth):
        return Salesforce(
            version=config.version,
            domain=config.domain,
            session=session,
            proxies=config.get_proxies(),
            username=credentials.user_name,
            password=credentials.password,
            consumer_key=credentials.consumer_key,
            consumer_secret=credentials.consumer_secret,
        )

    elif isinstance(credentials, JWTAuth):
        return Salesforce(
            version=config.version,
            domain=config.domain,
            session=session,
            proxies=config.get_proxies(),
            username=credentials.user_name,
            instance_url=credentials.instance_url,
            consumer_key=credentials.consumer_key,
            privatekey_file=credentials.privatekey_file,
            privatekey=credentials.privatekey,
        )
    elif isinstance(credentials, ConsumerKeySecretDomainAuth):
        # NOTE: For this authentication type, domain must be provided as part of the credentials set,
        # we therefore get it from credentials, not config
        return Salesforce(
            version=config.version,
            session=session,
            proxies=config.get_proxies(),
            consumer_key=credentials.consumer_key,
            consumer_secret=credentials.consumer_secret,
            domain=credentials.domain,
        )

    else:
        raise TypeError("You should provide a valid set of credentials.")
