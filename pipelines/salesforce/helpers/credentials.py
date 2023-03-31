"""
This module handles how credentials are read in dlt sources
"""
from typing import ClassVar, List
from dlt.common.configuration import configspec
from dlt.common.configuration.specs import CredentialsConfiguration
from dlt.common.typing import TSecretValue


@configspec
class SalesforceCredentialsBase(CredentialsConfiguration):
    """
    The Base version of all the SalesforceCredential classes.
    """
    subdomain: str
    __config_gen_annotations__: ClassVar[List[str]] = []


@configspec
class SalesforceCredentialsToken(SalesforceCredentialsBase):
    """
    This class is used to store credentials for Token Authentication
    """
    username: str
    password: TSecretValue
    security_token: TSecretValue
    client_id: str
    client_secret: TSecretValue
