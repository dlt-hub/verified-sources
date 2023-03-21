"""
This module handles how credentials are read in dlt sources
"""
from typing import ClassVar, List
from dlt.common.configuration import configspec
from dlt.common.configuration.specs import CredentialsConfiguration
from dlt.common.typing import TSecretValue


class GoogleAnalyticsCredentialsBase(CredentialsConfiguration):
    """
    The Base version of all the GoogleAnalyticsCredentials classes.
    """
    __config_gen_annotations__: ClassVar[List[str]] = []


@configspec
class GoogleAnalyticsCredentials(GoogleAnalyticsCredentialsBase):
    """
    This class is used to store credentials Google Analytics
    """
    pass
