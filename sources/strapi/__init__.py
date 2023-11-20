"""
Basic strapi source
"""
import dlt
from typing import List, Iterable
from dlt.sources import DltResource

from .helpers import get_endpoint


@dlt.source
def strapi_source(
    endpoints: List[str],
    api_secret_key: str = dlt.secrets.value,
    domain: str = dlt.secrets.value,
) -> Iterable[DltResource]:
    """
    Source function for retrieving data from Strapi.

    Args:
        endpoints (List[str]): List of collections to retrieve data from.
        api_secret_key (str): API secret key for authentication. Defaults to the value in the `dlt.secrets` object.
        domain (str): Domain name for the Strapi API. Defaults to the value in the `dlt.secrets` object.

    Yields:
        DltResource: Data resources from the specified collections.
    """
    for endpoint in endpoints:
        yield dlt.resource(  # type: ignore
            get_endpoint(api_secret_key, domain, endpoint),
            name=endpoint,
            write_disposition="replace",
        )
