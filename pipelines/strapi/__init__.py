"""
Basic strapi source
"""
import dlt
from typing import List, Sequence, Iterable
from dlt.extract.source import DltResource

from .helpers import get_endpoint


@dlt.source
def strapi_source(
    endpoints: List[str],
    api_secret_key: str = dlt.secrets.value,
    domain: str = dlt.secrets.value,
) -> Iterable[DltResource]:
    """strapi behaves like a mongo db, with topics and documents
    endpoints represents a list of collections
    """
    for endpoint in endpoints:
        yield dlt.resource(  # type: ignore
            get_endpoint(api_secret_key, domain, endpoint),
            name=endpoint,
            write_disposition="replace",
        )
