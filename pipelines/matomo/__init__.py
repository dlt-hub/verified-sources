"""Contains all sources and resources for the Matomo pipeline."""
from typing import Dict, Iterator, List
import dlt
from dlt.common.typing import DictStrAny, TDataItem
from dlt.extract.source import DltResource
from .helpers.matomo_client import MatomoAPIClient


@dlt.source(max_table_nesting=2)
def matomo(credentials: Dict[str, str] = dlt.secrets.value, queries: Iterator[DictStrAny] = dlt.config.value) -> List[DltResource]:
    """
    The source for the pipeline.
    :param credentials:
    :param queries: Dicts that contain information on the reports to retrieve
    :return:
    """

    # Create an instance of the Matomo API client
    client = MatomoAPIClient(base_url=credentials["url"], auth_token=credentials["api_token"])
    resource_list = []
    for query in queries:
        name = query["resource_name"]
        resource_list.append(dlt.resource(get_query(client=client, query=query), name=name, write_disposition="append"))
    return resource_list


def get_query(client: MatomoAPIClient,  query: DictStrAny) -> Iterator[TDataItem]:
    """
    Processes a query, loads it into a resource
    :param client:
    :param query:
    :return:
    """

    date = query["date"]
    extra_params = query["params"]
    methods = query["methods"]
    period = query["period"]
    site_ids = query["site_ids"]
    # Get the metadata for the available reports
    reports = client.get_query(date=date, extra_params=extra_params, methods=methods, period=period, site_ids=site_ids)
    yield from reports


@dlt.resource(write_disposition="replace", name="metadata")
def get_metadata(client: MatomoAPIClient,  id_site: int = dlt.config.value, period: str = dlt.config.value,
                 date: str = dlt.config.value) -> Iterator[TDataItem]:
    """

    :param client:
    :param id_site:
    :param period:
    :param date:
    :return:
    """
    # Get the metadata for the available reports
    reports = client.get_metadata(id_site=id_site, period=period, date=date)
    # Print the metadata
    for report in reports:
        yield report
