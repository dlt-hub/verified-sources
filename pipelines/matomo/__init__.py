"""Contains all sources and resources for the Matomo pipeline."""
from typing import Dict, Iterator, List
import dlt
from dlt.common.typing import TDataItem
from dlt.extract.source import DltResource
from .helpers.matomo_client import MatomoAPIClient


@dlt.source(max_table_nesting=2)
def matomo(credentials: Dict[str, str] = dlt.secrets.value, id_site: int = dlt.config.value,
           period: str = dlt.config.value, date: str = dlt.config.value, method: str = dlt.config.value) -> List[DltResource]:
    """

    :param credentials:
    :param id_site:
    :param period:
    :param date:
    :param method:
    :return:
    """

    # Create an instance of the Matomo API client
    matomo_client = MatomoAPIClient(base_url=credentials["url"], auth_token=credentials["api_token"])
    reports = get_reports(matomo_client=matomo_client, id_site=id_site, period=period, date=date, method=method)
    metadata = get_metadata(matomo_client=matomo_client, id_site=id_site, period=period, date=date, method=method)
    return [metadata, reports]


@dlt.resource(write_disposition="append", name="reports")
def get_reports(matomo_client: MatomoAPIClient, id_site: int = dlt.config.value, period: str = dlt.config.value,
                date: str = dlt.config.value, method: str = dlt.config.value) -> Iterator[TDataItem]:
    """

    :param method:
    :param matomo_client:
    :param id_site:
    :param period:
    :param date:
    :return:
    """
    # Get the metadata for the available reports
    reports = matomo_client.get_matomo_data(site_id=id_site, period=period, date=date, api_method=method)

    # Print the metadata
    for report in reports:
        yield report


@dlt.resource(write_disposition="replace", name="metadata")
def get_metadata(matomo_client: MatomoAPIClient, id_site: int = dlt.config.value, period: str = dlt.config.value,
                 date: str = dlt.config.value, method: str = dlt.config.value) -> Iterator[TDataItem]:
    """

    :param method:
    :param matomo_client:
    :param id_site:
    :param period:
    :param date:
    :return:
    """
    # Get the metadata for the available reports
    reports = matomo_client.get_matomo_data(period=period, date=date, site_id=id_site, api_method=method)
    # Print the metadata
    for report in reports:
        yield report
