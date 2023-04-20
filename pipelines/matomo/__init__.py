"""Contains all sources and resources for the Matomo pipeline."""
import logging
from typing import Dict, Iterator, List
import dlt
from dlt.common import logger
from dlt.common.typing import DictStrAny, TDataItem, TDataItems
from dlt.extract.source import DltResource
from .helpers.matomo_client import MatomoAPIClient


@dlt.source(max_table_nesting=2)
def matomo(credentials: Dict[str, str] = dlt.secrets.value, queries: List[DictStrAny] = dlt.config.value, filter_limit: int = dlt.config.value) -> List[DltResource]:
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
        batch_data = get_data_batch(client=client, query=query, filter_limit=filter_limit)
        resource_list.extend(batch_data)
    return resource_list


def get_data_batch(client: MatomoAPIClient, query: DictStrAny, filter_limit: int) -> List[DltResource]:
    """
    Get all the data in batches.
    :param client:
    :param query:
    :return:
    """

    name = query.get("resource_name")
    date = query.get("date", "previous1000")
    extra_params = query.get("extra_params", {})
    methods = query.get("methods", [])
    period = query.get("period", "date")
    site_ids = query.get("site_id", 2)
    filter_limit = filter_limit
    # Get the metadata for the available reports
    reports = client.get_query(date=date, extra_params=extra_params, methods=methods, period=period, site_id=site_ids, filter_limit=filter_limit)
    resource_list = []
    for report in reports:
        for method_data, method in zip(report, methods):
            method_resource = dlt.resource(process_report(method_data), name=f"{name}_{method}", write_disposition="append")
            resource_list.append(method_resource)
    return resource_list


def process_report(report: Iterator[TDataItem]) -> Iterator[TDataItem]:
    """

    :param report:
    :return:
    """

    if isinstance(report, dict):
        for key, value in report.items():
            # TODO: better way of checking for this
            # need to also check here if it is only a single row of data being received
            if not isinstance(value, dict):
                yield report
                break
            value["date"] = key
            yield value
    else:
        try:
            for value in report:
                yield value
        except Exception as e:
            logger.warning(e)
