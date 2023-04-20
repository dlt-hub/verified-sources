"""Contains all sources and resources for the Matomo pipeline."""
import logging
from typing import Dict, Iterator, List
import dlt
import pendulum
from dlt.common import logger
from dlt.common.typing import DictStrAny, TDataItem, TDataItems
from dlt.extract.source import DltResource
from .helpers.matomo_client import MatomoAPIClient

FIRST_DAY_OF_MILLENNIUM = "2000-01-01"


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
    stuff = get_triggered()
    resource_list = [stuff]
    for query in queries:
        name = query.get("resource_name")
        methods = query.get("methods", [])
        extra_params = query.get("extra_params", {})
        period = query.get("period", "day")
        site_id = query.get("site_id", 2)
        filter_limit = filter_limit
        batch_data = dlt.resource(get_data_batch(client=client,
                                                 query=query,
                                                 filter_limit=filter_limit),
                                  name=name)
        for method_data, method in zip(batch_data, methods):
            method_resource = dlt.resource(process_report(method_data), name=f"{name}_{method}", write_disposition="append")
            resource_list.append(method_resource)
    return resource_list


def get_data_batch(client: MatomoAPIClient, query: DictStrAny, filter_limit: int) -> List[DltResource]:
    """
    Get data in batches
    :param client:
    :param query:
    :param filter_limit:
    :return:
    """

    name = query.get("resource_name")
    extra_params = query.get("extra_params", {})
    methods = query.get("methods", [])
    period = query.get("period", "day")
    site_ids = query.get("site_id", 2)
    filter_limit = filter_limit

    # configure incremental loading. start_date prio: last dlt load -> set start time -> 2000-01-01
    # grab the start time from last dlt load if not filled, if that is also empty then use the first day of the millennium as the start time instead
    if f"last_load_{name}" in dlt.state():
        # take next day after yesterday to avoid double loads
        start_date = pendulum.parse(dlt.state()[f"last_load_{name}"]).to_date_string()
        if "date" in query:
            logger.warning(f"Using the starting date: {start_date} for incremental report: {name} and ignoring start date passed as argument {query['date']}")
    else:
        start_date = query.get("date", FIRST_DAY_OF_MILLENNIUM)
    date_range = f"{start_date},yesterday"
    # Get data for all queries
    reports = client.get_query(date=date_range, extra_params=extra_params, methods=methods, period=period, site_id=site_ids, filter_limit=filter_limit)
    for report in reports:
        yield report
    # save last_load in dlt state
    dlt.state()[f"last_load_{name}"] = pendulum.yesterday().to_date_string()


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
