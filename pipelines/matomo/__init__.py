"""Contains all sources and resources for the Matomo pipeline."""
from typing import Dict, Iterator, List
import dlt
import pendulum
from dlt.common import logger
from dlt.common.typing import DictStrAny, TDataItem
from dlt.extract.source import DltResource
from .helpers.matomo_client import MatomoAPIClient

FIRST_DAY_OF_MILLENNIUM = "2000-01-01"


@dlt.source(max_table_nesting=2)
def matomo(credentials: Dict[str, str] = dlt.secrets.value, queries: List[DictStrAny] = dlt.config.value) -> List[DltResource]:
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
        name = query.get("resource_name")
        batch_data = dlt.resource(get_data_batch, write_disposition="append", name=name)(
            client=client,
            query=query,
            last_date=dlt.sources.incremental("date", primary_key=())  # a primary key defined by the resource name will be used
        )
        resource_list.append(batch_data)
    return resource_list


@dlt.source(max_table_nesting=2)
def matomo_live_data(credentials: Dict[str, str] = dlt.secrets.value) -> List[DltResource]:
    """
    The source for the pipeline.
    :param credentials:
    :return:
    """

    # Create an instance of the Matomo API client
    client = MatomoAPIClient(base_url=credentials["url"], auth_token=credentials["api_token"])
    resource_list = [get_live_counters(client=client), get_last_visits_details(client=client), get_visitor_profile(client=client), get_most_recent_visitor_id(client=client)]

    return resource_list


def get_data_batch(client: MatomoAPIClient, query: DictStrAny, last_date: dlt.sources.incremental[pendulum.DateTime]) -> List[DltResource]:
    """
    Get data in batches
    :param client:
    :param query:
    :return:
    """

    name = query.get("resource_name")
    extra_params = query.get("extra_params", {})
    methods = query.get("methods", [])
    period = query.get("period", "day")
    site_ids = query.get("site_id", 2)
    start_date = query.get("date", None)

    # configure incremental loading. start_date prio: last dlt load -> set start time -> 2000-01-01
    if last_date.last_value:
        # take next day after yesterday to avoid double loads
        start_date = last_date.last_value.add(days=1).to_date_string()
        if start_date:
            logger.warning(f"Using the starting date: {last_date.last_value} for incremental report: {name} and ignoring start date passed as argument {start_date}")
    else:
        start_date = start_date or FIRST_DAY_OF_MILLENNIUM
    # configure end_date to yesterday as a date string and format date_range with starting and end dates
    end_date = pendulum.yesterday().to_date_string()
    date_range = f"{start_date},{end_date}"
    # Get all method data returned for a single query and iterate through pages received
    reports = client.get_query(date=date_range, extra_params=extra_params, methods=methods, period=period, site_id=site_ids)
    for method_data, method in zip(reports, methods):
        # process data for every method and save it in the correct table (1 for each method data)
        table_name = f"{name}_{method}"
        processed_report_generator = process_report(method_data)
        for data in processed_report_generator:
            yield dlt.mark.with_table_name(data, table_name)


def process_report(report: Iterator[TDataItem]) -> Iterator[TDataItem]:
    """
    Helper, loops through multiple formats of method_data and processes them into dlt resources
    :param report: Response from Matomo API containing data for a single method.
    :returns: generator of dicts.
    """
    if isinstance(report, dict):
        for key, value in report.items():
            # TODO: better way of checking for this
            # need to also check here if it is only a single row of data being received
            if not isinstance(value, dict):
                yield report
                break
            value["date"] = pendulum.parse(key)
            yield value
    else:
        try:
            for value in report:
                value["date"] = pendulum.yesterday()
                yield value
        except Exception as e:
            logger.warning(e)


@dlt.resource(name="live_counters", write_disposition="replace")
def get_live_counters(client: MatomoAPIClient):
    """

    :param client:
    :return:
    """
    method_data = client.get_method(date="previous100", period="day", site_id=2, method="Live.getCounters", extra_params={"lastMinutes": 120})
    processed_report_generator = process_report(method_data)
    yield from processed_report_generator


@dlt.resource(name="last_visit_details", write_disposition="replace")
def get_last_visits_details(client: MatomoAPIClient):
    """

    :param client:
    :return:
    """
    method_data = client.get_method(date="previous100", period="day", site_id=2, method="Live.getLastVisitsDetails", extra_params={})
    processed_report_generator = process_report(method_data)
    yield from processed_report_generator


@dlt.resource(name="visitor_profile", write_disposition="replace")
def get_visitor_profile(client: MatomoAPIClient):
    """

    :param client:
    :return:
    """
    method_data = client.get_method(date="previous100", period="day", site_id=2, method="Live.getVisitorProfile", extra_params={})
    processed_report_generator = process_report(method_data)
    yield from processed_report_generator


@dlt.resource(name="most_recent_visitor_id", write_disposition="replace")
def get_most_recent_visitor_id(client: MatomoAPIClient):
    """

    :param client:
    :return:
    """
    method_data = client.get_method(date="previous100", period="day", site_id=2, method="Live.getMostRecentVisitorId", extra_params={})
    processed_report_generator = process_report(method_data)
    yield from processed_report_generator
