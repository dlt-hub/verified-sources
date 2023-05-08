"""Loads reports and raw visits data from Matomo"""
from typing import Iterator, List
import dlt
import pendulum
from dlt.common.typing import DictStrAny, TDataItem
from dlt.extract.source import DltResource
from .helpers.matomo_client import MatomoAPIClient
from .helpers.data_processing import get_matomo_date_range, process_report, remove_active_visits


@dlt.source(max_table_nesting=2)
def matomo_reports(api_token: str = dlt.secrets.value, url: str = dlt.config.value, queries: List[DictStrAny] = dlt.config.value, site_id: int = dlt.config.value) -> List[DltResource]:
    """
    Executes and loads a set of reports defined in `queries` for a Matomo site site_id.
    :param api_token:
    :param url:
    :param queries: Dicts that contain information on the reports to retrieve
    :param site_id: site id of the website as per your matomo account
    :return: List of dlt resources, each containing a matomo report.
    """

    # Create an instance of the Matomo API client
    client = MatomoAPIClient(api_token=api_token, url=url)
    resource_list = []
    for query in queries:
        name = query.get("resource_name")
        batch_data = dlt.resource(get_data_batch, write_disposition="append", name=name)(
            client=client,
            query=query,
            site_id=site_id,
            last_date=dlt.sources.incremental("date", primary_key=())  # disables unique checks in incremental
        )
        resource_list.append(batch_data)
    return resource_list


def get_data_batch(client: MatomoAPIClient, query: DictStrAny, last_date: dlt.sources.incremental[pendulum.DateTime], site_id: int) -> Iterator[TDataItem]:
    """
    Gets data for a query in a batch.
    :param client: API Client used to make calls to Matomo API
    :param query: Dict specified in config.toml containing info on what data to retrieve from Matomo API
    :param last_date: Last date timestamp of the last data loaded for the batch data.
    :param site_id: Specifies the matomo site data is gathered from
    :return:
    """

    # unpack query data and process date range
    name = query['resource_name']
    extra_params = query.get("extra_params", {})
    methods = query["methods"]
    period = query.get("period", "day")
    start_date = query.get("date", None)
    date_range = get_matomo_date_range(start_date=start_date, last_date=last_date)

    # Get all method data returned for a single query and iterate through pages received
    reports = client.get_query(date=date_range, extra_params=extra_params, methods=methods, period=period, site_id=site_id)
    for method_data, method in zip(reports, methods):
        # process data for every method and save it in the correct table (1 for each method data)
        table_name = f"{name}_{method}"
        processed_report_generator = process_report(method_data)
        for data in processed_report_generator:
            yield dlt.mark.with_table_name(data, table_name)


@dlt.source(max_table_nesting=2)
def matomo_visits(
    api_token: str = dlt.secrets.value,
    url: str = dlt.config.value,
    live_events_site_id: int = dlt.config.value,
    initial_load_past_days: int = 10,
    visit_timeout_seconds: int = 1800,
    visit_max_duration_seconds: int = 3600,
    get_live_event_visitors: bool = False
) -> List[DltResource]:
    """
    Loads a list of visits. Initially loads the current day visits and all visits in `initial_past_days`. On subsequent loads, continues from last load. Active visits are skipped
    until a visit is closed and does not get more actions.
    :param api_token:
    :param url:
    :param live_events_site_id:
    :param load_past_days: how many past days to load on initial load.
    :param visit_timeout_seconds: a session timeout, after that inactivity time, visit is considered closed. this is a setting in Matomo. default is 30 min.
    :param visit_max_duration_seconds: maximum visit length after which we consider the visit closed. this is a safety mechanism - in case of visits that may take days we'd never count them as finalized as well as visits after them.
    :param get_live_event_visitors: Option, if set to true will retrieve data about unique visitors.
    :return: A list of resources containing event data.
    """

    # Create an instance of the Matomo API client
    client = MatomoAPIClient(api_token=api_token, url=url)
    # 10 days into the past is a default
    initial_server_timestamp = pendulum.today().subtract(days=initial_load_past_days).timestamp()
    visits_data_generator = get_last_visits(
        client=client,
        site_id=live_events_site_id,
        last_date=dlt.sources.incremental("serverTimestamp", initial_value=initial_server_timestamp),
        visit_timeout_seconds=visit_timeout_seconds,
        visit_max_duration_seconds=visit_max_duration_seconds
        )
    resource_list = [visits_data_generator]
    if get_live_event_visitors:
        resource_list.append(visits_data_generator | get_unique_visitors(client=client, site_id=live_events_site_id))
    return resource_list


@dlt.resource(name="visits", write_disposition="append", primary_key="idVisit", selected=True)
def get_last_visits(
    client: MatomoAPIClient,
    site_id: int,
    last_date: dlt.sources.incremental[float],
    visit_timeout_seconds: int = 1800,
    visit_max_duration_seconds: int = 3600,
    rows_per_page: int = 2000
) -> Iterator[TDataItem]:
    """
    Dlt resource which gets the visits in the given site id for the given timeframe. If there was a previous load the chosen timeframe is always last_load -> now. Otherwise, a start date can be
    provided to make the chosen timeframe start_date -> now. The default behaviour would be to get all visits available until now if neither last_load nor start_date are given.
    :param client: Used to make calls to Matomo API
    :param site_id: Every site in Matomo has a unique id
    :param last_date: date of last load for this resource if it exists
    :param rows_per_page: How many rows will be there per page.
    :returns: Iterator of dicts containing information on last visits in the given timeframe
    """

    extra_params = {
        "minTimestamp": last_date.last_value,
    }
    now = pendulum.now().timestamp()
    visits = client.get_method(site_id=site_id, method="Live.getLastVisitsDetails", extra_params=extra_params, rows_per_page=rows_per_page)
    for page_no, page in enumerate(visits):
        # remove active visits only from the first page
        if page_no == 0:
            page = remove_active_visits(page, visit_timeout_seconds, visit_max_duration_seconds, now)
        yield page


@dlt.transformer(data_from=get_last_visits, write_disposition="merge", name="visitors", primary_key="visitorId")    # type: ignore
def get_unique_visitors(visits: List[DictStrAny], client: MatomoAPIClient, site_id: int) -> Iterator[TDataItem]:
    """
    Dlt transformer. Receives information about visits from get_last_visits
    :param visits: List of dicts containing information on last visits in the given timeframe
    :param client: Used to make calls to Matomo API
    :param site_id: Every site in Matomo has a unique id
    :returns: Dict containing information about  the visitor
    """

    visitor_ids = [visit["visitorId"] for visit in visits]
    indexed_visitor_ids = [visitor_ids[i:i+100] for i in range(0, len(visitor_ids), 100)]
    for visitor_list in indexed_visitor_ids:
        method_data = client.get_visitors_batch(visitor_list=visitor_list, site_id=site_id)
        for method_dict in method_data:
            yield method_dict
