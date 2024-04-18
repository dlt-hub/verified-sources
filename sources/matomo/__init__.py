"""Loads reports and raw visits data from Matomo"""
from typing import Iterator, List, Iterable
import dlt
import pendulum
from dlt.common.typing import DictStrAny, TDataItem
from dlt.sources import DltResource
from .helpers.matomo_client import MatomoAPIClient
from .helpers.data_processing import (
    get_matomo_date_range,
    process_report,
    remove_active_visits,
)


@dlt.source(max_table_nesting=2)
def matomo_reports(
    api_token: str = dlt.secrets.value,
    url: str = dlt.config.value,
    queries: List[DictStrAny] = dlt.config.value,
    site_id: int = dlt.config.value,
) -> Iterable[DltResource]:
    """
    Executes and loads a set of reports defined in `queries` for a Matomo site site_id.

    Args:
        api_token (str): The API access token for authentication. Defaults to the value in the `dlt.secrets` object.
        url (str): The URL of the Matomo server. Defaults to the value in the `dlt.config` object.
        queries (List[DictStrAny]): Dicts that contain information on the reports to retrieve.
        site_id (int): Site ID of the website as per your Matomo account.

    Returns:
        Iterable[DltResource]: List of dlt resources, each containing a Matomo report.
    """

    # Create an instance of the Matomo API client
    client = MatomoAPIClient(api_token=api_token, url=url)
    for query in queries:
        name = query.get("resource_name")
        batch_data = dlt.resource(
            get_data_batch, write_disposition="append", name=name
        )(
            client=client,
            query=query,
            site_id=site_id,
            last_date=dlt.sources.incremental(
                "date", primary_key=()
            ),  # disables unique checks in incremental
        )
        yield batch_data


def get_data_batch(
    client: MatomoAPIClient,
    query: DictStrAny,
    last_date: dlt.sources.incremental[pendulum.DateTime],
    site_id: int,
) -> Iterator[TDataItem]:
    """
    Gets data for a query in a batch.

    Args:
        client (MatomoAPIClient): API Client used to make calls to Matomo API.
        query (DictStrAny): Dict specified in config.toml containing info on what data to retrieve from Matomo API.
        last_date (dlt.sources.incremental[pendulum.DateTime]): Last date timestamp of the last data loaded for the batch data.
        site_id (int): Specifies the Matomo site data is gathered from.

    Returns:
        Iterator[TDataItem]: Iterator yielding data items for the query.
    """

    # unpack query data and process date range
    name = query["resource_name"]
    extra_params = query.get("extra_params", {})
    methods = query["methods"]
    period = query.get("period", "day")
    start_date = query.get("date", None)
    date_range = get_matomo_date_range(start_date=start_date, last_date=last_date)

    # Get all method data returned for a single query and iterate through pages received
    reports = client.get_query(
        date=date_range,
        extra_params=extra_params,
        methods=methods,
        period=period,
        site_id=site_id,
    )
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
    get_live_event_visitors: bool = False,
) -> List[DltResource]:
    """
    Loads a list of visits. Initially loads the current day visits and all visits in `initial_past_days`.
    On subsequent loads, continues from last load. Active visits are skipped until a visit is closed and does not get more actions.

    Args:
        api_token (str): The API access token for authentication. Defaults to the value in the `dlt.secrets` object.
        url (str): The URL of the Matomo server. Defaults to the value in the `dlt.config` object.
        live_events_site_id (int): Site ID of the website for live events.
        initial_load_past_days (int): Number of past days to load on initial load. Defaults to 10.
        visit_timeout_seconds (int): Session timeout in seconds, after which a visit is considered closed. Defaults to 1800.
        visit_max_duration_seconds (int): Maximum visit duration in seconds after which a visit is considered closed. Defaults to 3600.
        get_live_event_visitors (bool): Option to retrieve data about unique visitors. Defaults to False.

    Returns:
        List[DltResource]: A list of resources containing event data.
    """

    # Create an instance of the Matomo API client
    client = MatomoAPIClient(api_token=api_token, url=url)
    # 10 days into the past is a default
    initial_server_timestamp = (
        pendulum.today().subtract(days=initial_load_past_days).timestamp()
    )
    visits_data_generator = get_last_visits(
        client=client,
        site_id=live_events_site_id,
        last_date=dlt.sources.incremental(
            "serverTimestamp", initial_value=initial_server_timestamp
        ),
        visit_timeout_seconds=visit_timeout_seconds,
        visit_max_duration_seconds=visit_max_duration_seconds,
    )
    resource_list = [visits_data_generator]
    if get_live_event_visitors:
        resource_list.append(
            visits_data_generator
            | get_unique_visitors(client=client, site_id=live_events_site_id)
        )
    return resource_list


@dlt.resource(
    name="visits", write_disposition="append", primary_key="idVisit", selected=True
)
def get_last_visits(
    client: MatomoAPIClient,
    site_id: int,
    last_date: dlt.sources.incremental[float],
    visit_timeout_seconds: int = 1800,
    visit_max_duration_seconds: int = 3600,
    rows_per_page: int = 2000,
) -> Iterator[TDataItem]:
    """
    Dlt resource which gets the visits in the given site id for the given timeframe. If there was a previous load, the chosen timeframe is always last_load -> now.
    Otherwise, a start date can be provided to make the chosen timeframe start_date -> now.
    The default behavior would be to get all visits available until now if neither last_load nor start_date are given.

    Args:
        client (MatomoAPIClient): Used to make calls to Matomo API.
        site_id (int): Every site in Matomo has a unique id.
        last_date (dlt.sources.incremental[float]): Date of last load for this resource if it exists.
        visit_timeout_seconds (int): A session timeout in seconds, after that inactivity time, visit is considered closed. Defaults to 1800.
        visit_max_duration_seconds (int): Maximum visit length in seconds after which we consider the visit closed. Defaults to 3600.
        rows_per_page (int): How many rows will be there per page.

    Returns:
        Iterator[TDataItem]: Iterator of dicts containing information on last visits in the given timeframe.
    """

    extra_params = {
        "minTimestamp": last_date.last_value,
    }
    now = pendulum.now().timestamp()
    visits = client.get_method(
        site_id=site_id,
        method="Live.getLastVisitsDetails",
        extra_params=extra_params,
        rows_per_page=rows_per_page,
    )
    for page_no, page in enumerate(visits):
        # remove active visits only from the first page
        if page_no == 0:
            page = remove_active_visits(
                page, visit_timeout_seconds, visit_max_duration_seconds, now
            )
        yield page


@dlt.transformer(
    data_from=get_last_visits,
    write_disposition="merge",
    name="visitors",
    primary_key="visitorId",
)
def get_unique_visitors(
    visits: List[DictStrAny], client: MatomoAPIClient, site_id: int
) -> Iterator[TDataItem]:
    """
    Dlt transformer. Receives information about visits from get_last_visits.

    Args:
        visits (List[DictStrAny]): List of dicts containing information on last visits in the given timeframe.
        client (MatomoAPIClient): Used to make calls to Matomo API.
        site_id (int): Every site in Matomo has a unique id.

    Returns:
        Iterator[TDataItem]: Dict containing information about the visitor.
    """

    visitor_ids = [visit["visitorId"] for visit in visits]
    indexed_visitor_ids = [
        visitor_ids[i : i + 100] for i in range(0, len(visitor_ids), 100)
    ]
    for visitor_list in indexed_visitor_ids:
        method_data = client.get_visitors_batch(
            visitor_list=visitor_list, site_id=site_id
        )
        for method_dict in method_data:
            yield method_dict
