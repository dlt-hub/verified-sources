"""Contains all sources and resources for the Matomo pipeline."""
from typing import Dict, Iterator, List
import dlt
import pendulum
from dlt.common.typing import DictStrAny, TDataItem
from dlt.extract.source import DltResource
from .helpers.matomo_client import MatomoAPIClient
from .helpers.data_processing import FIRST_DAY_OF_MILLENNIUM, get_matomo_last_load_timestamp, get_matomo_date_range, process_report, process_visitors


@dlt.source(max_table_nesting=2)
def matomo_reports(credentials: Dict[str, str] = dlt.secrets.value, queries: List[DictStrAny] = dlt.config.value) -> List[DltResource]:
    """
    The source for the pipeline.
    :param credentials:
    :param queries: Dicts that contain information on the reports to retrieve
    :return:
    """

    # Create an instance of the Matomo API client
    client = MatomoAPIClient(credentials=credentials)
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


def get_data_batch(client: MatomoAPIClient, query: DictStrAny, last_date: dlt.sources.incremental[pendulum.DateTime]) -> Iterator[TDataItem]:
    """
    Gets data for a query in a batch.
    :param client: API Client used to make calls to Matomo API
    :param query: Dict specified in config.toml containing info on what data to retrieve from Matomo API
    :param last_date: Last date timestamp of the last data loaded for the batch data.
    :return:
    """

    # unpack query data and process date range
    name = query['resource_name']
    extra_params = query.get("extra_params", {})
    methods = query["methods"]
    period = query.get("period", "day")
    site_id = query["site_id"]
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


@dlt.source(max_table_nesting=4)
def matomo_events(credentials: Dict[str, str] = dlt.secrets.value, live_events_site_id: int = dlt.config.value) -> List[DltResource]:
    """
    The source for the pipeline.
    :param credentials:
    :param live_events_site_id:
    :return: A list of resources containing event data.
    """

    # Create an instance of the Matomo API client
    client = MatomoAPIClient(credentials=credentials)
    visits_data_generator = get_last_visits(client=client, last_date=dlt.sources.incremental("last_visit_datetime", primary_key=()), site_id=live_events_site_id)
    resource_list = [visits_data_generator, visits_data_generator | get_unique_visitors(client=client, site_id=live_events_site_id)]
    return resource_list


@dlt.resource(name="visits", write_disposition="merge", primary_key="idVisit", selected=True)
def get_last_visits(client: MatomoAPIClient, site_id: int,  last_date: dlt.sources.incremental[pendulum.DateTime]) -> Iterator[TDataItem]:
    """
    Dlt resource which gets the visits in the given site id for the given timeframe. If there was a previous load the chosen timeframe is always last_load -> now. Otherwise, a start date can be
    provided to make the chosen timeframe start_date -> now. The default behaviour would be to get all visits available until now if neither last_load nor start_date are given.
    :param client: Used to make calls to Matomo API
    :param site_id: Every site in Matomo has a unique id
    :param last_date: date of last load for this resource if it exists
    :returns: Iterator of dicts containing information on last visits in the given timeframe
    """

    min_timestamp = get_matomo_last_load_timestamp(last_date=last_date)
    method_data = client.get_method(site_id=site_id, method="Live.getLastVisitsDetails", extra_params={"minTimestamp": min_timestamp})
    processed_report_generator = process_visitors(method_data)
    for processed_dict in processed_report_generator:
        yield processed_dict


@dlt.transformer(data_from=get_last_visits, write_disposition="merge", name="visitors", primary_key="visitorId")    # type: ignore
def get_unique_visitors(visit_details: DictStrAny, client: MatomoAPIClient, site_id: int) -> Iterator[TDataItem]:
    """
    Dlt transformer. Receives information about visits from get_last_visits
    :param visit_details: Dict containing information on last visits in the given timeframe
    :param client: Used to make calls to Matomo API
    :param site_id: Every site in Matomo has a unique id
    :returns: Dict containing information about  the visitor
    """
    visitor_id = visit_details["visitorId"]
    method_data = client.get_method(site_id=site_id, method="Live.getVisitorProfile", extra_params={"visitorId": visitor_id})
    yield method_data

