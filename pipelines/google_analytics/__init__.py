"""
Defines all the sources and resources needed for Google Analytics V4
"""
from typing import Iterator, List, Optional, Union
from pendulum.datetime import DateTime

import dlt
from dlt.common import logger, pendulum
from dlt.common.configuration.specs import GcpClientCredentialsWithDefault
from dlt.common.exceptions import MissingDependencyException
from dlt.common.typing import TDataItem, DictStrAny
from dlt.extract.source import DltResource
from .helpers.credentials import GoogleAnalyticsCredentialsOAuth
from .helpers.data_processing import get_report, process_dimension, process_metric, process_report
try:
    from google.oauth2.credentials import Credentials
except ImportError:
    raise MissingDependencyException("Google OAuth Library", ["google-auth-oauthlib"])
try:
    from google.analytics.data_v1beta import BetaAnalyticsDataClient
    from google.analytics.data_v1beta.types import DateRange, Dimension, DimensionExpression, DimensionMetadata, GetMetadataRequest, Metadata, Metric, MetricMetadata, RunReportRequest
except ImportError:
    raise MissingDependencyException("Google Analytics API Client", ["google-analytics-data"])
try:
    from apiclient.discovery import Resource
except ImportError:
    raise MissingDependencyException("Google API Client", ["google-api-python-client"])
try:
    from requests_oauthlib import OAuth2Session
except ImportError:
    raise MissingDependencyException("Requests-OAuthlib", ["requests_oauthlib"])

FIRST_DAY_OF_MILLENNIUM = "2000-01-01"


@dlt.source(max_table_nesting=2)
def google_analytics(
    credentials: Union[GoogleAnalyticsCredentialsOAuth, GcpClientCredentialsWithDefault] = dlt.secrets.value,
    property_id: int = dlt.config.value,
    queries: List[DictStrAny] = dlt.config.value,
    start_date: Optional[str] = FIRST_DAY_OF_MILLENNIUM,
    rows_per_page: int = 1000
) -> List[DltResource]:
    """
    The DLT source for Google Analytics. Will load basic Analytics info to the pipeline.
    :param credentials: Credentials to the Google Analytics Account
    :param property_id: A numeric Google Analytics property id. https://developers.google.com/analytics/devguides/reporting/data/v1/property-id
    :param queries: List containing info on the all the reports being requested with all the dimensions and metrics per report.
    :param start_date: Needs to be the string version of date in the format yyyy-mm-dd and some other values: https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/DateRange
    can be left empty for default incremental load behaviour.
    :param rows_per_page: Controls how many rows are retrieved per page in the reports. Default is 10000, maximum possible is 100000.
    :return resource_list: list containing all the resources in the Google Analytics Pipeline.
    """
    # validate input params for the most common mistakes
    try:
        property_id = int(property_id)
    except ValueError:
        raise ValueError(f"{property_id} is an invalid google property id. Please use a numeric id, and not your Measurement ID like G-7F1AE12JLR")
    if property_id == 0:
        raise ValueError("Google Analytics property id is 0. Did you forget to configure it?")
    if not rows_per_page:
        raise ValueError("Rows per page cannot be 0")
    # generate access token for credentials if we are using OAuth2.0
    if isinstance(credentials, GoogleAnalyticsCredentialsOAuth):
        credentials.auth("https://www.googleapis.com/auth/analytics.readonly")
        credentials = Credentials.from_authorized_user_info(info={
            "client_id": credentials.client_id,
            "client_secret": credentials.client_secret,
            "refresh_token": credentials.refresh_token,
            "token": credentials.access_token
        })
    # use service account to authenticate if not using OAuth2.0
    else:
        credentials = credentials.to_service_account_credentials()

    # Build the service object for Google Analytics api.
    client = BetaAnalyticsDataClient(credentials=credentials)
    # get metadata needed for some resources
    metadata = get_metadata(client=client, property_id=property_id)
    resource_list = [metadata | metrics_table, metadata | dimensions_table]
    for query in queries:
        # always add "date" to dimensions so we are able to track the last day of a report
        dimensions=query["dimensions"]
        if "date" not in dimensions:
            # make a copy of dimensions
            dimensions = dimensions + ["date"]
        resource_name = query["resource_name"]
        resource_list.append(
            dlt.resource(basic_report, name=resource_name, write_disposition="append")(
                client=client,
                rows_per_page=rows_per_page,
                property_id=property_id,
                dimensions=dimensions,
                metrics=query["metrics"],
                resource_name=resource_name,
                start_date=start_date,
                last_date=dlt.sources.incremental("date", primary_key=())  # pass empty primary key to avoid unique checks, a primary key defined by the resource will be used
            )
        )
    return resource_list


def basic_report(
    client: Resource,
    rows_per_page: int,
    dimensions: List[str],
    metrics: List[str],
    property_id: int,
    resource_name: str,
    start_date: str,
    last_date: dlt.sources.incremental[DateTime]
) -> Iterator[TDataItem]:
    """
    Retrieves the data for a report given dimensions, metrics and filters required for the report.
    :param client: The Google Analytics client used to make requests.
    :param dimensions: Dimensions for the report, see metadata for full list of dimensions.
    :param metrics: Metrics for the report, see metadata for full list of metrics.
    :param property_id: A reference to the Google Analytics project. https://developers.google.com/analytics/devguides/reporting/data/v1/property-id
    :param rows_per_page: Controls how many rows are retrieved per page in the reports. Default is 10000, maximum possible is 100000.
    :param resource_name: The resource name used to save incremental into dlt state
    :param start_date: Incremental load start_date, default is taken from dlt state if exists.
    :param last_date: Incremental load end date, default is taken from dlt state if exists.
    :returns: Generator of all rows of data in the report.
    """

    # grab the start time from last dlt load if not filled, if that is also empty then use the first day of the millennium as the start time instead
    if last_date.last_value:
        if start_date != FIRST_DAY_OF_MILLENNIUM:
            logger.warning(f"Using the starting date: {last_date.last_value} for incremental report: {resource_name} and ignoring start date passed as argument {start_date}")
        # take next day after yesterday to avoid double loads
        start_date = last_date.last_value.add(days=1).to_date_string()
    else:
        start_date = start_date or FIRST_DAY_OF_MILLENNIUM
    # configure end_date to yesterday as a date string
    end_date = pendulum.yesterday().to_date_string()
    # fill dimensions and metrics with the proper api client objects
    dimension_list = [Dimension(name=dimension) for dimension in dimensions]
    metric_list = [Metric(name=metric) for metric in metrics]
    # use helpers to send request and process it
    processed_response = get_report(client=client, property_id=property_id, dimension_list=dimension_list, metric_list=metric_list, limit=rows_per_page, start_date=start_date, end_date=end_date)
    yield from processed_response


@dlt.resource(selected=False)
def get_metadata(client: Resource, property_id: int) -> Iterator[Metadata]:
    """
    Get all the metrics and dimensions for a report
    :param client: The Google Analytics client used to make requests.
    :param property_id: A reference to the Google Analytics project. https://developers.google.com/analytics/devguides/reporting/data/v1/property-id
    :returns: Generator of Metadata objects. Only 1 is expected but yield is used as dlt resources require yield to be used.
    """

    # make call to get metadata
    request = GetMetadataRequest(
        name=f"properties/{property_id}/metadata"
    )
    metadata: Metadata = client.get_metadata(request)
    yield metadata


@dlt.transformer(data_from=get_metadata, write_disposition="replace", name="metrics")   # type: ignore
def metrics_table(metadata: Metadata) -> Iterator[TDataItem]:
    """
    Loads data for metrics
    :param metadata: Metadata class object which contains all the information stored in the GA4 metadata
    :return: Generator of dicts, 1 metric at a time
    """

    for metric in metadata.metrics:
        processed_metric = process_metric(metric=metric)
        yield processed_metric


@dlt.transformer(data_from=get_metadata, write_disposition="replace", name="dimensions")    # type: ignore
def dimensions_table(metadata: Metadata) -> Iterator[TDataItem]:
    """
    Loads data for dimensions
    :param metadata: Metadata class object which contains all the information stored in the GA4 metadata
    :return: Generator of dicts, 1 dimension at a time
    """
    for dimension in metadata.dimensions:
        processed_dimension = process_dimension(dimension=dimension)
        yield processed_dimension
