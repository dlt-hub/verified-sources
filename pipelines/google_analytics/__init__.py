"""
Defines all the sources and resources needed for Google Analytics V4
"""
from typing import Iterator, List, Union
import dlt
from dlt.common import pendulum
from dlt.common.configuration.specs import GcpClientCredentialsWithDefault
from dlt.common.exceptions import MissingDependencyException
from dlt.common.typing import TDataItem, DictStrAny
from dlt.extract.source import DltResource
from .helpers.credentials import GoogleAnalyticsCredentialsOAuth
from .helpers.data_processing import get_report, process_dimension, process_metric, process_report
try:
    from google.analytics.data_v1beta import BetaAnalyticsDataClient
    from google.analytics.data_v1beta.types import DateRange, Dimension, DimensionExpression, DimensionMetadata, GetMetadataRequest, Metadata, Metric, MetricMetadata, RunReportRequest
except ImportError:
    raise MissingDependencyException("Google Analytics API Client", ["google-analytics-data"])
try:
    from apiclient.discovery import build, Resource
except ImportError:
    raise MissingDependencyException("Google API Client", ["google-api-python-client"])


FIRST_DAY_OF_MILLENNIUM = "2000-01-01"


@dlt.source(max_table_nesting=2)
def google_analytics(credentials: Union[GoogleAnalyticsCredentialsOAuth, GcpClientCredentialsWithDefault] = dlt.secrets.value,
                     property_id: int = dlt.config.value,
                     rows_per_page: int = dlt.config.value,
                     queries: List[DictStrAny] = dlt.config.value,
                     start_date: str = dlt.config.value,
                     end_date: str = dlt.config.value) -> List[DltResource]:
    """
    The DLT source for Google Analytics. Will load basic Analytics info to the pipeline.
    :param credentials: Credentials to the Google Analytics Account
    :param property_id: A reference to the Google Analytics project. https://developers.google.com/analytics/devguides/reporting/data/v1/property-id
    :param rows_per_page: Controls how many rows are retrieved per page in the reports. Default is 10000, maximum possible is 100000.
    :param queries: List containing info on the all the reports being requested with all the dimensions and metrics per report.
    :param start_date: Needs to be the string version of date in the format yyyy-mm-dd and some other values: https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/DateRange
    can be left empty for default incremental load behaviour.
    :param end_date: Needs to be the string version of date in the format yyyy-mm-dd and some other values: https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/DateRange
    can be left empty for default incremental load behaviour.
    :return resource_list: list containing all the resources in the Google Analytics Pipeline.
    """

    # generate access token for credentials if we are using OAuth2.0
    if isinstance(credentials, GoogleAnalyticsCredentialsOAuth):
        credentials.auth()
    # Build the service object for Google Analytics api.
    client = BetaAnalyticsDataClient(credentials=credentials.to_service_account_credentials())
    # get metadata needed for some resources
    metadata = get_metadata(client=client, property_id=property_id)
    resource_list = [metadata | metrics_table, metadata | dimensions_table]
    for query in queries:
        name = query["resource_name"]
        dimensions = query["dimensions"]
        metrics = query["metrics"]
        resource_list.append(dlt.resource(basic_report(client=client, rows_per_page=rows_per_page, property_id=property_id, dimensions=dimensions, metrics=metrics,
                                                       resource_name=name, start_date=start_date, end_date=end_date),
                                          name=name, write_disposition="replace"))
    return resource_list


def basic_report(client: Resource, rows_per_page: int, dimensions: List[str], metrics: List[str], property_id: int,
                 resource_name: str, start_date: str = None, end_date: str = None) -> Iterator[TDataItem]:
    """
    Retrieves the data for a report given dimensions, metrics and filters required for the report.
    :param client: The Google Analytics client used to make requests.
    :param dimensions: Dimensions for the report, see metadata for full list of dimensions.
    :param metrics: Metrics for the report, see metadata for full list of metrics.
    :param property_id: A reference to the Google Analytics project. https://developers.google.com/analytics/devguides/reporting/data/v1/property-id
    :param rows_per_page: Controls how many rows are retrieved per page in the reports. Default is 10000, maximum possible is 100000.
    :param resource_name: The resource name used to save incremental into dlt state
    :param start_date: Incremental load start_date, default is taken from dlt state if exists
    :param end_date: Incremental load end_date, default is always set to the date when the load is started.
    :returns: Generator of all rows of data in the report.
    """

    # grab the start time from last dlt load if not filled, if that is also empty then use the first day of the millennium as the start time instead
    if not start_date:
        start_date = dlt.state().setdefault(f"last_load_{resource_name}", FIRST_DAY_OF_MILLENNIUM)
    else:
        # if start time is given convert to a string as required by the GA4 API
        start_date = start_date
    # configure end_date
    if not end_date:
        end_date = pendulum.today().to_date_string()
    else:
        end_date = end_date

    # fill dimensions and metrics with the proper api client objects
    dimension_list = [Dimension(name=dimension) for dimension in dimensions]
    metric_list = [Metric(name=metric) for metric in metrics]
    # use helpers to send request and process it
    processed_response = get_report(client=client, property_id=property_id, dimension_list=dimension_list, metric_list=metric_list, limit=rows_per_page, start_date=start_date, end_date=end_date)
    yield from processed_response
    # update the last load time in the dlt state after a successful load
    dlt.state()[f"last_load_{resource_name}"] = end_date


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


@dlt.transformer(data_from=get_metadata, write_disposition="replace", name="metrics")
def metrics_table(metadata: Metadata) -> Iterator[TDataItem]:
    """
    Loads data for metrics
    :param metadata: Metadata class object which contains all the information stored in the GA4 metadata
    :return: Generator of dicts, 1 metric at a time
    """

    for metric in metadata.metrics:
        processed_metric = process_metric(metric=metric)
        yield processed_metric


@dlt.transformer(data_from=get_metadata, write_disposition="replace", name="dimensions")
def dimensions_table(metadata: Metadata) -> Iterator[TDataItem]:
    """
    Loads data for dimensions
    :param metadata: Metadata class object which contains all the information stored in the GA4 metadata
    :return: Generator of dicts, 1 dimension at a time
    """
    for dimension in metadata.dimensions:
        processed_dimension = process_dimension(dimension=dimension)
        yield processed_dimension
