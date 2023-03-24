"""
Defines all the sources and resources needed for Google Analytics V4
"""

from typing import Iterator, List, Optional
import dlt
from dlt.common import logger
from dlt.common.configuration.specs import GcpClientCredentialsWithDefault
from dlt.common.exceptions import MissingDependencyException
from dlt.common.typing import TDataItem, DictStrAny
from dlt.extract.source import DltResource
from .helpers.data_processing import process_dimension, process_metric, process_report

try:
    from google.analytics.data_v1beta import BetaAnalyticsDataClient
    from google.analytics.data_v1beta.types import (
        DateRange,
        Dimension,
        DimensionExpression,
        DimensionMetadata,
        Filter,
        FilterExpression,
        Metadata,
        Metric,
        MetricMetadata,
        RunReportRequest,
        GetMetadataRequest
    )
except ImportError:
    raise MissingDependencyException("Google Analytics API Client", ["google-analytics-data"])
try:
    from apiclient.discovery import build, Resource
except ImportError:
    raise MissingDependencyException("Google API Client", ["google-api-python-client"])


@dlt.source(max_table_nesting=2)
def google_analytics(credentials: GcpClientCredentialsWithDefault = dlt.secrets.value,
                     property_id: int = dlt.config.value,
                     rows_per_page: int = dlt.config.value,
                     queries: List[DictStrAny] = dlt.config.value) -> List[DltResource]:
    """
    The DLT source for Google Analytics. Will load basic Analytics info to the pipeline.
    :param credentials: Credentials to the Google Analytics Account
    :param property_id: A reference to the Google Analytics project. https://developers.google.com/analytics/devguides/reporting/data/v1/property-id
    :param rows_per_page: Controls how many rows are retrieved per page in the reports. Default is 10000, maximum possible is 100000.
    :param queries:
    :return resource_list: list containing all the resources in the Google Analytics Pipeline.
    """

    # Build the service object for Google Analytics api.
    client = BetaAnalyticsDataClient(credentials=credentials.to_service_account_credentials())
    # get metadata needed for some resources
    metadata = get_metadata(client=client, property_id=property_id)
    resource_list = [metadata | metrics_table, metadata | dimensions_table]
    for query in queries:
        name = query["resource_name"]
        dimensions = query["dimensions"]
        metrics = query["metrics"]
        resource_list.append(dlt.resource(basic_report(client=client, rows_per_page=rows_per_page, property_id=property_id, dimensions=dimensions, metrics=metrics),
                                          name=name, write_disposition="replace"))
    return resource_list


def basic_report(client: Resource, rows_per_page: int, dimensions: List[str], metrics: List[str], property_id: int) -> Iterator[TDataItem]:
    """
    Retrieves the data for a report given dimensions, metrics and filters required for the report.
    :param client: The Google Analytics client used to make requests.
    :param dimensions: Dimensions for the report, see metadata for full list of dimensions.
    :param metrics: Metrics for the report, see metadata for full list of metrics.
    :param property_id: A reference to the Google Analytics project. https://developers.google.com/analytics/devguides/reporting/data/v1/property-id
    :param rows_per_page: Controls how many rows are retrieved per page in the reports. Default is 10000, maximum possible is 100000.
    :return:
    """

    # TODO: incremental load
    # TODO: get more data: multiple metrics, multiple dimensions?
    # fill dimensions and metrics with the proper api client objects
    dimension_list = []
    metric_list = []
    for dimension in dimensions:
        dimension_list.append(Dimension(name=dimension))
    for metric in metrics:
        metric_list.append(Metric(name=metric))
    # loop through all the pages,
    # the total number of rows is received after the first request, for the first request to be sent through, initializing the row_count to 1 would suffice
    offset = 0
    row_count = 1
    limit = rows_per_page
    while offset < row_count:
        # make request to get the particular page
        request = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=dimension_list,
            metrics=metric_list,
            offset=offset,
            limit=limit,
            date_ranges=[DateRange(start_date="2020-03-31", end_date="today")],
        )
        response = client.run_report(request)

        processed_response_generator = process_report(response=response, metrics=metrics, dimensions=dimensions)
        yield from processed_response_generator

        # update
        row_count = response.row_count
        offset += limit


@dlt.resource(selected=False)
def get_metadata(client: Resource, property_id: int) -> Iterator[TDataItem]:
    """
    Get all the metrics and dimensions for a report
    :param client:
    :param property_id:
    :return:
    """

    # make call to get metadata
    request = GetMetadataRequest(
        name=f"properties/{property_id}/metadata"
    )
    metadata = client.get_metadata(request)
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
