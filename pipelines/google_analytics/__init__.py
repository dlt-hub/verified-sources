"""
Defines all the sources and resources needed for Google Analytics V4
"""

from typing import Iterator, List
import dlt
from dlt.common import logger
from dlt.common.configuration.specs import GcpClientCredentialsWithDefault
from dlt.common.exceptions import MissingDependencyException
from dlt.common.typing import TDataItem
from dlt.extract.source import DltResource
from .helpers.data_processing import process_dimension, process_metric, process_report

try:
    from google.analytics.data_v1beta import BetaAnalyticsDataClient
    from google.analytics.data_v1beta.types import (
        DateRange,
        Dimension,
        DimensionMetadata,
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
def google_analytics(credentials: GcpClientCredentialsWithDefault = dlt.secrets.value, property_id: int = dlt.config.value) -> List[DltResource]:
    """
    The DLT source for Google Analytics. Will load basic Analytics info to the pipeline.
    :param credentials:
    :param property_id:
    :return resource_list: list containing all the resources in the Google Analytics Pipeline.
    """

    # Build the service object for Google Analytics api.
    # service = build("analyticsreporting", "v4", credentials=credentials.to_service_account_credentials())
    client = BetaAnalyticsDataClient(credentials=credentials.to_service_account_credentials())

    return [get_metadata(client=client, property_id=property_id) | metrics_table,
            get_metadata(client=client, property_id=property_id) | dimensions_table,
            sample_analytics_resource(client=client)
            ]


@dlt.resource(name="sample_data_report", write_disposition="replace")
def sample_analytics_resource(client: Resource) -> Iterator[TDataItem]:
    # TODO: incremental load
    # TODO: helper for report loading
    # TODO: get more data: multiple metrics, multiple dimensions?
    # TODO: pagination handling
    # Using a default constructor instructs the client to use the credentials
    # specified in GOOGLE_APPLICATION_CREDENTIALS environment variable.
    request = RunReportRequest(
        property="properties/293833391",
        dimensions=[Dimension(name="browser")],
        metrics=[Metric(name="activeUsers")],
        date_ranges=[DateRange(start_date="2020-03-31", end_date="today")],
    )
    response = client.run_report(request)
    processed_response_generator = process_report(response=response)
    yield from processed_response_generator


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

