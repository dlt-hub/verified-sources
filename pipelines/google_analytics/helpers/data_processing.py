"""
This module contains helpers that process data and make it ready for loading into the database
"""

from typing import Iterator, List
from dlt.common.exceptions import MissingDependencyException
from dlt.common.typing import DictStrAny, TDataItems

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
        GetMetadataRequest,
        RunReportResponse
    )
except ImportError:
    raise MissingDependencyException("Google Analytics API Client", ["google-analytics-data"])
try:
    from apiclient.discovery import build, Resource
except ImportError:
    raise MissingDependencyException("Google API Client", ["google-api-python-client"])


def process_metric(metric: MetricMetadata) -> DictStrAny:
    """
    Will process a MetricMetadata object into a dict
    :param metric: MetricMetadata Object
    :return metric_dict: The dict version of the object
    """
    metric_dict = {"api_name": metric.api_name,
                   "category": metric.category,
                   "custom_definition": metric.custom_definition,
                   "description": metric.description,
                   "expression": metric.expression,
                   "type": metric.type_,
                   "ui_name": metric.ui_name
                   }
    return metric_dict


def process_dimension(dimension: DimensionMetadata) -> DictStrAny:
    """
    Will process a DimensionMetadata object into a dict
    :param dimension: DimensionMetadata Object
    :return dimension_dict: The dict version of the object
    """
    dimension_dict = {"api_name": dimension.api_name,
                      "category": dimension.category,
                      "custom_definition": dimension.custom_definition,
                      "description": dimension.description,
                      "ui_name": dimension.ui_name
                      }
    return dimension_dict


def process_report(response: RunReportResponse, dimensions: List[str], metrics: List[str]) -> Iterator[TDataItems]:
    """

    :param response:
    :param dimensions:
    :param metrics:
    :return:
    """
    for row in response.rows:
        #response_dict = {dimensions[i]: row.dimension_values[i].value for i in range(len(dimensions))}
        yield {"dimension_values": row.dimension_values[0].value, "metric_values": row.metric_values[0].value}
