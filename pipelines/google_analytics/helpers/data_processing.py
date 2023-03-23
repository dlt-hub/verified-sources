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
        MetricType,
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

    metrics_headers = [header.name for header in response.metric_headers]
    dimensions_headers = [header.name for header in response.dimension_headers]
    for row in response.rows:
        response_dict = {}
        for i in range(len(dimensions_headers)):
            response_dict[dimensions_headers[i]] = row.dimension_values[i].value
        for i in range(len(metrics_headers)):
            # get metric type and process the value depending on type. Save metric name including type as well for the columns
            metric_type = response.metric_headers[i].type_
            metric_value = process_metric_value(metric_type=metric_type, value=row.metric_values[i].value)
            response_dict[f"{metrics_headers[i]}_{metric_type.name.split('_')[1]}"] = metric_value
        yield response_dict


def process_metric_value(metric_type, value):
    """
    Process metric type, convert it from string to the correct type and return it.
    :param metric_type: The type of the metric
    :param value: The value of the metric as a string
    :returns: The given value converted to the correct Data Type
    """

    # So far according to GA4 documentation these are the correct types: https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/MetricType
    # 0 for strings, 1 for ints and 2-12 are different types of floating points.
    if metric_type.value == 0:
        return value
    elif metric_type.value == 1:
        return int(value)
    else:
        return float(value)
