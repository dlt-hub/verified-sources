"""
This module contains helpers that process data and make it ready for loading into the database
"""

from typing import Any, Iterator, List, Union
from dlt.common.pendulum import pendulum
from dlt.common.exceptions import MissingDependencyException
from dlt.common.typing import DictStrAny, TDataItem, TDataItems
try:
    from google.analytics.data_v1beta import BetaAnalyticsDataClient
    from google.analytics.data_v1beta.types import DateRange, Dimension, DimensionExpression, DimensionMetadata, GetMetadataRequest, Metadata, Metric, MetricMetadata, MetricType, \
        RunReportRequest, RunReportResponse
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
    metric_dict = {
        "api_name": metric.api_name,
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


def get_report(client: Resource, property_id: int, dimension_list: List[Dimension], metric_list: List[Metric], limit: int, start_date: str, end_date: str) -> Iterator[TDataItem]:
    """
    Gets all the possible pages of reports with the given query parameters. Will process every page and yield a dictionary for every row of the report.
    :param client: The Google Analytics client used to make requests.
    :param property_id: A reference to the Google Analytics project. https://developers.google.com/analytics/devguides/reporting/data/v1/property-id
    :param dimension_list: A list of all the dimensions requested in the query.
    :param metric_list: A list of all the metrics requested in the query
    :param limit: Describes how many rows there should be per page
    :param start_date: The starting date of the query
    :param end_date: The ending date of the query
    :returns: Generator of all rows of data in the report.
    """

    # loop through all the pages
    # the total number of rows is received after the first request, for the first request to be sent through, initializing the row_count to 1 would suffice
    offset = 0
    row_count = 1
    limit = limit
    while offset < row_count:
        # make request to get the particular page
        request = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=dimension_list,
            metrics=metric_list,
            offset=offset,
            limit=limit,
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        )
        # process request
        response = client.run_report(request)
        processed_response_generator = process_report(response=response)
        yield from processed_response_generator
        # update
        row_count = response.row_count
        offset += limit


def process_report(response: RunReportResponse) -> Iterator[TDataItems]:
    """
    Will receive a single page for a report response, process it and return a generator for every row of data in the report page.
    :param response: The API response for a single page of the report.
    :returns: Generator of dicts for every row of the report page.
    """

    metrics_headers = [header.name for header in response.metric_headers]
    dimensions_headers = [header.name for header in response.dimension_headers]
    for row in response.rows:
        response_dict: DictStrAny = {dimension_header: _resolve_dimension_value(dimension_header, dimension_value.value)
                                     for dimension_header, dimension_value in zip(dimensions_headers, row.dimension_values)}
        for i in range(len(metrics_headers)):
            # get metric type and process the value depending on type. Save metric name including type as well for the columns
            metric_type = response.metric_headers[i].type_
            metric_value = process_metric_value(metric_type=metric_type, value=row.metric_values[i].value)
            response_dict[f"{metrics_headers[i]}_{metric_type.name.split('_')[1]}"] = metric_value
        yield response_dict


def process_metric_value(metric_type: MetricType, value: str) -> Union[str, int, float]:
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


def _resolve_dimension_value(dimension_name: str, dimension_value: str) -> Any:
    """
    Helper that will receive a dimension's name and value and convert to a datetime object if need be.
    :param dimension_name: Name of dimension.
    :param dimension_value: Value of the dimension.
    :returns: The value of the dimension with the correct data type.
    """

    if dimension_name == "date":
        return pendulum.from_format(dimension_value, "YYYYMMDD", tz="UTC")
    elif dimension_name == "dateHour":
        return pendulum.from_format(dimension_value, "YYYYMMDDHH", tz="UTC")
    elif dimension_name == "dateHourMinute":
        return pendulum.from_format(dimension_value, "YYYYMMDDHHmm", tz="UTC")
    else:
        return dimension_value
