"""
Defines all the sources and resources needed for Google Analytics V4
"""

from typing import Iterator, Optional, Sequence, Union, List
import dlt
from dlt.common.exceptions import MissingDependencyException
from dlt.common.typing import TDataItem
from dlt.extract.source import DltResource
try:
    from google.analytics.data_v1beta import BetaAnalyticsDataClient
    from google.analytics.data_v1beta.types import (
        DateRange,
        Dimension,
        Metric,
        RunReportRequest,
    )
except ImportError:
    raise MissingDependencyException("Google Analytics API Client", ["google-analytics-data"])


@dlt.source(max_table_nesting=2)
def google_analytics(credentials: dict = dlt.secrets.value) -> List[DltResource]:
    """
    The DLT source for Google Analytics. Will load basic Analytics info to the pipeline.
    :param credentials:
    :return resource_list: list containing all the resources in the Google Analytics Pipeline.
    """
    resource_list = []
    return resource_list

