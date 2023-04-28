"""Helpers for processing data from API calls."""

from typing import Iterator, List, Union
import dlt
from dlt.common import logger
from dlt.common.pendulum import pendulum
from dlt.common.typing import DictStrAny, TDataItem

FIRST_DAY_OF_MILLENNIUM = "2000-01-01"
FIRST_DAY_OF_MILLENNIUM_TIMESTAMP = 946684800.0


def process_report(report: Iterator[TDataItem]) -> Union[DictStrAny, List[TDataItem]]:
    """
    Helper, loops through multiple formats of method_data and processes them into dlt resources
    :param report: Response from Matomo API containing data for a single method.
    :returns: generator of dicts.
    """

    processed_report = []
    if isinstance(report, dict):
        for key, value in report.items():
            if isinstance(value, list):
                for el in value:
                    el["date"] = pendulum.parse(key)
                    processed_report.append(el)
            elif isinstance(value, dict):
                value["date"] = pendulum.parse(key)
                processed_report.append(value)
            else:
                return report
    else:
        try:
            for value in report:
                value["date"] = pendulum.yesterday()
                processed_report.append(value)
        except Exception as e:
            logger.warning(str(e))
            raise ValueError("Method doesn't support report data!")
    return processed_report


def process_visitors(visitors_list: List[DictStrAny]) -> Iterator[TDataItem]:
    """
    Helper to process data from Matomo live visits data.
    :param visitors_list: A list of dicts as returned by Matomo API
    :returns: A generator of processed dicts from the raw API call
    """
    for visitor_info in visitors_list:
        last_visit_datetime = pendulum.from_timestamp(visitor_info["serverTimestamp"])
        visitor_info["last_visit_datetime"] = last_visit_datetime
        yield visitor_info


def get_matomo_date_range(start_date: str, last_date: dlt.sources.incremental[pendulum.DateTime]) -> str:
    """
    Given a default starting date and the last load date for a resource, it will output a valid date range for Matomo API data retrieval
    :param start_date: Default starting date string
    :param last_date: Last date loaded saved in dlt state
    :return date_range: formatted string for a date range - starting_date,end_date
    """
    # configure incremental loading. start_date prio: last dlt load -> set start time -> 2000-01-01
    if last_date.last_value:
        if start_date:
            logger.warning(f"Using the starting date: {last_date.last_value} for the query and ignoring start date passed as argument {start_date}!")
        # take next day after yesterday to avoid double loads
        start_date = last_date.last_value.add(days=1).to_date_string()
    else:
        start_date = start_date or FIRST_DAY_OF_MILLENNIUM
    # configure end_date to yesterday as a date string and format date_range with starting and end dates
    end_date = pendulum.yesterday().to_date_string()
    date_range = f"{start_date},{end_date}"
    return date_range
