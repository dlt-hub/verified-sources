"""Helpers for processing data from API calls."""

from typing import Iterator, List, Union
import dlt
from dlt.common import logger
from dlt.common.pendulum import pendulum
from dlt.common.typing import DictStrAny, TDataItem

from ..settings import DEFAULT_START_DATE


def process_report(report: Iterator[TDataItem]) -> Union[DictStrAny, List[TDataItem]]:
    """
    Processes the report data from the Matomo API into a suitable format.

    Args:
        report (Iterator[TDataItem]): Response from Matomo API containing data for a single method.

    Returns:
        Union[DictStrAny, List[TDataItem]]: Processed report data in a suitable format.
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


def remove_active_visits(
    visits: List[DictStrAny],
    visit_timeout_seconds: int,
    visit_max_duration_seconds: int,
    now: float,
) -> List[DictStrAny]:
    """
    Removes active visits from the list of visits based on timeout and max duration criteria.

    Args:
        visits (List[DictStrAny]): List of visits.
        visit_timeout_seconds (int): Session timeout in seconds.
        visit_max_duration_seconds (int): Maximum visit duration in seconds.
        now (float): Current timestamp.

    Returns:
        List[DictStrAny]: Updated list of visits with active visits removed.
    """
    cutoff = -1
    for idx, visit in enumerate(visits):
        last_action_delta = now - visit["lastActionTimestamp"]
        visit_duration = now - visit["firstActionTimestamp"]
        if (
            last_action_delta < visit_timeout_seconds
            and visit_duration < visit_max_duration_seconds
        ):
            # print(f"visit {visit['idVisit']} must be eliminated due to {last_action_delta} and {visit_duration}")
            # remove this element and all elements earlier in the list
            cutoff = idx
    return visits[cutoff + 1 :]


def get_matomo_date_range(
    start_date: str, last_date: dlt.sources.incremental[pendulum.DateTime]
) -> str:
    """
    Generates a valid date range for Matomo API data retrieval based on the start date and last load date.

    Args:
        start_date (str): Default starting date string.
        last_date (dlt.sources.incremental[pendulum.DateTime]): Last date loaded saved in dlt state.

    Returns:
        str: Formatted date range string for the API request.
    """
    # configure incremental loading. start_date prio: last dlt load -> set start time -> 2000-01-01
    if last_date.last_value:
        if start_date:
            logger.warning(
                f"Using the starting date: {last_date.last_value} for the query and ignoring start date passed as argument {start_date}!"
            )
        # take next day after yesterday to avoid double loads
        start_date = last_date.last_value.add(days=1).to_date_string()
    else:
        start_date = start_date or DEFAULT_START_DATE
    # configure end_date to yesterday as a date string and format date_range with starting and end dates
    end_date = pendulum.yesterday().to_date_string()
    date_range = f"{start_date},{end_date}"
    return date_range
