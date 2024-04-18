import time
from typing import Any

import dlt
from dlt.common.time import timedelta
from zendesk import (
    pendulum,
    zendesk_chat,
    zendesk_talk,
    zendesk_support,
    make_date_ranges,
)


def incremental_load_all_default() -> Any:
    """
    Loads all possible tables for Zendesk Support, Chat, Talk
    """
    # FULL PIPELINE RUN
    pipeline = dlt.pipeline(
        pipeline_name="dlt_zendesk_pipeline",
        destination="postgres",
        full_refresh=False,
        dataset_name="sample_zendesk_data3",
    )

    # zendesk support source function
    data_support = zendesk_support(load_all=True)
    # zendesk chat source function
    data_chat = zendesk_chat()
    # zendesk talk source function
    data_talk = zendesk_talk()
    # run pipeline with all 3 sources
    info = pipeline.run(data=[data_support, data_chat, data_talk])
    return info


def load_support_with_pivoting() -> Any:
    """
    Loads Zendesk Support data with pivoting. Simply done by setting the pivot_ticket_fields to true - default option. Loads only the base tables.
    """
    pipeline = dlt.pipeline(
        pipeline_name="zendesk_support_pivoting",
        destination="postgres",
        full_refresh=False,
    )
    data = zendesk_support(load_all=False, pivot_ticket_fields=True)
    info = pipeline.run(data=data)
    return info


def incremental_load_all_start_date() -> Any:
    """
    Implements incremental load when possible to Support, Chat and Talk Endpoints. The default behaviour gets data since the last load time saved in dlt state or
    1st Jan 2000 if there has been no previous loading of the resource. With this setting, the sources will load data since the given data for all incremental endpoints.
    Last load time will still be updated.
    """

    # Choosing starting point for incremental load - optional, the default is the last load time. If no last load time
    # the start time will be the 1st day of the millennium
    # start time needs to be a pendulum datetime object
    start_date = pendulum.DateTime(year=2023, month=1, day=1).in_timezone("UTC")

    pipeline = dlt.pipeline(
        pipeline_name="dlt_zendesk_pipeline",
        destination="postgres",
        full_refresh=False,
        dataset_name="sample_zendesk_data",
    )
    data = zendesk_support(load_all=True, start_date=start_date)
    data_chat = zendesk_chat(start_date=start_date)
    data_talk = zendesk_talk(start_date=start_date)
    info = pipeline.run(data=[data, data_chat, data_talk])
    return info


def incremental_load_with_backloading() -> Any:
    """Backload historic data in ranges. In this method we load all tickets so far created since Jan 1st 2023 but one week at a time
    and then switch to incrementally loading new tickets.
    This can useful to reduce the potiential failure window when loading large amounts of historic data.
    This approach can be used with all incremental Zendesk sources.
    """
    pipeline = dlt.pipeline(
        pipeline_name="dlt_zendesk_pipeline",
        destination="postgres",
        full_refresh=False,
        dataset_name="sample_zendesk_data",
    )

    # Load ranges of dates to load between January 1st 2023 and today
    min_start_date = pendulum.DateTime(year=2023, month=1, day=1).in_timezone("UTC")
    max_end_date = pendulum.today()
    # Generate tuples of date ranges, each with 1 week in between.
    ranges = make_date_ranges(min_start_date, max_end_date, timedelta(weeks=1))

    # Run the pipeline in a loop for each 1 week range
    for start, end in ranges:
        print(f"Loading tickets between {start} and {end}")
        data = zendesk_support(start_date=start, end_date=end).with_resources("tickets")
        info = pipeline.run(data=data)
        print(info)

    # Backloading is done, now we continue loading with incremental state, starting where the backloading left off
    print(f"Loading with incremental state, starting at {end}")
    data = zendesk_support(start_date=end).with_resources("tickets")
    info = pipeline.run(data)
    print(info)


if __name__ == "__main__":
    # simple run where everything is loaded
    start = time.time()
    load_info = incremental_load_all_default()
    end = time.time()
    print(load_info)
    print(f"Time taken: {end-start}")
