import time
import dlt
from zendesk import DateTime, zendesk_chat, zendesk_talk, zendesk_support


def load_all():
    """
    Loads all possible tables for Zendesk Support, Chat, Talk
    """
    # FULL PIPELINE RUN
    pipeline = dlt.pipeline(pipeline_name="dlt_zendesk_pipeline", destination="postgres", full_refresh=False, dataset_name="sample_zendesk_data3")

    # zendesk support source function
    data_support = zendesk_support(load_all=True)
    # zendesk chat source function
    data_chat = zendesk_chat()
    # zendesk talk source function
    data_talk = zendesk_talk()
    # run pipeline with all 3 sources
    info = pipeline.run(data=[data_support, data_chat, data_talk])
    return info


def ticket_pivot_fields():
    """
    Loads Zendesk Support data with pivoting. Simply done by setting the pivot_ticket_fields to true - default option. Loads only the base tables.
    """
    pipeline = dlt.pipeline(pipeline_name="zendesk_support_pivoting", destination="postgres", full_refresh=False)
    data = zendesk_support(load_all=False, pivot_ticket_fields=True)
    info = pipeline.run(data=data)
    return info


def incremental_pipeline():
    """
    Implements incremental load when possible to Support, Chat and Talk Endpoints. The default behaviour gets data since the last load time saved in dlt state or
    1st Jan 2000 if there has been no previous loading of the resource. With this setting, the sources will load data since the given data for all incremental endpoints.
    Last load time will still be updated.
    """

    # Choosing starting point for incremental load - optional, the default is the last load time. If no last load time
    # the start time will be the 1st day of the millennium
    # start time needs to be a pendulum datetime object
    start_time = DateTime(year=2023, month=1, day=1)

    pipeline = dlt.pipeline(pipeline_name="dlt_zendesk_pipeline", destination="postgres", full_refresh=False, dataset_name="sample_zendesk_data")
    data = zendesk_support(load_all=True, incremental_start_time=start_time)
    data_chat = zendesk_chat(incremental_start_time=start_time)
    data_talk = zendesk_talk(incremental_start_time=start_time)
    info = pipeline.run(data=[data, data_chat, data_talk])
    return info


if __name__ == "__main__":
    # simple run where everything is loaded
    start = time.time()
    load_info = load_all()
    end = time.time()
    print(load_info)
    print(f"Time taken: {end-start}")
