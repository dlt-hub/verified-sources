import dlt
from airtable import airtable_source


def load_entire_base(pipeline: dlt.Pipeline) -> None:
    # Loads all tables inside a given base.
    # Find the base ID starting with "app". See https://support.airtable.com/docs/finding-airtable-ids
    all_event_planning_tables = airtable_source(base_id="appctwIznRf5lqe62")
    load_info = pipeline.run(all_event_planning_tables, write_disposition="replace")
    print(load_info)


def load_select_tables_from_base_by_id(pipeline: dlt.Pipeline) -> None:
    # Loads specific table IDs.
    # Starts with "tbl". See https://support.airtable.com/docs/finding-airtable-ids
    # See example: https://airtable.com/appctwIznRf5lqe62/tblPjXnwd3V2RWgJS/
    airtables = airtable_source(
        base_id="appctwIznRf5lqe62",
        table_ids=["tblPjXnwd3V2RWgJS", "tbltdCacZQPxI7fV0"],
    )
    load_info = pipeline.run(airtables, write_disposition="replace")
    print(load_info)


def load_select_tables_from_base_by_name(pipeline: dlt.Pipeline) -> None:
    # Loads specific table names.
    # Filtering by names is less reliable than filtering on IDs because names can be changed by Airtable users.
    # See example: https://airtable.com/appctwIznRf5lqe62/tblOe4fjtZfnvqAHd/
    budget_table = airtable_source(
        base_id="appctwIznRf5lqe62",
        table_names=["💰 Budget"],
    )
    load_info = pipeline.run(budget_table, write_disposition="replace")
    print(load_info)


def load_table_for_ci(pipeline: dlt.Pipeline) -> None:
    # Setup for CI of dlt hub
    questionnaire_table = airtable_source(
        base_id="appcChDyP0pZeC76v", table_ids=["tbl1sN4CpPv8pBll4"]
    )
    load_info = pipeline.run(questionnaire_table, write_disposition="replace")
    print(load_info)


if __name__ == "__main__":
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name="airtable", destination="duckdb", dataset_name="airtable_data"
    )

    load_table_for_ci(pipeline)
    # load_select_tables_from_base_by_id(pipeline)
    # load_select_tables_from_base_by_name(pipeline)
    # load_entire_base(pipeline)
