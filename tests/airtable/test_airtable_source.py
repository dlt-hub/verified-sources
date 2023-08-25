from tests.utils import (
    ALL_DESTINATIONS,
    assert_load_info,
    load_table_counts,
    assert_query_data,
)
import pytest
import dlt
from dlt.common.utils import uniq_id
from sources.airtable import airtable_source


def make_pipeline(destination_name: str) -> dlt.Pipeline:
    return dlt.pipeline(
        pipeline_name="airtable_test",
        destination=destination_name,
        dataset_name="airtable_test_data" + uniq_id(),
    )


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_table_by_id(destination_name: str) -> None:
    pipeline = make_pipeline(destination_name)
    questionnaire_table = airtable_source(
        base_id="appcChDyP0pZeC76v", table_ids=["tbl1sN4CpPv8pBll4"]
    )
    run_single_table_assertions(pipeline, questionnaire_table)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_table_by_name(destination_name: str) -> None:
    pipeline = make_pipeline(destination_name)
    questionnaire_table = airtable_source(
        base_id="appcChDyP0pZeC76v", table_names=["Sheet1"]
    )
    run_single_table_assertions(pipeline, questionnaire_table)


def run_single_table_assertions(pipeline, questionnaire_table):
    row_count: int = 22
    airtable_name: str = "Sheet1"

    load_info = pipeline.run(questionnaire_table, write_disposition="replace")

    assert_load_info(load_info)

    loaded_table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    assert loaded_table_names == [airtable_name.lower()]

    counts_of_rows = load_table_counts(pipeline, *loaded_table_names)
    assert counts_of_rows[airtable_name.lower()] == row_count

    query_string = (
        "select fields__question_1 from sheet1 where fields__name = 'Tina Quinn'"
    )
    expected_table_data = ["Maybe"]
    assert_query_data(pipeline, query_string, expected_table_data)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_all_tables_in_base(destination_name: str) -> None:
    all_event_planning_tables = airtable_source(base_id="appctwIznRf5lqe62")
    pipeline = make_pipeline(destination_name)
    load_info = pipeline.run(all_event_planning_tables, write_disposition="replace")
    assert_load_info(load_info)

    loaded_table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    expected_table_names = [
        "_attendees",
        "_budget",
        "_schedule",
        "_speakers",
        "_schedule__fields__speaker_sx",
        "_speakers__fields__speaking_at",
    ]
    assert set(loaded_table_names) == set(expected_table_names)

    counts_of_rows = load_table_counts(pipeline, *loaded_table_names)
    assert counts_of_rows == {
        "_attendees": 11,
        "_budget": 7,
        "_schedule": 19,
        "_speakers": 9,
        "_speakers__fields__speaking_at": 16,
        "_schedule__fields__speaker_sx": 16,
    }
