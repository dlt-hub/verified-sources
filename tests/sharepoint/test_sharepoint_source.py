"""Live tests against the test site described in README.md."""

from typing import Any, Optional, Set

import dlt
import pytest

from sources.sharepoint import (
    SharepointFilesConfig,
    SharepointListConfig,
    sharepoint_files,
    sharepoint_list,
)
from tests.utils import (
    ALL_DESTINATIONS,
    assert_load_info,
    load_table_counts,
    load_tables_to_dicts,
)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_list(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="sharepoint_list",
        destination=destination_name,
        dataset_name="sharepoint_data",
        dev_mode=True,
    )
    config = SharepointListConfig(
        list_title="dlt toy tasks",
        table_name="tasks",
        select="Title,Status,DueDate,StoryPoints",
    )
    assert_load_info(pipeline.run(sharepoint_list(config)))
    rows = load_tables_to_dicts(pipeline, "tasks")["tasks"]
    assert {row["title"] for row in rows} == {
        "Write spec",
        "Build source",
        "Review PR",
        "Live test",
        "Merge",
    }
    assert {row["story_points"] for row in rows} == {3, 8, 2, 5, None}

    assert pipeline.run(sharepoint_list(config)).loads_ids == []
    assert load_table_counts(pipeline, "tasks") == {"tasks": 5}


def test_list_pages() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="sharepoint_list_pages",
        destination="duckdb",
        dataset_name="sharepoint_data",
        dev_mode=True,
    )
    config = SharepointListConfig(list_title="dlt toy paging", table_name="paging")
    assert_load_info(pipeline.run(sharepoint_list(config)))
    rows = load_tables_to_dicts(pipeline, "paging")["paging"]
    assert len(rows) == 1000
    assert all(row["title"] is not None for row in rows)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_files(destination_name: str) -> None:
    """Loads the report files, then nothing on the unchanged second run."""
    pipeline = dlt.pipeline(
        pipeline_name="sharepoint_files",
        destination=destination_name,
        dataset_name="sharepoint_data",
        dev_mode=True,
    )
    config = SharepointFilesConfig(
        folder_path="dlt test",
        file_type="csv",
        table_name="reports",
        file_glob="report_*.csv",
    )
    assert_load_info(pipeline.run(sharepoint_files(config)))
    rows = load_tables_to_dicts(pipeline, "reports")["reports"]
    assert {row["customer"] for row in rows} == {
        "Alice",
        "Bob",
        "Carla",
        "Dan",
        "Eve",
        "Gus",
    }
    assert {row["_file_name"] for row in rows} == {
        "report_2024_01.csv",
        "report_2024_02.csv",
        "report_2024_03.csv",
    }
    assert next(row["amount"] for row in rows if row["customer"] == "Eve") is None

    assert pipeline.run(sharepoint_files(config)).loads_ids == []
    assert load_table_counts(pipeline, "reports") == {"reports": 6}


@pytest.mark.parametrize(
    "config, row_count, column, values",
    [
        (
            SharepointFilesConfig(
                folder_path="dlt test",
                file_type="csv",
                table_name="t",
                file_glob="**/report_*.csv",
            ),
            7,
            "customer",
            {"Old", "Alice", "Bob", "Carla", "Dan", "Eve", "Gus"},
        ),
        (
            SharepointFilesConfig(
                folder_path="",
                file_type="csv",
                table_name="t",
                file_glob="root_*.csv",
                library="Documents",
            ),
            2,
            "a",
            {1, 3},
        ),
        (
            SharepointFilesConfig(
                folder_path="dlt test/R&D (2024)",
                file_type="excel",
                table_name="t",
                reader_kwargs={"sheet_name": "Data"},
            ),
            3,
            "code__v_text",
            {None, "A-7"},
        ),
        (
            SharepointFilesConfig(
                folder_path="dlt_paging", file_type="csv", table_name="t"
            ),
            205,
            None,
            None,
        ),
    ],
    ids=[
        "subfolders",
        "library_root",
        "excel_in_folder_with_special_chars",
        "folder_with_205_files",
    ],
)
def test_files_variants(
    config: SharepointFilesConfig,
    row_count: int,
    column: Optional[str],
    values: Optional[Set[Any]],
) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="sharepoint_files_variants",
        destination="duckdb",
        dataset_name="sharepoint_data",
        dev_mode=True,
    )
    assert_load_info(pipeline.run(sharepoint_files(config)))
    rows = load_tables_to_dicts(pipeline, "t")["t"]
    assert len(rows) == row_count
    if column:
        assert {row.get(column) for row in rows} == values
