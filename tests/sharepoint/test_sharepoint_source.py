"""Live tests against the test site described in README.md."""

from typing import Any, Dict, Optional, Set

import dlt
import pytest

from sources.sharepoint import sharepoint_files, sharepoint_list
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
    tasks = sharepoint_list(
        "dlt toy tasks", included_columns=["Title", "Status", "DueDate", "StoryPoints"]
    ).with_name("tasks")
    assert_load_info(pipeline.run(tasks))
    rows = load_tables_to_dicts(pipeline, "tasks")["tasks"]
    assert {row["title"] for row in rows} == {
        "Write spec",
        "Build source",
        "Review PR",
        "Live test",
        "Merge",
    }
    assert {row["story_points"] for row in rows} == {3, 8, 2, 5, None}

    assert pipeline.run(tasks).loads_ids == []
    assert load_table_counts(pipeline, "tasks") == {"tasks": 5}


def test_list_pages() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="sharepoint_list_pages",
        destination="duckdb",
        dataset_name="sharepoint_data",
        dev_mode=True,
    )
    assert_load_info(
        pipeline.run(sharepoint_list("dlt toy paging").with_name("paging"))
    )
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
    config: Dict[str, Any] = {
        "folder_path": "dlt test",
        "file_type": "csv",
        "table_name": "reports",
        "file_glob": "report_*.csv",
    }
    assert_load_info(pipeline.run(sharepoint_files(**config)))
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

    assert pipeline.run(sharepoint_files(**config)).loads_ids == []
    assert load_table_counts(pipeline, "reports") == {"reports": 6}


@pytest.mark.parametrize(
    "config, row_count, column, values",
    [
        (
            {"folder_path": "dlt test", "file_glob": "**/report_*.csv"},
            7,
            "customer",
            {"Old", "Alice", "Bob", "Carla", "Dan", "Eve", "Gus"},
        ),
        (
            {"folder_path": "", "file_glob": "root_*.csv", "library": "Documents"},
            2,
            "a",
            {1, 3},
        ),
        (
            {
                "folder_path": "dlt test/R&D (2024)",
                "file_type": "excel",
                "reader_kwargs": {"sheet_name": "Data"},
            },
            3,
            "code__v_text",
            {None, "A-7"},
        ),
        ({"folder_path": "dlt_paging"}, 205, None, None),
    ],
    ids=[
        "subfolders",
        "library_root",
        "excel_in_folder_with_special_chars",
        "folder_with_205_files",
    ],
)
def test_files_variants(
    config: Dict[str, Any],
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
    source = sharepoint_files(**{"file_type": "csv", "table_name": "t", **config})
    assert_load_info(pipeline.run(source))
    rows = load_tables_to_dicts(pipeline, "t")["t"]
    assert len(rows) == row_count
    if column:
        assert {row.get(column) for row in rows} == values
