"""Live tests against the test site described in README.md."""

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import dlt
import pandas as pd
import pytest
from dlt.common.configuration import resolve_configuration
from dlt.sources import DltResource

from sources.sharepoint import SharepointCredentials, sharepoint_files, sharepoint_list
from sources.sharepoint.graph import site_client
from tests.sharepoint.seed import data
from tests.sharepoint.utils import (
    delete_item,
    ensure_folder,
    item_path,
    last_row_count,
    loaded_rows,
    upload,
)
from tests.utils import (
    ALL_DESTINATIONS,
    assert_load_info,
    load_table_counts,
    load_tables_to_dicts,
    skip_if_not_active,
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
    # a date-only column is midnight in the time zone of the site, Pacific, converted to UTC
    assert {row["due_date"] for row in rows} == {
        datetime(2024, 4, day, 7, tzinfo=timezone.utc) for day in (1, 8, 15, 30)
    } | {None}

    pipeline.run(tasks)
    assert pipeline.last_trace.last_normalize_info.row_counts.get("tasks", 0) == 0
    assert load_table_counts(pipeline, "tasks") == {"tasks": 5}


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_list_pages(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="sharepoint_list_pages",
        destination=destination_name,
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
    assert {row["_dlt_file_name"] for row in rows} == {
        "report_2024_01.csv",
        "report_2024_02.csv",
        "report_2024_03.csv",
    }
    assert next(row["amount"] for row in rows if row["customer"] == "Eve") is None

    pipeline.run(sharepoint_files(**config))
    assert pipeline.last_trace.last_normalize_info.row_counts.get("reports", 0) == 0
    assert load_table_counts(pipeline, "reports") == {"reports": 6}


def seeded_files(**kwargs: Any) -> DltResource:
    return sharepoint_files(
        data.INCREMENTAL_FOLDER,
        "csv",
        table_name="seeded",
        file_glob="inc_*.csv",
        **kwargs,
    )


def seeded_names(pipeline: dlt.Pipeline) -> Set[str]:
    return {row["_dlt_file_name"] for row in loaded_rows(pipeline, "seeded")}


@skip_if_not_active("duckdb")
def test_seeded_full_and_incremental() -> None:
    """Loads the seeded files, nothing on the next run, from `start_date` on a new pipeline and in windows."""
    seeded = data.incremental_files()
    modified = [when for _, _, when in seeded]
    names = {name for name, _, _ in seeded}

    pipeline = dlt.pipeline(
        pipeline_name="sharepoint_seeded", destination="duckdb", dev_mode=True
    )
    assert_load_info(pipeline.run(seeded_files()))
    assert {
        (row["_dlt_file_name"], row["_dlt_file_modified_at"])
        for row in loaded_rows(pipeline, "seeded")
    } == {(name, when) for name, _, when in seeded}
    files_state = pipeline.state["sources"]["sharepoint_files"]["resources"][
        "seeded_files"
    ]
    assert files_state["incremental"]["modification_date"]["last_value"] == max(
        modified
    )

    pipeline.run(seeded_files())
    assert last_row_count(pipeline, "seeded") == 0
    assert load_table_counts(pipeline, "seeded") == {"seeded": len(seeded)}

    start = sorted(modified)[len(modified) // 2]
    since = dlt.pipeline(
        pipeline_name="sharepoint_seeded_start", destination="duckdb", dev_mode=True
    )
    assert_load_info(since.run(seeded_files(start_date=start)))
    assert seeded_names(since) == {name for name, _, when in seeded if when >= start}

    # bounds are seeded values, so a value on a bound loads in exactly one window
    values = sorted(modified)
    quarters = [values[len(values) * k // 4] for k in (1, 2, 3)]
    # with `min` the windows go back in time, the last one ends before the first seeded value
    for last_value_func, bounds in (
        (max, [values[0], *quarters, values[-1] + timedelta(seconds=1)]),
        (min, [values[-1], *quarters[::-1], values[0] - timedelta(seconds=1)]),
    ):
        backfill = dlt.pipeline(
            pipeline_name=f"sharepoint_seeded_backfill_{last_value_func.__name__}",
            destination="duckdb",
            dev_mode=True,
        )
        for start, end in zip(bounds, bounds[1:]):
            expected = sum(
                start <= when < end if last_value_func is max else end < when <= start
                for when in modified
            )
            assert expected > 0
            window = dlt.sources.incremental(
                "modification_date",
                initial_value=start,
                end_value=end,
                last_value_func=last_value_func,
                range_start="closed",
                range_end="open",
            )
            assert_load_info(backfill.run(seeded_files(incremental=window)))
            assert last_row_count(backfill, "seeded") == expected
        # windows with an end value do not store incremental state
        assert "sources" not in backfill.state
        assert seeded_names(backfill) == names


def graph_client() -> Any:
    credentials = resolve_configuration(
        SharepointCredentials(), sections=("sources", "sharepoint")
    )
    return site_client(**dict(credentials))


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_files_incremental_changes(destination_name: str) -> None:
    """Adds, updates and deletes files of the run between runs of the default incremental"""
    folder = f"dlt_changes/{uuid.uuid4().hex}"
    base = datetime(2022, 1, 1, tzinfo=timezone.utc)
    client = graph_client()
    item_ids: Dict[str, str] = {}

    def write(name: str, value: int, minute: int) -> None:
        content = f"name,value\n{name},{value}\n".encode()
        modified = base + timedelta(minutes=minute)
        item_ids[name] = upload(client, f"{folder}/{name}.csv", content, modified)

    def run() -> int:
        files = sharepoint_files(folder, "csv", table_name="changes", file_glob="*.csv")
        # a run without changes loads no package
        pipeline.run(files).raise_on_failed_jobs()
        return last_row_count(pipeline, "changes")

    def loaded() -> List[Tuple[str, int]]:
        rows = loaded_rows(pipeline, "changes")
        return sorted((row["name"], int(row["value"])) for row in rows)

    pipeline = dlt.pipeline(
        pipeline_name="sharepoint_changes",
        destination=destination_name,
        dev_mode=True,
    )
    ensure_folder(client, folder)
    try:
        for minute, name in enumerate(("a1", "a2", "a3"), 1):
            write(name, 1, minute)
        assert run() == 3

        # the file with the last value, a3, is read again and dropped as a duplicate
        write("a1", 2, 4)
        delete_item(client, item_ids.pop("a2"))
        write("b1", 1, 5)
        assert run() == 2
        # the listing does not show deleted files, the row of a2 stays
        expected = [("a1", 2), ("a2", 1), ("a3", 1), ("b1", 1)]
        assert loaded() == expected

        assert run() == 0
        assert loaded() == expected
    finally:
        delete_item(client, client.get(item_path(folder)).json()["id"])


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_list_incremental_changes(destination_name: str) -> None:
    """Adds, updates and deletes items of a list made for the run between delta syncs"""
    title = f"dlt changes {uuid.uuid4().hex}"
    client = graph_client()
    created = client.post(
        "lists",
        json={
            "displayName": title,
            "columns": [{"name": "Value", "number": {}}],
            "list": {"template": "genericList"},
        },
    )
    created.raise_for_status()
    items_path = f"lists/{created.json()['id']}/items"
    item_ids: Dict[str, str] = {}

    def add(name: str) -> None:
        response = client.post(items_path, json={"fields": {"Title": name, "Value": 1}})
        response.raise_for_status()
        item_ids[name] = response.json()["id"]

    def run() -> int:
        items = sharepoint_list(title, included_columns=["Title", "Value"])
        items.apply_hints(columns={"_deleted": {"hard_delete": True}})
        # a run without changes loads no package
        pipeline.run(items.with_name("changes")).raise_on_failed_jobs()
        return last_row_count(pipeline, "changes")

    def loaded() -> List[Tuple[str, int]]:
        rows = loaded_rows(pipeline, "changes")
        return sorted((row["title"], int(row["value"])) for row in rows)

    pipeline = dlt.pipeline(
        pipeline_name="sharepoint_list_changes",
        destination=destination_name,
        dev_mode=True,
    )
    try:
        for name in ("a1", "a2", "a3"):
            add(name)
        assert run() == 3

        client.request(
            f"{items_path}/{item_ids['a1']}/fields", method="PATCH", json={"Value": 2}
        ).raise_for_status()
        client.request(
            f"{items_path}/{item_ids['a2']}", method="DELETE"
        ).raise_for_status()
        add("b1")
        # a1, the delete of a2 and b1
        assert run() == 3
        expected = [("a1", 2), ("a3", 1), ("b1", 1)]
        assert loaded() == expected

        assert run() == 0
        assert loaded() == expected
    finally:
        client.request(
            f"lists/{created.json()['id']}", method="DELETE"
        ).raise_for_status()


@pytest.mark.parametrize(
    "config, table, row_count, column, values",
    [
        (
            {"folder_path": "dlt test", "file_glob": "**/report_*.csv"},
            "t",
            7,
            "customer",
            {"Old", "Alice", "Bob", "Carla", "Dan", "Eve", "Gus"},
        ),
        (
            {"folder_path": "", "file_glob": "root_*.csv", "library": "Documents"},
            "t",
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
            "t_data",
            3,
            "code__v_text",
            {None, "A-7"},
        ),
        ({"folder_path": "dlt_paging"}, "t", 205, None, None),
        (
            {"folder_path": "archive", "library": data.ARCHIVE_LIBRARY},
            "t",
            1,
            "customer",
            {"Archived"},
        ),
    ],
    ids=[
        "subfolders",
        "library_root",
        "excel_in_folder_with_special_chars",
        "folder_with_205_files",
        "other_library",
    ],
)
@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_files_variants(
    destination_name: str,
    config: Dict[str, Any],
    table: str,
    row_count: int,
    column: Optional[str],
    values: Optional[Set[Any]],
) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="sharepoint_files_variants",
        destination=destination_name,
        dataset_name="sharepoint_data",
        dev_mode=True,
    )
    source = sharepoint_files(**{"file_type": "csv", "table_name": "t", **config})
    assert_load_info(pipeline.run(source))
    rows = load_tables_to_dicts(pipeline, table)[table]
    assert len(rows) == row_count
    if column:
        assert {row.get(column) for row in rows} == values


@skip_if_not_active("duckdb")
def test_read_library_url_with_pandas() -> None:
    """pandas reads the library named in the url, the default library has no such file."""
    credentials = resolve_configuration(
        SharepointCredentials(), sections=("sources", "sharepoint")
    )
    storage_options = dict(credentials)
    url = f"sharepoint://{data.ARCHIVE_LIBRARY}/{data.ARCHIVE_FILE}"
    assert pd.read_csv(url, storage_options=storage_options)["customer"].tolist() == [
        "Archived"
    ]
    with pytest.raises(FileNotFoundError):
        pd.read_csv(
            f"sharepoint:///{data.ARCHIVE_FILE}", storage_options=storage_options
        )
