"""Mocked tests of the SharePoint list sources: Graph calls go to requests_mock, loads go to duckdb."""

import re
from functools import partial
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse

import dlt
import pytest
from dlt.extract.exceptions import ResourceExtractionError
from dlt.pipeline.exceptions import PipelineStepFailed
from dlt.sources import DltResource
from dlt.sources.helpers.requests import HTTPError
from requests_mock import Mocker

from sources.sharepoint import LIST_PAGE_SIZE, sharepoint_list, sharepoint_lists
from tests.sharepoint.utils import (
    ACCESS_TOKEN,
    LISTS_URL,
    NOT_FOUND,
    SITE_LISTS,
    SITE_URL,
    cause_of_type,
    list_delta_url,
    list_item,
    lists_requests,
    mock_delta,
    mock_list,
    mock_pages,
    new_pipeline,
    query_of,
    requests_to,
    site_list,
)
from tests.utils import assert_load_info, load_table_counts, load_tables_to_dicts

TASKS_DELTA_URL = list_delta_url("Tasks")
FULL_RELOAD_HINT = 'Run the pipeline with refresh="drop_resources"'
NO_LISTS_MESSAGE = (
    "No lists to load: list_titles is empty or the site has no visible generic list."
)


REMOVED = {"id": "1", "deleted": {"state": "deleted"}, "fields": {}}


def delta_query(expand: str) -> Dict[str, List[str]]:
    """Returns the query of the first delta request that selects `expand`."""
    return {"$expand": [expand], "$top": [str(LIST_PAGE_SIZE)]}


def task_rows(pipeline: dlt.Pipeline) -> Set[Tuple[Any, ...]]:
    """Returns the loaded tasks as (id, title, _deleted)."""
    rows = load_tables_to_dicts(pipeline, "tasks")["tasks"]
    return {(row["id"], row.get("title"), row.get("_deleted")) for row in rows}


@pytest.mark.parametrize(
    "list_title,included_columns,expand",
    [
        pytest.param("Tasks", None, "fields", id="generic_list_on_second_page"),
        pytest.param(
            "Documents",
            ["Title", "Due_x0020_Date"],
            "fields($select=Due_x0020_Date,Title)",
            id="document_library_with_columns",
        ),
        pytest.param("R&D tasks", None, "fields", id="title_with_space_and_ampersand"),
    ],
)
def test_sharepoint_list_pagination(
    graph: Mocker, list_title: str, included_columns: List[str], expand: str
) -> None:
    inner_id = {"id": "3", "fields": {"id": "inner", "Title": "c"}}
    mock_list(
        graph, list_title, [[list_item("1", "a")], [list_item("2", "b"), inner_id]]
    )

    resource = sharepoint_list(list_title, included_columns)
    rows = list(resource)

    assert resource.name == list_title
    assert rows == [
        {"Title": "a", "id": "1", "_dlt_list_idx": 0},
        {"Title": "b", "id": "2", "_dlt_list_idx": 1},
        {"Title": "c", "id": "3", "_dlt_list_idx": 2},
    ]
    delta_requests = requests_to(graph, "/items/delta")
    assert len(delta_requests) == 2
    assert (
        urlparse(delta_requests[0].url).path
        == urlparse(list_delta_url(list_title)).path
    )
    assert query_of(delta_requests[0]) == delta_query(expand)
    assert lists_requests(graph) == []
    assert {r.headers["Authorization"] for r in requests_to(graph, SITE_URL)} == {
        f"Bearer {ACCESS_TOKEN}"
    }


def test_sharepoint_list_system_fields(graph: Mocker) -> None:
    """Every field Graph returns is loaded, only the @odata annotations are dropped."""
    item = list_item("1", "a")
    item["fields"].update(
        {
            "Status": "Open",
            "Attachments": False,
            "LinkTitle": "a",
            "Edit": "",
            "_UIVersionString": "1.0",
            "_ComplianceFlags": "",
        }
    )
    mock_list(graph, "Tasks", [[item]])

    rows = list(sharepoint_list("Tasks"))

    assert set(rows[0]) == {
        "Title",
        "Status",
        "Attachments",
        "LinkTitle",
        "Edit",
        "_UIVersionString",
        "_ComplianceFlags",
        "id",
        "_dlt_list_idx",
    }


def test_sharepoint_list_with_name(graph: Mocker) -> None:
    mock_list(graph, "Tasks", [[list_item("1", "a")]])
    pipeline = new_pipeline("sharepoint_list")

    assert_load_info(pipeline.run(sharepoint_list("Tasks").with_name("task_status")))

    assert [t["name"] for t in pipeline.default_schema.data_tables()] == ["task_status"]
    assert load_table_counts(pipeline, "task_status") == {"task_status": 1}


def test_sharepoint_list_graph_error(graph: Mocker) -> None:
    graph.get(TASKS_DELTA_URL, status_code=400, json=NOT_FOUND)

    with pytest.raises(ResourceExtractionError) as exc_info:
        list(sharepoint_list("Tasks"))

    assert str(cause_of_type(exc_info.value, HTTPError)).startswith("400 Client Error")
    assert lists_requests(graph) == []


def test_sharepoint_list_delta_sync(graph: Mocker) -> None:
    """Loads all items first, then only the changes from the stored delta link, deletions included, and the whole list again on refresh."""
    nested = {"id": "3", "fields": {"Tags": ["x"], "Owner": {"LookupId": 7}}}
    pipeline = new_pipeline("sharepoint_list")
    delta_link = mock_list(
        graph, "Tasks", [[list_item("1", "a"), list_item("2", "b"), nested]]
    )
    assert_load_info(pipeline.run(sharepoint_list("Tasks")))
    assert load_table_counts(pipeline, "tasks", "tasks__tags") == {
        "tasks": 3,
        "tasks__tags": 1,
    }
    assert lists_requests(graph) == []
    assert [t["name"] for t in pipeline.default_schema.data_tables()] == [
        "tasks",
        "tasks__tags",
    ]
    table = pipeline.default_schema.tables["tasks"]
    assert table["write_disposition"] == "merge"
    assert table["columns"]["id"]["primary_key"] is True
    assert table["columns"]["_dlt_list_idx"]["dedup_sort"] == "desc"
    assert table["columns"]["owner__lookup_id"]["data_type"] == "bigint"
    assert "tags" not in table["columns"]

    graph.reset_mock()
    removed = {"id": "3", "deleted": {"state": "deleted"}, "fields": {}}
    next_link = mock_delta(graph, delta_link, [[list_item("2", "b2"), removed]], "t2")
    assert_load_info(pipeline.run(sharepoint_list("Tasks")))
    assert [r.url for r in requests_to(graph, TASKS_DELTA_URL)] == [delta_link]
    assert task_rows(pipeline) == {
        ("1", "a", None),
        ("2", "b2", None),
        ("3", None, True),
    }
    assert load_table_counts(pipeline, "tasks__tags") == {"tasks__tags": 0}

    graph.reset_mock()
    mock_delta(graph, next_link, [[]], "t3")
    pipeline.run(sharepoint_list("Tasks"))
    assert [r.url for r in requests_to(graph, TASKS_DELTA_URL)] == [next_link]
    assert load_table_counts(pipeline, "tasks", "tasks__tags") == {
        "tasks": 3,
        "tasks__tags": 0,
    }

    graph.reset_mock()
    mock_list(graph, "Tasks", [[list_item("1", "a")]])
    assert_load_info(pipeline.run(sharepoint_list("Tasks"), refresh="drop_resources"))
    assert [r.url.split("?")[0] for r in requests_to(graph, TASKS_DELTA_URL)] == [
        TASKS_DELTA_URL
    ]
    assert load_table_counts(pipeline, "tasks") == {"tasks": 1}


def test_sharepoint_list_continues_after_several_pages(graph: Mocker) -> None:
    """The next run continues from the delta link of the last page of a multi-page read."""
    pipeline = new_pipeline("sharepoint_list")
    delta_link = mock_list(
        graph, "Tasks", [[list_item("1", "a")], [list_item("2", "b")]]
    )
    assert_load_info(pipeline.run(sharepoint_list("Tasks")))
    assert len(requests_to(graph, TASKS_DELTA_URL)) == 2

    graph.reset_mock()
    mock_delta(graph, delta_link, [[list_item("3", "c")]], "t2")
    assert_load_info(pipeline.run(sharepoint_list("Tasks")))

    assert [r.url for r in requests_to(graph, TASKS_DELTA_URL)] == [delta_link]
    assert load_table_counts(pipeline, "tasks") == {"tasks": 3}


@pytest.mark.parametrize(
    "last_occurrence,expected",
    [
        pytest.param(
            list_item("1", "a2"), {("1", "a2", None), ("2", "b", None)}, id="update"
        ),
        pytest.param(REMOVED, {("1", None, True), ("2", "b", None)}, id="delete"),
    ],
)
def test_sharepoint_list_last_occurrence_wins(
    graph: Mocker, last_occurrence: Dict[str, Any], expected: Set[Tuple[Any, ...]]
) -> None:
    """Graph may send an item more than once in a feed; its last occurrence is what loads."""
    pages = [[list_item("1", "a"), list_item("2", "b")], [last_occurrence]]
    mock_list(graph, "Tasks", pages)
    pipeline = new_pipeline("sharepoint_list")

    assert_load_info(pipeline.run(sharepoint_list("Tasks")))

    assert task_rows(pipeline) == expected


def hard_deleting_list() -> DltResource:
    return sharepoint_list("Tasks").apply_hints(
        columns={"_deleted": {"hard_delete": True}}
    )


@pytest.mark.parametrize(
    "tasks,hard_delete",
    [
        pytest.param(partial(sharepoint_lists, ["Tasks"]), False, id="soft_by_default"),
        pytest.param(
            partial(sharepoint_lists, ["Tasks"], hard_delete=True),
            True,
            id="lists_argument",
        ),
        pytest.param(hard_deleting_list, True, id="list_hint"),
    ],
)
def test_sharepoint_list_hard_delete(
    graph: Mocker, tasks: Callable[[], Any], hard_delete: bool
) -> None:
    """A deleted item keeps its row with `_deleted` set, unless `_deleted` has the hard_delete hint."""
    pipeline = new_pipeline("sharepoint_list")
    delta_link = mock_list(graph, "Tasks", [[list_item("1", "a"), list_item("2", "b")]])
    assert_load_info(pipeline.run(tasks()))
    mock_delta(graph, delta_link, [[{**REMOVED, "id": "2"}]], "t2")

    assert_load_info(pipeline.run(tasks()))

    kept = {("1", "a", None)}
    assert task_rows(pipeline) == (kept if hard_delete else kept | {("2", None, True)})
    columns = pipeline.default_schema.tables["tasks"]["columns"]
    assert columns["_deleted"].get("hard_delete", False) is hard_delete


def test_sharepoint_list_expired_delta_link(graph: Mocker) -> None:
    pipeline = new_pipeline("sharepoint_list")
    delta_link = mock_list(graph, "Tasks", [[list_item("1", "a")]])
    assert_load_info(pipeline.run(sharepoint_list("Tasks")))
    body = {"code": "resyncChangesApplyDifferences", "message": "Resync required."}
    graph.get(delta_link, status_code=410, json={"error": body})

    with pytest.raises(PipelineStepFailed) as exc_info:
        pipeline.run(sharepoint_list("Tasks"))

    assert str(cause_of_type(exc_info.value, ValueError)) == (
        "The stored delta link of list 'Tasks' is no longer valid (Graph 410)."
        f" {FULL_RELOAD_HINT} to read the whole list again."
    )
    assert lists_requests(graph) == []


@pytest.mark.parametrize(
    "list_titles,expected_tables",
    [
        pytest.param(None, ["tasks"], id="visible_generic_lists_by_default"),
        pytest.param(["Tasks", "Contacts"], ["tasks", "contacts"], id="named_lists"),
    ],
)
def test_sharepoint_lists_tables(
    graph: Mocker, list_titles: Optional[List[str]], expected_tables: List[str]
) -> None:
    hidden = site_list("Hidden", "genericList")
    hidden["list"]["hidden"] = True
    mock_pages(graph, LISTS_URL, SITE_LISTS + [[hidden]])
    for title in ("Tasks", "Contacts"):
        mock_list(graph, title, [[list_item("1", "a")]])
    pipeline = new_pipeline("sharepoint_lists")

    source = sharepoint_lists(list_titles)
    assert_load_info(pipeline.run(source))

    assert sorted(t["name"] for t in pipeline.default_schema.data_tables()) == sorted(
        expected_tables
    )
    assert load_table_counts(pipeline, *expected_tables) == {
        t: 1 for t in expected_tables
    }
    assert bool(lists_requests(graph)) is (list_titles is None)


@pytest.mark.parametrize(
    "list_titles,site_lists",
    [
        pytest.param([], SITE_LISTS, id="no_titles"),
        pytest.param(None, SITE_LISTS[:1], id="no_visible_generic_list"),
    ],
)
def test_sharepoint_lists_creation_errors(
    graph: Mocker,
    list_titles: Optional[List[str]],
    site_lists: List[List[Dict[str, Any]]],
) -> None:
    mock_pages(graph, LISTS_URL, site_lists)

    with pytest.raises(ValueError, match=re.escape(NO_LISTS_MESSAGE)):
        sharepoint_lists(list_titles)

    assert requests_to(graph, "/items/delta") == []


@pytest.mark.parametrize(
    "section",
    [
        pytest.param("SOURCES__SHAREPOINT__TASKS", id="resource_section"),
        pytest.param("SOURCES__SHAREPOINT", id="source_section"),
    ],
)
def test_sharepoint_list_config_from_env(
    graph: Mocker, monkeypatch: pytest.MonkeyPatch, section: str
) -> None:
    monkeypatch.setenv(f"{section}__INCLUDED_COLUMNS", '["Title"]')
    mock_list(graph, "Tasks", [[list_item("1", "a")]])

    assert list(sharepoint_list("Tasks")) == [
        {"Title": "a", "id": "1", "_dlt_list_idx": 0}
    ]
    assert query_of(requests_to(graph, "/items/delta")[0]) == delta_query(
        "fields($select=Title)"
    )


def test_sharepoint_list_columns_changed(graph: Mocker) -> None:
    """Other columns start a full sync of the same table, which merges into the earlier rows."""
    pipeline = new_pipeline("sharepoint_list")
    mock_list(graph, "Tasks", [[list_item("1", "a")]])
    assert_load_info(pipeline.run(sharepoint_list("Tasks", ["Title"])))
    mock_list(graph, "Tasks", [[list_item("2", "b")]], "t2")

    assert_load_info(pipeline.run(sharepoint_list("Tasks", ["Title", "Status"])))

    assert load_table_counts(pipeline, "tasks") == {"tasks": 2}
    first, second = requests_to(graph, "/items/delta")
    assert query_of(first) == delta_query("fields($select=Title)")
    assert query_of(second) == delta_query("fields($select=Status,Title)")


def test_sharepoint_list_column_order_ignored(graph: Mocker) -> None:
    pipeline = new_pipeline("sharepoint_list")
    delta_link = mock_list(graph, "Tasks", [[list_item("1", "a")]])
    assert_load_info(pipeline.run(sharepoint_list("Tasks", ["Title", "Status"])))
    mock_delta(graph, delta_link, [[list_item("2", "b")]], "t2")

    assert_load_info(pipeline.run(sharepoint_list("Tasks", ["Status", "Title"])))

    assert load_table_counts(pipeline, "tasks") == {"tasks": 2}
    first, second = requests_to(graph, "/items/delta")
    assert query_of(first) == delta_query("fields($select=Status,Title)")
    assert second.url == delta_link
