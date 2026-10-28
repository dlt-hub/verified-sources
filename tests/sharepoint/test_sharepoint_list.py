"""Mocked tests of the SharePoint list sources: Graph calls go to requests_mock, loads go to duckdb."""

from typing import List, Optional
from urllib.parse import urlparse

import pytest
from dlt.extract.exceptions import ResourceExtractionError
from dlt.pipeline.exceptions import PipelineStepFailed
from dlt.sources.helpers.requests import HTTPError
from requests_mock import Mocker

from sources.sharepoint import sharepoint_list, sharepoint_lists
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
    tasks_config,
)
from tests.utils import assert_load_info, load_table_counts, load_tables_to_dicts

TASKS_DELTA_URL = list_delta_url("Tasks")
FULL_RELOAD_HINT = 'Run the pipeline with refresh="drop_resources"'


@pytest.mark.parametrize(
    "list_title,included_columns,expand",
    [
        pytest.param("Tasks", None, "fields", id="generic_list_on_second_page"),
        pytest.param(
            "Documents",
            ["Title", "Due_x0020_Date"],
            "fields($select=Title,Due_x0020_Date)",
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

    rows = list(sharepoint_list(**tasks_config(included_columns, list_title)))

    assert rows == [
        {"Title": "a", "id": "1"},
        {"Title": "b", "id": "2"},
        {"Title": "c", "id": "3"},
    ]
    delta_requests = requests_to(graph, "/items/delta")
    assert len(delta_requests) == 2
    assert (
        urlparse(delta_requests[0].url).path
        == urlparse(list_delta_url(list_title)).path
    )
    assert query_of(delta_requests[0]) == {"$expand": [expand]}
    assert lists_requests(graph) == []
    assert {r.headers["Authorization"] for r in requests_to(graph, SITE_URL)} == {
        f"Bearer {ACCESS_TOKEN}"
    }


@pytest.mark.parametrize(
    "included_columns,expected_keys",
    [
        pytest.param(
            None, {"Title", "Status", "Attachments", "id"}, id="default_columns_cleaned"
        ),
        pytest.param(
            ["Title", "LinkTitle", "_UIVersionString"],
            {
                "Title",
                "Status",
                "Attachments",
                "LinkTitle",
                "Edit",
                "_UIVersionString",
                "_ComplianceFlags",
                "id",
            },
            id="included_columns_kept",
        ),
    ],
)
def test_sharepoint_list_system_fields(
    graph: Mocker, included_columns: List[str], expected_keys: set
) -> None:
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

    rows = list(sharepoint_list(**tasks_config(included_columns)))

    assert set(rows[0]) == expected_keys


def test_sharepoint_list_default_table_name(graph: Mocker) -> None:
    mock_list(graph, "Tasks", [[list_item("1", "a")]])
    source = sharepoint_list("Tasks")
    assert list(source.resources) == ["Tasks"]


def test_sharepoint_list_not_found(graph: Mocker) -> None:
    graph.get(list_delta_url("Nope"), status_code=404, json=NOT_FOUND)
    mock_pages(graph, LISTS_URL, SITE_LISTS)

    with pytest.raises(ResourceExtractionError) as exc_info:
        list(sharepoint_list("Nope", table_name="t"))

    assert (
        str(cause_of_type(exc_info.value, ValueError))
        == "List 'Nope' not found in the site. Available lists: ['Documents', 'Contacts', 'Tasks']"
    )


@pytest.mark.parametrize(
    "failing_url",
    [
        pytest.param(TASKS_DELTA_URL, id="items"),
        pytest.param(LISTS_URL, id="lists_looked_up_for_a_missing_list"),
    ],
)
def test_sharepoint_list_graph_errors(graph: Mocker, failing_url: str) -> None:
    graph.get(TASKS_DELTA_URL, status_code=404, json=NOT_FOUND)
    graph.get(
        failing_url,
        status_code=400,
        json={"error": {"code": "invalidRequest", "message": "Bad request."}},
    )

    with pytest.raises(ResourceExtractionError) as exc_info:
        list(sharepoint_list(**tasks_config()))

    assert str(cause_of_type(exc_info.value, HTTPError)).startswith("400 Client Error")


def test_sharepoint_list_delta_sync(graph: Mocker) -> None:
    """Loads all items first, then only the changes from the stored delta link, deletions included."""
    nested = {"id": "3", "fields": {"Tags": ["x"], "Owner": {"LookupId": 7}}}
    pipeline = new_pipeline("sharepoint_list")
    delta_link = mock_list(
        graph, "Tasks", [[list_item("1", "a"), list_item("2", "b"), nested]]
    )
    assert_load_info(pipeline.run(sharepoint_list(**tasks_config())))
    assert load_table_counts(pipeline, "tasks") == {"tasks": 3}
    assert lists_requests(graph) == []
    assert [t["name"] for t in pipeline.default_schema.data_tables()] == ["tasks"]
    table = pipeline.default_schema.tables["tasks"]
    assert table["write_disposition"] == "merge"
    assert table["columns"]["id"]["primary_key"] is True
    assert table["columns"]["_deleted"]["hard_delete"] is True
    assert [table["columns"][name]["data_type"] for name in ("tags", "owner")] == [
        "json",
        "json",
    ]

    graph.reset_mock()
    removed = {"id": "3", "deleted": {"state": "deleted"}, "fields": {}}
    next_link = mock_delta(graph, delta_link, [[list_item("2", "b2"), removed]], "t2")
    assert_load_info(pipeline.run(sharepoint_list(**tasks_config())))
    assert [r.url for r in requests_to(graph, TASKS_DELTA_URL)] == [delta_link]
    assert {
        (r["id"], r["title"]) for r in load_tables_to_dicts(pipeline, "tasks")["tasks"]
    } == {
        ("1", "a"),
        ("2", "b2"),
    }

    graph.reset_mock()
    mock_delta(graph, next_link, [[]], "t3")
    pipeline.run(sharepoint_list(**tasks_config()))
    assert load_table_counts(pipeline, "tasks") == {"tasks": 2}

    graph.reset_mock()
    mock_list(graph, "Tasks", [[list_item("1", "a")]])
    assert_load_info(
        pipeline.run(sharepoint_list(**tasks_config()), refresh="drop_resources")
    )
    assert [r.url.split("?")[0] for r in requests_to(graph, TASKS_DELTA_URL)] == [
        TASKS_DELTA_URL
    ]
    assert load_table_counts(pipeline, "tasks") == {"tasks": 1}


@pytest.mark.parametrize(
    "status_code,body",
    [
        pytest.param(410, {"code": "resyncRequired", "message": "Gone."}, id="gone"),
        pytest.param(404, NOT_FOUND["error"], id="not_found"),
    ],
)
def test_sharepoint_list_expired_delta_link(
    graph: Mocker, status_code: int, body: dict
) -> None:
    pipeline = new_pipeline("sharepoint_list")
    delta_link = mock_list(graph, "Tasks", [[list_item("1", "a")]])
    assert_load_info(pipeline.run(sharepoint_list(**tasks_config())))
    graph.get(delta_link, status_code=status_code, json={"error": body})

    with pytest.raises(PipelineStepFailed) as exc_info:
        pipeline.run(sharepoint_list(**tasks_config()))

    assert str(cause_of_type(exc_info.value, ValueError)) == (
        f"The stored delta link of list 'Tasks' is no longer valid (Graph {status_code})."
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
    graph: Mocker, list_titles: list, expected_tables: list
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
    "list_titles,site_lists,message",
    [
        pytest.param(
            [], SITE_LISTS, "sharepoint_lists has no list to load", id="no_titles"
        ),
        pytest.param(
            None,
            SITE_LISTS[:1],
            "sharepoint_lists has no list to load",
            id="no_visible_generic_list",
        ),
        pytest.param(
            ["My List", "my_list"],
            SITE_LISTS,
            "Lists 'My List' and 'my_list' would both load into table 'my_list'",
            id="same_table_name",
        ),
    ],
)
def test_sharepoint_lists_creation_errors(
    graph: Mocker, list_titles: Optional[List[str]], site_lists: list, message: str
) -> None:
    mock_pages(graph, LISTS_URL, site_lists)

    with pytest.raises(ValueError, match=message):
        sharepoint_lists(list_titles)

    assert requests_to(graph, "/items/delta") == []


def test_sharepoint_list_config_from_env(
    graph: Mocker, monkeypatch: pytest.MonkeyPatch
) -> None:
    for name, value in {
        "LIST_TITLE": "Tasks",
        "TABLE_NAME": "tasks",
        "INCLUDED_COLUMNS": '["Title"]',
    }.items():
        monkeypatch.setenv(f"SOURCES__SHAREPOINT__SHAREPOINT_LIST__{name}", value)
    mock_list(graph, "Tasks", [[list_item("1", "a")]])

    assert list(sharepoint_list()) == [{"Title": "a", "id": "1"}]
    assert query_of(requests_to(graph, "/items/delta")[0]) == {
        "$expand": ["fields($select=Title)"]
    }


@pytest.mark.parametrize(
    "changed",
    [
        pytest.param({"list_title": "Contacts"}, id="other_list"),
        pytest.param({"included_columns": ["Title"]}, id="other_columns"),
    ],
)
def test_sharepoint_list_settings_changed(graph: Mocker, changed: dict) -> None:
    pipeline = new_pipeline("sharepoint_list")
    mock_list(graph, "Tasks", [[list_item("1", "a")]])
    assert_load_info(pipeline.run(sharepoint_list(**tasks_config())))
    mock_list(graph, "Contacts", [[list_item("1", "a")]])

    with pytest.raises(PipelineStepFailed) as exc_info:
        pipeline.run(sharepoint_list(**tasks_config(**changed)))

    assert str(cause_of_type(exc_info.value, ValueError)) == (
        "Table 'tasks' was loaded with another list or columns."
        f" {FULL_RELOAD_HINT} to load it again."
    )
    assert_load_info(
        pipeline.run(
            sharepoint_list(**tasks_config(**changed)), refresh="drop_resources"
        )
    )
