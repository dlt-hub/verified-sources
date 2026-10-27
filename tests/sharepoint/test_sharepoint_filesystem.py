"""Mocked tests of the SharePoint filesystem driver and the files source: Graph calls go to requests_mock."""

import codecs
import os
import re
from typing import Any, Dict, List, Tuple, Type
from urllib.parse import parse_qs, urlparse

import fsspec
import pandas as pd
import pyarrow as pa
import pytest
from dlt.common import Decimal, pendulum
from dlt.common.pendulum import timedelta
from dlt.extract.exceptions import ResourceExtractionError
from dlt.pipeline.exceptions import PipelineStepFailed
from dlt.sources.helpers.requests import HTTPError
from requests_mock import Mocker

from sources.sharepoint import sharepoint_files
from sources.sharepoint.filesystem import (
    ITEM_FIELDS,
    SharepointFileSystem,
    make_url,
    validate_folder_path,
)
from sources.sharepoint.graph import entra_auth
from tests.sharepoint.utils import (
    AADSTS_ERROR,
    ACCESS_TOKEN,
    CLIENT_ID,
    CLIENT_SECRET,
    DOWNLOAD_URL,
    DRIVES_URL,
    NEW,
    NEWER,
    NOT_FOUND,
    OLD,
    SITE_ID,
    TENANT_ID,
    TOKEN_URL,
    File,
    cause_of_type,
    credentials_env,
    data_columns,
    downloaded_ids,
    excel_bytes,
    files_config,
    files_containing,
    graph,
    load_file,
    loaded_columns,
    loaded_rows,
    mock_files,
    mock_folder,
    mock_pages,
    new_fs,
    new_pipeline,
    parquet_bytes,
    query_of,
    requests_to,
    rows_per_file,
    token_response,
)
from tests.utils import assert_load_info, load_table_counts

SITE_PATH = urlparse(f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}").path
DRIVE_PAGES = [[{"id": "d1", "name": "Documents"}], [{"id": "d2", "name": "Archive"}]]
SPENT = timedelta(hours=1, minutes=30)
QUOTED_FOLDER = "R&D (2024)/Client's #1+Q2 100%/Données"
QUOTED_PATH = "root:/R%26D%20%282024%29/Client%27s%20%231%2BQ2%20100%25/Donn%C3%A9es:"


def test_ls_pages_children_into_entries(graph: Mocker) -> None:
    first_page = [
        File("A", "a.csv", NEW, b"12345"),
        File("F", "2023", OLD, folder=True),
    ]
    mock_folder(graph, "Reports", [first_page, [File("B", "b c.csv", NEWER, b"1")]])
    fs = new_fs()

    entries = fs.ls("sharepoint:///Reports/", detail=True)

    assert [
        tuple(entry[key] for key in ("name", "size", "type", "modified", "id"))
        for entry in entries
    ] == [
        ("Reports/a.csv", 5, "file", pendulum.datetime(2024, 1, 1), "A"),
        ("Reports/2023", 0, "directory", pendulum.datetime(2019, 6, 1), "F"),
        ("Reports/b c.csv", 1, "file", pendulum.datetime(2024, 2, 1), "B"),
    ]
    assert fs.ls("Reports") == ["Reports/a.csv", "Reports/2023", "Reports/b c.csv"]
    children_requests = requests_to(graph, "/children")
    assert len(children_requests) == 4
    assert query_of(children_requests[0]) == {"$select": [ITEM_FIELDS]}
    assert query_of(children_requests[1]) == {"$skiptoken": ["1"]}


@pytest.mark.parametrize(
    "path,expected_path",
    [
        pytest.param("", "drive/root/children", id="root_empty"),
        pytest.param("sharepoint://", "drive/root/children", id="root_protocol"),
        pytest.param(
            f"{QUOTED_FOLDER}/", f"drive/{QUOTED_PATH}/children", id="quoted_segments"
        ),
    ],
)
def test_item_path_quoted_per_segment(
    graph: Mocker, path: str, expected_path: str
) -> None:
    graph.get(re.compile("/children"), json={"value": []})

    assert new_fs().ls(path) == []

    [request] = requests_to(graph, "/children")
    assert urlparse(request.url).path == f"{SITE_PATH}/{expected_path}"


@pytest.mark.parametrize(
    "library,drive",
    [
        pytest.param(None, "drive", id="default_library"),
        pytest.param("Archive", "drives/d2", id="named_library"),
    ],
)
def test_library_resolves_to_its_drive_once(
    graph: Mocker, library: str, drive: str
) -> None:
    mock_pages(graph, DRIVES_URL, DRIVE_PAGES)
    mock_folder(graph, "", [[File("A", "a.csv")]], drive)
    fs = new_fs(library)

    assert fs.ls("") == fs.ls("") == ["a.csv"]

    paths = [urlparse(r.url).path for r in graph.request_history]
    assert paths.count(f"{SITE_PATH}/drives") == (2 if library else 0)
    assert paths.count(f"{SITE_PATH}/{drive}/root/children") == 2


def test_missing_library_lists_available_names(graph: Mocker) -> None:
    mock_pages(graph, DRIVES_URL, DRIVE_PAGES)

    with pytest.raises(ValueError) as exc_info:
        new_fs("Nope").ls("")

    assert (
        str(exc_info.value)
        == "Document library 'Nope' not found. Available libraries: ['Documents', 'Archive']"
    )


@pytest.mark.parametrize(
    "operation,kind",
    [
        pytest.param("ls", "Folder", id="ls"),
        pytest.param("info", "Folder or file", id="info"),
    ],
)
@pytest.mark.parametrize(
    "status_code,body,error,message",
    [
        pytest.param(
            404,
            {"json": NOT_FOUND},
            FileNotFoundError,
            "{kind} 'Reports' not found in the library",
            id="not_found_graph_error",
        ),
        pytest.param(
            404,
            {"text": "Not Found"},
            FileNotFoundError,
            "{kind} 'Reports' not found in the library",
            id="not_found_plain_text",
        ),
        pytest.param(
            403,
            {"json": {"error": {"code": "accessDenied", "message": "Access denied."}}},
            HTTPError,
            "403 Client Error",
            id="forbidden",
        ),
    ],
)
def test_ls_and_info_errors_map_the_status(
    graph: Mocker,
    operation: str,
    kind: str,
    status_code: int,
    body: Dict[str, Any],
    error: Type[Exception],
    message: str,
) -> None:
    graph.get(re.compile("/root:/Reports:"), status_code=status_code, **body)

    with pytest.raises(error) as exc_info:
        getattr(new_fs(), operation)("Reports")

    assert str(exc_info.value).startswith(message.format(kind=kind))


@pytest.mark.parametrize(
    "url,status_code,error,message",
    [
        pytest.param(
            "/root:/Reports:",
            404,
            FileNotFoundError,
            "Folder 'Reports' not found",
            id="missing_folder",
        ),
        pytest.param(
            "/root:/Reports/2023:/children",
            403,
            HTTPError,
            "403 Client Error",
            id="forbidden_subfolder",
        ),
    ],
)
def test_unreadable_folder_fails_the_source(
    graph: Mocker,
    url: str,
    status_code: int,
    error: Type[Exception],
    message: str,
) -> None:
    """A folder that cannot be listed fails the load instead of leaving its files out."""
    a = File("A", "a.csv", NEW, b"n\n1\n")
    mock_folder(graph, "Reports", [[a, File("F", "2023", folder=True)]])
    graph.get(re.compile(url), status_code=status_code, json=NOT_FOUND)

    with pytest.raises(ResourceExtractionError) as exc_info:
        list(sharepoint_files(files_config(file_glob="**/*.csv")))

    assert str(cause_of_type(exc_info.value, error)).startswith(message)
    assert downloaded_ids(graph) == []


def test_root_folder_loads_files(graph: Mocker) -> None:
    mock_folder(graph, "", [[File("A", "a.csv", NEW, b"n\n1\n")]])

    rows = list(sharepoint_files(files_config(folder_path="")))

    assert [(row["n"], row["_file_url"]) for row in rows] == [
        (1, "sharepoint:///a.csv")
    ]


@pytest.mark.parametrize(
    "folder_path,expected",
    [
        pytest.param("", "", id="empty"),
        pytest.param("/", "", id="slash"),
        pytest.param("/Reports/2024/", "Reports/2024", id="surrounding_slashes"),
        pytest.param(QUOTED_FOLDER, QUOTED_FOLDER, id="allowed_punctuation"),
    ],
)
def test_validate_folder_path_strips_slashes(folder_path: str, expected: str) -> None:
    assert validate_folder_path(folder_path) == expected


@pytest.mark.parametrize(
    "fields,message",
    [
        pytest.param(
            {"folder_path": "a//b"}, "empty segment ('//'): 'a//b'", id="empty_segment"
        ),
        pytest.param(
            {"folder_path": 'a|\\?><:*"*b'},
            "['\"', '*', ':', '<', '>', '?', '\\\\', '|']",
            id="disallowed_characters",
        ),
        pytest.param(
            {"folder_path": "a\x85\x7f\n\tb"},
            "['\\t', '\\n', '\\x7f', '\\x85']",
            id="control_characters",
        ),
        pytest.param(
            {"folder_path": "Reports [2024]"},
            "cannot contain '[' or ']'",
            id="glob_characters",
        ),
        pytest.param(
            {"file_type": "xml"}, "file_type must be one of", id="unknown_file_type"
        ),
        pytest.param(
            {"file_type": "parquet", "reader_kwargs": {"chunksize": 2}},
            "may only contain 'columns'",
            id="parquet_extra_kwargs",
        ),
    ],
)
def test_invalid_config_fails_at_source_creation(
    requests_mock: Mocker, fields: Dict[str, Any], message: str
) -> None:
    with pytest.raises(ValueError, match=re.escape(message)):
        sharepoint_files(files_config(**fields))

    assert requests_mock.call_count == 0


def test_file_read_in_ranges_with_one_token(graph: Mocker) -> None:
    """The redirect target gets the Range header but not the bearer token."""
    content = bytes(range(10))
    mock_folder(graph, "Reports", [[File("A", "a.bin", NEW, content)]])
    fs = new_fs()

    with fs.open("sharepoint:///Reports/a.bin", block_size=4) as f:
        assert f.read(3) == content[:3]
        assert f.read() == content[3:]
    with fs.open("Reports/a.bin") as f:
        assert f.read(1024) == content

    ranges = ["bytes=0-6", "bytes=7-9", "bytes=0-9"]
    assert [r.headers["Range"] for r in requests_to(graph, "/content")] == ranges
    assert [r.headers["Range"] for r in requests_to(graph, DOWNLOAD_URL)] == ranges
    assert ["Authorization" in r.headers for r in requests_to(graph, DOWNLOAD_URL)] == [
        False
    ] * 3
    assert {r.headers["Authorization"] for r in requests_to(graph, SITE_PATH)} == {
        f"Bearer {ACCESS_TOKEN}"
    }
    [token_request] = requests_to(graph, TOKEN_URL)
    assert parse_qs(token_request.text) == {
        "client_id": [CLIENT_ID],
        "client_secret": [CLIENT_SECRET],
        "grant_type": ["client_credentials"],
        "scope": ["https://graph.microsoft.com/.default"],
    }


def test_hash_in_folder_and_file_names_kept_end_to_end(graph: Mocker) -> None:
    folder, path = "Reports/Q1 #2", "Reports/Q1 #2/Client #1.csv"
    mock_folder(graph, folder, [[File("A", "Client #1.csv", NEW, b"n\n1\n")]])
    fs = new_fs()

    assert fs.ls(f"sharepoint:///{folder}") == [path]
    assert fs.info(f"sharepoint:///{path}")["name"] == path
    with fs.open(f"sharepoint:///{path}") as f:
        assert f.read() == b"n\n1\n"
    assert (
        make_url("sharepoint", path, f"sharepoint:///{folder}")
        == f"sharepoint:///{path}"
    )
    rows = list(sharepoint_files(files_config(folder_path=folder)))
    assert [(row["n"], row["_file_url"]) for row in rows] == [
        (1, f"sharepoint:///{path}")
    ]
    assert {urlparse(r.url).path for r in requests_to(graph, "/root:/")} == {
        f"{SITE_PATH}/drive/root:/Reports/Q1%20%232:",
        f"{SITE_PATH}/drive/root:/Reports/Q1%20%232:/children",
        f"{SITE_PATH}/drive/root:/Reports/Q1%20%232/Client%20%231.csv:",
    }


def test_download_error_hides_the_download_url(graph: Mocker) -> None:
    mock_folder(graph, "Reports", [[File("A", "a.csv", NEW, b"n\n1\n")]])
    graph.get(f"{DOWNLOAD_URL}/A", status_code=403, text="denied")

    with pytest.raises(ResourceExtractionError) as exc_info:
        list(sharepoint_files(files_config()))

    error = cause_of_type(exc_info.value, HTTPError)
    assert str(error) == "403 error downloading drive/items/A/content"
    assert error.response.status_code == 403
    assert "tempauth" not in str(exc_info.value)


def test_token_refreshed_after_expiry(graph: Mocker) -> None:
    graph.post(
        TOKEN_URL, [token_response("first", expires_in=0), token_response("second")]
    )
    mock_folder(graph, "Reports", [[File("A", "a.csv")]])
    fs = new_fs()

    for _ in range(3):
        assert fs.ls("Reports") == ["Reports/a.csv"]

    methods = [r.method for r in graph.request_history]
    assert methods == ["POST", "GET", "POST", "GET", "GET"]
    tokens = [r.headers["Authorization"] for r in requests_to(graph, "/children")]
    assert tokens == ["Bearer first", "Bearer second", "Bearer second"]


@pytest.mark.parametrize(
    "expires_in,expected_seconds",
    [pytest.param(3600, 3540, id="an_hour"), pytest.param(30, 0, id="under_a_minute")],
)
def test_token_expires_a_minute_early(
    graph: Mocker, expires_in: int, expected_seconds: int
) -> None:
    graph.post(TOKEN_URL, **token_response("t", expires_in=expires_in))
    auth = entra_auth(TENANT_ID, CLIENT_ID, CLIENT_SECRET)

    auth.obtain_token()

    assert (auth.token_expiry - pendulum.now()).total_seconds() == pytest.approx(
        expected_seconds, abs=5
    )


def test_token_error_keeps_entra_message(graph: Mocker) -> None:
    rejected = {"error": "invalid_client", "error_description": AADSTS_ERROR}
    graph.post(TOKEN_URL, status_code=401, json=rejected)
    mock_folder(graph, "Reports", [[File("A", "a.csv")]])

    with pytest.raises(HTTPError) as exc_info:
        new_fs().ls("Reports")

    assert TOKEN_URL in str(exc_info.value)
    assert f"invalid_client: {AADSTS_ERROR}" in str(exc_info.value)
    assert [r.method for r in graph.request_history] == ["POST"]


@pytest.mark.parametrize(
    "file_glob,file_urls,listed_folders",
    [
        pytest.param(
            "*",
            ["sharepoint:///Reports/a.csv", "sharepoint:///Reports/b.txt"],
            1,
            id="all_files_of_folder",
        ),
        pytest.param("*.csv", ["sharepoint:///Reports/a.csv"], 1, id="csv_of_folder"),
        pytest.param(
            "**/*.csv",
            ["sharepoint:///Reports/2023/c.csv", "sharepoint:///Reports/a.csv"],
            2,
            id="csv_recursive",
        ),
    ],
)
def test_file_glob_filters_listing(
    graph: Mocker, file_glob: str, file_urls: List[str], listed_folders: int
) -> None:
    a, b = File("A", "a.csv", NEW, b"n\n1\n"), File("B", "b.txt", NEW, b"n\n2\n")
    mock_folder(graph, "Reports", [[a, b, File("F", "2023", folder=True)]])
    mock_folder(graph, "Reports/2023", [[File("C", "c.csv", NEW, b"n\n3\n")]])

    rows = list(sharepoint_files(files_config(file_glob=file_glob)))

    assert sorted(row["_file_url"] for row in rows) == file_urls
    assert len(requests_to(graph, "/children")) == listed_folders


def test_files_load_incrementally(graph: Mocker) -> None:
    """Loads new and modified files only, merges a modified file over its own rows and reloads everything on refresh."""
    a = File("A", "report_a.csv", OLD, b"n,_file_name\n1,X\n2,X\n3,X\n")
    b = File("B", "report_b.csv", NEW, b"n\n4\n5\n")
    blank = File("C", "blank.csv", NEW, b"\n")
    mock_folder(graph, "Reports", [[a, b, blank]])
    pipeline = new_pipeline("sharepoint_incremental")

    assert_load_info(pipeline.run(sharepoint_files(files_config())))

    assert downloaded_ids(graph) == ["A", "B", "C"]
    assert rows_per_file(pipeline) == {"report_a.csv": 3, "report_b.csv": 2}
    assert {
        (r["_file_url"], r["_file_modified_at"]) for r in loaded_rows(pipeline)
    } == {
        ("sharepoint:///Reports/report_a.csv", pendulum.datetime(2019, 6, 1)),
        ("sharepoint:///Reports/report_b.csv", pendulum.datetime(2024, 1, 1)),
    }
    assert [t["name"] for t in pipeline.default_schema.data_tables()] == ["reports"]
    table = pipeline.default_schema.tables["reports"]
    assert table["write_disposition"] == "merge"
    assert table["columns"]["_file_url"]["merge_key"] is True
    lineage = ("_file_url", "_file_name", "_file_modified_at")
    assert [table["columns"][name]["data_type"] for name in lineage] == [
        "text",
        "text",
        "timestamp",
    ]
    secret_traces = [
        t
        for t in pipeline.last_trace.resolved_config_values
        if t.key == "client_secret"
    ]
    assert {(t.value, t.is_secret_hint) for t in secret_traces} == {(None, True)}
    assert os.path.isfile(os.path.join(pipeline.working_dir, "trace.pickle"))
    assert files_containing(pipeline.working_dir, CLIENT_SECRET.encode()) == []

    graph.reset_mock()
    unchanged = pipeline.run(sharepoint_files(files_config()))

    assert downloaded_ids(graph) == []
    assert unchanged.loads_ids == []

    graph.reset_mock()
    modified_b = File("B", "report_b.csv", NEWER, b"n\n7\n")
    d = File("D", "report_d.csv", NEW, b"n\n6\n")
    mock_folder(graph, "Reports", [[a, modified_b, blank, d]])
    assert_load_info(pipeline.run(sharepoint_files(files_config())))

    assert downloaded_ids(graph) == ["B", "D"]
    assert rows_per_file(pipeline) == {
        "report_a.csv": 3,
        "report_b.csv": 1,
        "report_d.csv": 1,
    }

    for refresh in ("drop_resources", "drop_data"):
        graph.reset_mock()
        assert_load_info(
            pipeline.run(sharepoint_files(files_config()), refresh=refresh)
        )

        assert downloaded_ids(graph) == ["A", "B", "C", "D"]
        assert rows_per_file(pipeline) == {
            "report_a.csv": 3,
            "report_b.csv": 1,
            "report_d.csv": 1,
        }


@pytest.mark.parametrize(
    "name,content,fields,sort_by,expected",
    [
        pytest.param(
            "report.csv",
            b"row,k,due\n1,1,2024-01-01\n2,2,\n3,A,2024-01-03\n4,,\n",
            {"reader_kwargs": {"chunksize": 2, "parse_dates": ["due"]}},
            "row",
            {
                "k": ("bigint", [1, 2, None, None]),
                "k__v_text": ("text", [None, None, "A", None]),
                "due": (
                    "timestamp",
                    [
                        pendulum.datetime(2024, 1, 1),
                        None,
                        pendulum.datetime(2024, 1, 3),
                        None,
                    ],
                ),
            },
            id="csv_chunks_with_changing_types",
        ),
        pytest.param(
            "report.xlsx",
            excel_bytes(
                {
                    "Other": [["junk"], ["x"]],
                    "Data": [
                        ["id", "code", "due", "spent", "spent_or_text", 2024],
                        [1, 101, pendulum.date(2024, 3, 1), SPENT, SPENT, SPENT],
                        [2, "A-7", None, None, "pending", SPENT],
                        [3, 103.5, pendulum.date(2024, 3, 3), SPENT, SPENT, SPENT],
                    ],
                }
            ),
            {"file_type": "excel", "reader_kwargs": {"sheet_name": "Data"}},
            "id",
            {
                "code": ("bigint", [101, None, None]),
                "code__v_text": ("text", [None, "A-7", None]),
                "code__v_double": ("double", [None, None, 103.5]),
                "due": (
                    "timestamp",
                    [
                        pendulum.datetime(2024, 3, 1),
                        None,
                        pendulum.datetime(2024, 3, 3),
                    ],
                ),
                "spent": ("double", [5400.0, None, 5400.0]),
                "spent_or_text": ("double", [5400.0, None, 5400.0]),
                "spent_or_text__v_text": ("text", [None, "pending", None]),
                "_2024": ("double", [5400.0] * 3),
            },
            id="excel_sheet_with_mixed_types",
        ),
        pytest.param(
            "report.jsonl",
            codecs.BOM_UTF8
            + b'{"n": 1, "a": 1, "obj": {"x": 1}, "seen_at": 1700000000}\n'
            b'{"n": 2, "a": 2, "obj": {"x": 2}, "seen_at": 1700000060}\n'
            b'{"n": 3, "a": "text", "obj": {"x": 3}, "seen_at": 1700000120}\n',
            {"file_type": "jsonl"},
            "n",
            {
                "a": ("bigint", [1, 2, None]),
                "a__v_text": ("text", [None, None, "text"]),
                "obj": ("json", ['{"x":1}', '{"x":2}', '{"x":3}']),
                "seen_at": ("bigint", [1700000000 + 60 * i for i in range(3)]),
            },
            id="jsonl_with_nested_object",
        ),
    ],
)
def test_file_columns_load_with_their_types(
    graph: Mocker,
    name: str,
    content: bytes,
    fields: Dict[str, Any],
    sort_by: str,
    expected: Dict[str, Tuple[str, List[Any]]],
) -> None:
    pipeline = load_file(graph, name, content, **fields)

    assert loaded_columns(pipeline, sort_by, expected) == expected
    assert data_columns(pipeline) == {sort_by, *expected}


def test_parquet_types_and_selected_columns(graph: Mocker) -> None:
    table = pa.table(
        {
            "n": [1, 2],
            "tags": pa.array([["a", "b"], []], type=pa.list_(pa.string())),
            "amount": pa.array([Decimal("12.34"), None], type=pa.decimal128(10, 2)),
            "seen_at": pa.array(
                [pendulum.datetime(2024, 1, 1), None], type=pa.timestamp("us", tz="UTC")
            ),
            "in_s": pa.array([5400, None], pa.duration("s")),
            "in_ms": pa.array([5_400_000, None], pa.duration("ms")),
            "in_ns": pa.array([5_400_000_000_000, None], pa.duration("ns")),
            "_file_name": ["X", "X"],
            "skipped": ["x", "y"],
        }
    )
    columns = {"columns": table.column_names[:-1]}

    pipeline = load_file(
        graph,
        "report.parquet",
        parquet_bytes(table),
        file_type="parquet",
        reader_kwargs=columns,
    )

    expected = {
        "tags": ("json", ['["a","b"]', "[]"]),
        "amount": ("decimal", [Decimal("12.34"), None]),
        "seen_at": ("timestamp", [pendulum.datetime(2024, 1, 1), None]),
        "in_s": ("double", [5400.0, None]),
        "in_ms": ("double", [5400.0, None]),
        "in_ns": ("double", [5400.0, None]),
        "_file_name": ("text", ["report.parquet"] * 2),
        "_file_url": ("text", ["sharepoint:///Reports/report.parquet"] * 2),
        "_file_modified_at": ("timestamp", [pendulum.datetime(2024, 1, 1)] * 2),
    }
    assert loaded_columns(pipeline, "n", expected) == expected
    assert "skipped" not in data_columns(pipeline)


def test_parquet_files_with_other_column_types_load_as_variants(graph: Mocker) -> None:
    first = pd.DataFrame({"value": [5, 0, 6], "label": [None] * 3}).iloc[[0, 2]]
    second = pd.DataFrame({"id": [1, 2], "value": [7, None], "label": ["ok", None]})
    a = File("A", "a.parquet", OLD, first.to_parquet())
    b = File("B", "b.parquet", NEW, second.set_index("id").to_parquet())
    mock_folder(graph, "Reports", [[a, b]])
    pipeline = new_pipeline("sharepoint_parquet_types")

    assert_load_info(pipeline.run(sharepoint_files(files_config(file_type="parquet"))))

    expected = {
        "id": ("bigint", [None, None, 1, 2]),
        "value": ("bigint", [5, 6, 7, None]),
        "label": ("text", [None, None, "ok", None]),
    }
    assert loaded_columns(pipeline, "_file_name", expected) == expected
    assert data_columns(pipeline) == set(expected)

    c = File("C", "c.parquet", NEWER, pd.DataFrame({"value": [1]}).to_parquet())
    d = File("D", "d.parquet", NEWER, pd.DataFrame({"value": [7.5]}).to_parquet())
    mock_folder(graph, "Reports", [[c, d]])
    assert_load_info(pipeline.run(sharepoint_files(files_config(file_type="parquet"))))

    assert data_columns(pipeline) == {*expected, "value__v_double"}
    assert rows_per_file(pipeline) == {
        "a.parquet": 2,
        "b.parquet": 2,
        "c.parquet": 1,
        "d.parquet": 1,
    }


@pytest.mark.parametrize(
    "file_type,content",
    [
        pytest.param("csv", b"", id="empty_csv"),
        pytest.param("csv", b"\n", id="newline_csv"),
        pytest.param("csv", b"\r\n", id="crlf_csv"),
        pytest.param("csv", codecs.BOM_UTF8, id="bom_only_csv"),
        pytest.param("csv", b"\n" * 2000, id="whitespace_over_1kib_csv"),
        pytest.param("csv", b"n\n", id="header_only_csv"),
        pytest.param("jsonl", b"", id="empty_jsonl"),
        pytest.param("jsonl", b" \n", id="blank_jsonl"),
        pytest.param("excel", b"", id="empty_excel"),
        pytest.param("excel", excel_bytes({"Data": [["id"]]}), id="header_only_excel"),
        pytest.param("parquet", b"", id="empty_parquet"),
    ],
)
def test_file_without_rows_yields_a_deletion_marker(
    graph: Mocker, file_type: str, content: bytes
) -> None:
    mock_files(graph, {"blank": content})

    assert list(sharepoint_files(files_config(file_type=file_type))) == [
        {
            "_file_url": "sharepoint:///Reports/blank",
            "_file_name": "blank",
            "_file_modified_at": pendulum.datetime(2024, 1, 1),
            "_deleted": True,
        }
    ]


def test_emptied_file_loses_its_rows(graph: Mocker) -> None:
    a = File("A", "a.csv", NEW, b"n\n1\n2\n")
    b = File("B", "b.csv", NEW, b"n\n3\n")
    mock_folder(graph, "Reports", [[a, b]])
    pipeline = new_pipeline("sharepoint_emptied")
    assert_load_info(pipeline.run(sharepoint_files(files_config())))
    assert rows_per_file(pipeline) == {"a.csv": 2, "b.csv": 1}

    graph.reset_mock()
    mock_folder(graph, "Reports", [[File("A", "a.csv", NEWER, b"n\n"), b]])
    assert_load_info(pipeline.run(sharepoint_files(files_config())))

    assert downloaded_ids(graph) == ["A"]
    assert rows_per_file(pipeline) == {"b.csv": 1}
    columns = pipeline.default_schema.tables["reports"]["columns"]
    assert columns["_deleted"]["hard_delete"] is True


def test_emptied_sheet_loses_its_rows(graph: Mocker) -> None:
    book = excel_bytes({"Data": [["n"], [1], [2]], "Other": [["n"], [3]]})
    mock_folder(graph, "Reports", [[File("A", "book.xlsx", NEW, book)]])
    config = files_config(file_type="excel", table_name="book")
    pipeline = new_pipeline("sharepoint_emptied_sheet")
    assert_load_info(pipeline.run(sharepoint_files(config)))
    assert load_table_counts(pipeline, "book_data", "book_other") == {
        "book_data": 2,
        "book_other": 1,
    }

    graph.reset_mock()
    book = excel_bytes({"Data": [["n"]], "Other": [["n"], [3]]})
    mock_folder(graph, "Reports", [[File("A", "book.xlsx", NEWER, book)]])
    assert_load_info(pipeline.run(sharepoint_files(config)))

    assert load_table_counts(pipeline, "book_data", "book_other") == {
        "book_data": 0,
        "book_other": 1,
    }


def test_filesystem_needs_a_client_or_all_credentials() -> None:
    with pytest.raises(ValueError, match="Pass a Graph client, or tenant_id"):
        SharepointFileSystem(tenant_id=TENANT_ID, client_id=CLIENT_ID)


def test_files_config_resolves_from_env_without_arguments(
    graph: Mocker, monkeypatch: pytest.MonkeyPatch
) -> None:
    for name, value in {
        "FOLDER_PATH": "/Reports/",
        "FILE_TYPE": "csv",
        "TABLE_NAME": "reports",
        "LIBRARY": "Archive",
        "FILE_GLOB": "*.csv",
        "READER_KWARGS": '{"sep": ";"}',
    }.items():
        monkeypatch.setenv(
            f"SOURCES__SHAREPOINT__SHAREPOINT_FILES_CONFIG__{name}", value
        )
    mock_pages(graph, DRIVES_URL, DRIVE_PAGES)
    a, b = File("A", "a.csv", NEW, b"a;b\n1;2\n"), File(
        "B", "b.txt", NEW, b"a;b\n3;4\n"
    )
    mock_folder(graph, "Reports", [[a, b]], "drives/d2")

    rows = list(sharepoint_files())

    assert [(row["a"], row["b"], row["_file_url"]) for row in rows] == [
        (1, 2, "sharepoint://Archive/Reports/a.csv")
    ]


def test_workbook_loads_one_table_per_sheet(graph: Mocker) -> None:
    content = excel_bytes(
        {"Data": [["id", "v"], [1, "a"]], "Empty": [], "Sales 2024": [["id"], [2]]}
    )
    pipeline = load_file(
        graph, "book.xlsx", content, file_type="excel", table_name="book"
    )

    tables = {t["name"]: t for t in pipeline.default_schema.data_tables()}
    assert set(tables) == {"book_data", "book_empty", "book_sales_2024"}
    assert tables["book_sales_2024"]["write_disposition"] == "merge"
    assert tables["book_sales_2024"]["columns"]["_file_url"]["merge_key"] is True
    assert tables["book_sales_2024"]["columns"]["_deleted"]["hard_delete"] is True
    assert load_table_counts(
        pipeline, "book_data", "book_empty", "book_sales_2024"
    ) == {
        "book_data": 1,
        "book_empty": 0,
        "book_sales_2024": 1,
    }


def test_filesystem_opens_through_fsspec_storage_options(graph: Mocker) -> None:
    mock_folder(graph, "Reports", [[File("A", "a.csv", NEW, b"n\n1\n")]])

    fs = fsspec.filesystem(
        "sharepoint",
        tenant_id=TENANT_ID,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        site_id=SITE_ID,
    )

    assert fs.ls("sharepoint:///Reports") == ["Reports/a.csv"]
    with fs.open("sharepoint:///Reports/a.csv") as f:
        assert f.read() == b"n\n1\n"


@pytest.mark.parametrize(
    "library,url",
    [
        pytest.param(None, "sharepoint://Archive/Reports/a.csv", id="named_on_default"),
        pytest.param("Documents", "sharepoint:///Reports/a.csv", id="default_on_named"),
    ],
)
def test_url_of_another_library_is_refused(
    graph: Mocker, library: str, url: str
) -> None:
    with pytest.raises(ValueError) as exc_info:
        new_fs(library).open(url)

    assert str(exc_info.value).startswith(f"'{url}' is not in library")
