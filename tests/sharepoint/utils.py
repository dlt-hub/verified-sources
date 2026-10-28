"""Graph response builders, requests_mock helpers and load result helpers for the SharePoint tests."""

import io
import os
from collections import Counter
from functools import partial
from typing import Any, Dict, Iterable, List, NamedTuple, Set, Tuple, Type, TypeVar
from urllib.parse import parse_qs, quote, urlparse

import dlt
import openpyxl  # type: ignore[import-untyped]
import pyarrow as pa
import pyarrow.parquet as pq
from requests_mock import Mocker

from sources.sharepoint import sharepoint_files
from sources.sharepoint.filesystem import SharepointFileSystem
from sources.sharepoint.graph import entra_auth, graph_client
from tests.utils import assert_load_info, load_tables_to_dicts

TENANT_ID = "test-tenant"
CLIENT_ID = "test-client"
CLIENT_SECRET = "test-client-secret-value"
SITE_ID = "contoso.sharepoint.com,site-guid,web-guid"
ACCESS_TOKEN = "test-access-token"
TOKEN_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
SITE_URL = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}"
DOWNLOAD_URL = "https://contoso.sharepoint.com/download"
LISTS_URL = f"{SITE_URL}/lists"
DRIVES_URL = f"{SITE_URL}/drives"
OLD = "2019-06-01T00:00:00Z"
NEW = "2024-01-01T00:00:00Z"
NEWER = "2024-02-01T00:00:00Z"
AADSTS_ERROR = "AADSTS7000215: Invalid client secret provided."
NOT_FOUND = {"error": {"code": "itemNotFound", "message": "Not found."}}

E = TypeVar("E", bound=BaseException)


class File(NamedTuple):
    """A file or a folder of the mocked document library."""

    item_id: str
    name: str
    modified: str = NEW
    content: bytes = b""
    folder: bool = False


def token_response(access_token: str, expires_in: int = 3600) -> Dict[str, Any]:
    """Returns the requests_mock keyword arguments of a successful token response."""
    return {
        "json": {
            "token_type": "Bearer",
            "access_token": access_token,
            "expires_in": expires_in,
        }
    }


def new_fs(library: str = None) -> SharepointFileSystem:
    auth = entra_auth(TENANT_ID, CLIENT_ID, CLIENT_SECRET)
    return SharepointFileSystem(graph_client(SITE_ID, auth), library)


def item_url(path: str, drive: str = "drive") -> str:
    """Returns the Graph URL of the item at `path`, each segment quoted."""
    if not path:
        return f"{SITE_URL}/{drive}/root"
    quoted = "/".join(quote(segment, safe="") for segment in path.split("/"))
    return f"{SITE_URL}/{drive}/root:/{quoted}:"


def graph_item(file: File) -> Dict[str, Any]:
    """Returns the file as Graph lists it among the children of its folder."""
    kind: Dict[str, Any] = (
        {"folder": {"childCount": 0}}
        if file.folder
        else {
            "file": {"mimeType": "application/octet-stream"},
            "size": len(file.content),
        }
    )
    return {
        "id": file.item_id,
        "name": file.name,
        "lastModifiedDateTime": file.modified,
        **kind,
    }


def mock_pages(graph: Mocker, url: str, pages: List[List[Dict[str, Any]]]) -> None:
    """Registers a Graph collection at `url`, each page linking to the next one."""
    for number, page in enumerate(pages):
        body: Dict[str, Any] = {"value": page}
        if number + 1 < len(pages):
            body["@odata.nextLink"] = f"{url}?$skiptoken={number + 1}"
        graph.get(f"{url}?$skiptoken={number}" if number else url, json=body)


def mock_folder(
    graph: Mocker, folder: str, pages: List[List[File]], drive: str = "drive"
) -> None:
    """Registers the folder, its children pages and the info and content of each file."""
    url = item_url(folder, drive)
    name = folder.rpartition("/")[2] or "root"
    graph.get(url, json=graph_item(File(f"folder-{name}", name, folder=True)))
    mock_pages(
        graph,
        f"{url}/children",
        [[graph_item(file) for file in page] for page in pages],
    )
    for file in (file for page in pages for file in page if not file.folder):
        mock_download(graph, folder, file, drive)


def mock_download(graph: Mocker, folder: str, file: File, drive: str = "drive") -> None:
    """Registers the file info and its /content as a redirect to a URL serving byte ranges."""
    path = f"{folder}/{file.name}" if folder else file.name
    graph.get(item_url(path, drive), json=graph_item(file))
    graph.get(
        f"{SITE_URL}/{drive}/items/{file.item_id}/content",
        status_code=302,
        headers={"Location": f"{DOWNLOAD_URL}/{file.item_id}?tempauth=TEMPAUTH"},
    )
    graph.get(
        f"{DOWNLOAD_URL}/{file.item_id}", content=partial(ranged_content, file.content)
    )


def ranged_content(content: bytes, request: Any, context: Any) -> bytes:
    """Serves the bytes the Range header asks for, as the download host does."""
    start, end = request.headers["Range"].removeprefix("bytes=").split("-")
    context.status_code = 206
    return content[int(start) : int(end) + 1]


def mock_files(graph: Mocker, contents: Dict[str, bytes], modified: str = NEW) -> None:
    """Registers the 'Reports' folder with one file per entry of `contents`, named by its key and id."""
    files = [File(name, name, modified, content) for name, content in contents.items()]
    mock_folder(graph, "Reports", [files])


def site_list(name: str, template: str) -> Dict[str, Any]:
    return {
        "id": f"list-{name.lower()}",
        "displayName": name,
        "webUrl": f"https://contoso.sharepoint.com/Lists/{name}",
        "list": {"template": template},
    }


SITE_LISTS = [
    [site_list("Documents", "documentLibrary"), site_list("Contacts", "contacts")],
    [site_list("Tasks", "genericList")],
]


def list_delta_url(title: str) -> str:
    """Returns the delta query URL of the list named `title`, the title quoted as a path segment."""
    return f"{LISTS_URL}/{quote(title, safe='')}/items/delta"


def mock_list(
    graph: Mocker, title: str, item_pages: List[List[Dict[str, Any]]], token: str = "t1"
) -> str:
    """Registers the delta pages of the list named `title`; returns the delta link."""
    return mock_delta(graph, list_delta_url(title), item_pages, token)


def lists_requests(graph: Mocker) -> List[Any]:
    """Returns the recorded requests of the site's list collection, not those of one list."""
    lists_path = urlparse(LISTS_URL).path
    return [r for r in graph.request_history if urlparse(r.url).path == lists_path]


def mock_delta(
    graph: Mocker, url: str, pages: List[List[Dict[str, Any]]], token: str
) -> str:
    """Registers a delta query at `url` whose last page carries the delta link; returns that link."""
    joiner = "&" if "?" in url else "?"
    delta_link = f"{url}{joiner}token={token}"
    for number, page in enumerate(pages):
        body: Dict[str, Any] = {"value": page}
        if number + 1 < len(pages):
            body["@odata.nextLink"] = f"{url}{joiner}$skiptoken={number + 1}"
        else:
            body["@odata.deltaLink"] = delta_link
        graph.get(f"{url}{joiner}$skiptoken={number}" if number else url, json=body)
    return delta_link


def list_item(item_id: str, title: str) -> Dict[str, Any]:
    """Returns a list item as Graph returns it with $expand=fields."""
    return {"id": item_id, "fields": {"@odata.etag": f'"{item_id},1"', "Title": title}}


def files_config(**fields: Any) -> Dict[str, Any]:
    """Returns the arguments loading csv files of 'Reports' into 'reports', updated with `fields`."""
    return {
        "folder_path": "Reports",
        "file_type": "csv",
        "table_name": "reports",
        **fields,
    }


def tasks_config(
    included_columns: List[str] = None, list_title: str = "Tasks"
) -> Dict[str, Any]:
    """Returns the arguments loading the list `list_title` into 'tasks'."""
    return {
        "list_title": list_title,
        "table_name": "tasks",
        "included_columns": included_columns,
    }


def new_pipeline(name: str) -> dlt.Pipeline:
    return dlt.pipeline(
        pipeline_name=name,
        destination="duckdb",
        dataset_name=f"{name}_data",
        dev_mode=True,
    )


def load_file(graph: Mocker, name: str, content: bytes, **fields: Any) -> dlt.Pipeline:
    """Loads one file of the 'Reports' folder into a new pipeline and returns the pipeline."""
    mock_files(graph, {name: content})
    pipeline = new_pipeline(f"sharepoint_{name.replace('.', '_')}")
    assert_load_info(pipeline.run(sharepoint_files(**files_config(**fields))))
    return pipeline


def parquet_bytes(table: pa.Table) -> bytes:
    content = io.BytesIO()
    pq.write_table(table, content)
    return content.getvalue()


def excel_bytes(sheets: Dict[str, List[List[Any]]]) -> bytes:
    """Returns an xlsx workbook with one sheet per entry of `sheets`, in order."""
    workbook = openpyxl.Workbook()
    workbook.remove(workbook.active)
    for title, rows in sheets.items():
        sheet = workbook.create_sheet(title)
        for row in rows:
            sheet.append(row)
    content = io.BytesIO()
    workbook.save(content)
    return content.getvalue()


def requests_to(graph: Mocker, fragment: str) -> List[Any]:
    """Returns the recorded requests whose URL contains `fragment`."""
    return [request for request in graph.request_history if fragment in request.url]


def downloaded_ids(graph: Mocker) -> List[str]:
    """Returns the ids of the files downloaded through /content, sorted, each once."""
    return sorted(
        {
            urlparse(request.url).path.split("/")[-2]
            for request in requests_to(graph, "/content")
        }
    )


def query_of(request: Any) -> Dict[str, List[str]]:
    return parse_qs(urlparse(request.url).query)


def cause_of_type(error: BaseException, error_type: Type[E]) -> E:
    """Returns the first exception of `error_type` in the chain of causes of `error`."""
    while not isinstance(error, error_type):
        assert error.__cause__ is not None, f"no {error_type.__name__} in the chain"
        error = error.__cause__
    return error


def loaded_rows(pipeline: dlt.Pipeline, table: str = "reports") -> List[Dict[str, Any]]:
    return load_tables_to_dicts(pipeline, table)[table]


def rows_per_file(pipeline: dlt.Pipeline) -> Dict[str, int]:
    """Returns the number of loaded rows of each file name in the 'reports' table."""
    return dict(Counter(row["_file_name"] for row in loaded_rows(pipeline)))


def loaded_columns(
    pipeline: dlt.Pipeline, sort_by: str, names: Iterable[str]
) -> Dict[str, Tuple[str, List[Any]]]:
    """Returns the data type and the values, in `sort_by` order, of each named column of 'reports'."""
    columns = pipeline.default_schema.get_table_columns("reports")
    rows = sorted(loaded_rows(pipeline), key=lambda row: row[sort_by])
    return {
        name: (columns[name]["data_type"], [row[name] for row in rows])
        for name in names
    }


def data_columns(pipeline: dlt.Pipeline) -> Set[str]:
    """Returns the names of the 'reports' columns read from the files."""
    columns = pipeline.default_schema.get_table_columns("reports")
    return {
        name
        for name in columns
        if name != "_deleted" and not name.startswith(("_dlt_", "_file_"))
    }


def files_containing(directory: str, data: bytes) -> List[str]:
    """Returns the paths of the files under `directory` whose content contains `data`."""
    found = []
    for root, _, names in os.walk(directory):
        for name in names:
            path = os.path.join(root, name)
            with open(path, "rb") as f:
                if data in f.read():
                    found.append(path)
    return found
