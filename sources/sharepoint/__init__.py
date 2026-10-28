"""Loads SharePoint lists and the files of document libraries through Microsoft Graph.

`sharepoint_lists` and `sharepoint_list` sync list items with Graph's delta query, `sharepoint_files`
reads the rows of csv, jsonl, parquet and excel files. API reference:
https://learn.microsoft.com/graph/api/resources/sharepoint
"""

from typing import Any, Dict, Iterable, Iterator, List, Optional
from urllib.parse import quote

import dlt
from dlt.common.configuration import with_config
from dlt.common.configuration.specs import CredentialsConfiguration, configspec
from dlt.common.typing import TDataItems, TSecretStrValue
from dlt.sources import DltResource
from dlt.sources.filesystem import FileItemDict
from dlt.sources.filesystem import filesystem as list_files
from dlt.sources.helpers.requests import HTTPError
from dlt.sources.helpers.rest_client import RESTClient

from .filesystem import SharepointFileSystem, validate_folder_path
from .graph import entra_auth, graph_client
from .readers import TFileType, file_rows

# fields SharePoint fills for its own UI, not data of the item
UI_FIELDS = {
    "Edit",
    "LinkTitle",
    "LinkTitleNoMenu",
    "DocIcon",
    "FolderChildCount",
    "ItemChildCount",
}


@configspec
class SharepointCredentials(CredentialsConfiguration):
    """Entra ID app credentials and the site to read.

    Attributes:
        tenant_id (str): Directory (tenant) id of the Entra ID app.
        client_id (str): Application (client) id of the Entra ID app.
        client_secret (TSecretStrValue): Client secret value of the Entra ID app.
        site_id (str): Composite Graph site id "<host>.sharepoint.com,<site-collection-guid>,<web-guid>".
    """

    tenant_id: str = None
    client_id: str = None
    client_secret: TSecretStrValue = None
    site_id: str = None


@dlt.resource(
    name=lambda args: args["list_title"],
    write_disposition="merge",
    primary_key="id",
    columns={"_deleted": {"data_type": "bool", "hard_delete": True}},
)
def sharepoint_list(
    list_title: str = dlt.config.value,
    included_columns: Optional[List[str]] = None,
    credentials: SharepointCredentials = dlt.secrets.value,
) -> Iterator[TDataItems]:
    """Syncs the items of one list: the first run reads the whole list, later runs only what changed.

    The resource and its table are named after the list; rename them with `with_name`. Items
    deleted in SharePoint are deleted from the table. `refresh="drop_resources"` reads the whole
    list again. Multi-value columns become child tables; `sharepoint_lists` with
    `source.max_table_nesting = 0` set on it keeps them as json.

    Args:
        list_title (str): Display name of the list.
        included_columns (Optional[List[str]]): Internal column names to load. Without it, Graph's
            default columns are loaded, minus the ones SharePoint uses only in its UI.
        credentials (SharepointCredentials): Entra ID app credentials and the site id.

    Yields:
        TDataItems: Pages of items keyed by "id"; a deleted item has `_deleted` set and no fields.
    """
    yield from list_items(site_client(credentials), list_title, included_columns)


@dlt.source(name="sharepoint_lists")
def sharepoint_lists(
    list_titles: Optional[List[str]] = None,
    credentials: SharepointCredentials = dlt.secrets.value,
) -> Iterable[DltResource]:
    """Loads several lists of the site, one table per list, each synced like `sharepoint_list`.

    Args:
        list_titles (Optional[List[str]]): Display names of the lists to load. Without it, the
            site's lists are read when the source is created and every visible list of the
            generic template (made with "Blank list" or "From CSV") is loaded.
        credentials (SharepointCredentials): Entra ID app credentials and the site id.

    Returns:
        Iterable[DltResource]: One resource per list.

    Raises:
        ValueError: If there is no list to load, or two lists would load into the same table.
    """
    client = site_client(credentials)
    titles = list_titles
    if titles is None:
        titles = [
            site_list["displayName"]
            for site_list in site_lists(client)
            if site_list.get("list", {}).get("template") == "genericList"
            and not site_list.get("list", {}).get("hidden")
        ]
    if not titles:
        raise ValueError("sharepoint_lists has no list to load")
    naming = dlt.current.source_schema().naming
    tables: Dict[str, str] = {}
    for title in titles:
        table = naming.normalize_table_identifier(title)
        if table in tables:
            raise ValueError(
                f"Lists '{tables[table]}' and '{title}' would both load into table '{table}'."
                " Load one of them with sharepoint_list renamed with with_name."
            )
        tables[table] = title
    return [sharepoint_list(title, credentials=credentials) for title in titles]


def site_client(credentials: SharepointCredentials) -> RESTClient:
    """Returns a Graph client rooted at the site of the credentials."""
    auth = entra_auth(
        credentials.tenant_id, credentials.client_id, credentials.client_secret
    )
    return graph_client(credentials.site_id, auth)


def site_lists(client: RESTClient) -> List[Dict[str, Any]]:
    """Returns every list of the site as Graph describes it."""
    return [site_list for page in client.paginate("lists") for site_list in page]


def list_items(
    client: RESTClient, list_title: str, included_columns: Optional[List[str]]
) -> Iterator[TDataItems]:
    """Yields the pages of one list from Graph's delta query, continuing from the stored delta link."""
    select = ",".join(sorted(included_columns)) if included_columns else None
    expand = f"fields($select={select})" if select else "fields"
    state = dlt.current.resource_state()
    # a delta link continues exactly the list and columns it was issued for
    key = f"delta_link:{list_title}:{expand}"
    delta_link = state.get(key)
    if delta_link:
        pages = client.paginate(delta_link)
    else:
        pages = client.paginate(
            f"lists/{quote(list_title, safe='')}/items/delta",
            params={"$expand": expand},
        )
    try:
        for page in pages:
            yield [item_fields(item, keep_all=select is not None) for item in page]
            next_link = page.response.json().get("@odata.deltaLink")
            if next_link:
                state[key] = next_link
    except HTTPError as e:
        status = e.response.status_code if e.response is not None else None
        if delta_link and status in (404, 410):
            raise ValueError(
                f"The stored delta link of list '{list_title}' is no longer valid"
                f' (Graph {status}). Run the pipeline with refresh="drop_resources" to'
                " read the whole list again."
            ) from e
        raise


def item_fields(item: Dict[str, Any], keep_all: bool) -> Dict[str, Any]:
    """Returns the fields of a list item plus its id, or its id with `_deleted` for a deleted item."""
    if "deleted" in item:
        return {"id": item["id"], "_deleted": True}
    fields = {
        key: value
        for key, value in item.get("fields", {}).items()
        if not key.startswith("@odata.")
        and (keep_all or (key not in UI_FIELDS and not key.startswith("_")))
    }
    return {**fields, "id": item["id"]}


@with_config(sections=("sources", "sharepoint"))
def sharepoint_folder(
    folder_path: str = dlt.config.value,
    library: Optional[str] = None,
    file_glob: str = "*",
    credentials: SharepointCredentials = dlt.secrets.value,
) -> DltResource:
    """Lists the files of a library folder with dlt's `filesystem` resource, new or modified files only.

    The resource yields `FileItem`s, so any reader transformer can follow it: `read_files`, or
    dlt's `read_csv`, `read_jsonl` and `read_parquet`.

    Args:
        folder_path (str): Folder relative to the library root; "" lists the root itself.
        library (Optional[str]): Display name of the document library, the site's default
            library when not set.
        file_glob (str): Glob matched against paths under the folder; "**/*.csv" also
            searches subfolders.
        credentials (SharepointCredentials): Entra ID app credentials and the site id.

    Returns:
        DltResource: dlt's `filesystem` resource over the folder, incremental on `modification_date`.

    Raises:
        ValueError: If folder_path is not valid.
    """
    folder_path = validate_folder_path(folder_path)
    fs = SharepointFileSystem(site_client(credentials), library)
    return list_files(
        bucket_url=f"sharepoint://{library or ''}/{folder_path}",
        credentials=fs,
        file_glob=file_glob,
        incremental=dlt.sources.incremental("modification_date"),
    )


@dlt.transformer(write_disposition="merge", merge_key="file_url")
def read_files(
    items: Iterable[FileItemDict],
    file_type: TFileType = dlt.config.value,
    reader_kwargs: Optional[Dict[str, Any]] = None,
) -> Iterator[TDataItems]:
    """Reads csv, jsonl, parquet or excel files into rows that carry `file_url`, `file_name` and `file_modified_at`.

    Pipe it from `sharepoint_folder` or from dlt's `filesystem` resource. A modified file
    replaces its own rows; rename the table with `with_name` or a `table_name` hint. Excel
    workbooks load one table per sheet, "<table>_<sheet>", unless one `sheet_name` is given.

    Args:
        items (Iterable[FileItemDict]): The files, as the listing resource yields them.
        file_type (TFileType): One of "csv", "jsonl", "parquet" or "excel".
        reader_kwargs (Optional[Dict[str, Any]]): Arguments for the pandas reader, such as
            "sep", "encoding" or "sheet_name"; for parquet the pyarrow "columns".

    Yields:
        TDataItems: Batches of rows.
    """
    kwargs = dict(reader_kwargs or {})
    resource = dlt.current.resource()
    hint = resource.table_name
    table = hint if isinstance(hint, str) else resource.name
    yield from file_rows(items, file_type, kwargs, table)


@dlt.source(name="sharepoint_files", max_table_nesting=0)
def sharepoint_files(
    folder_path: str = dlt.config.value,
    file_type: TFileType = dlt.config.value,
    table_name: str = dlt.config.value,
    library: Optional[str] = None,
    file_glob: str = "*",
    reader_kwargs: Optional[Dict[str, Any]] = None,
    credentials: SharepointCredentials = dlt.secrets.value,
) -> DltResource:
    """Loads the rows of the files matching a glob in a folder: `sharepoint_folder` piped into `read_files`.

    Only new or modified files are read; a modified file replaces its own rows, a renamed file
    counts as a new file, and a file emptied or deleted in SharePoint keeps its rows.
    `refresh="drop_resources"` reads every file again.

    Args:
        folder_path (str): Folder relative to the library root; "" loads the root itself.
        file_type (TFileType): One of "csv", "jsonl", "parquet" or "excel".
        table_name (str): Destination table; Excel sheets go to "<table_name>_<sheet>".
        library (Optional[str]): Display name of the document library, the site's default
            library when not set.
        file_glob (str): Glob matched against paths under the folder; "**/*.csv" also
            searches subfolders.
        reader_kwargs (Optional[Dict[str, Any]]): Arguments for the pandas reader, such as
            "sep", "encoding" or "sheet_name"; for parquet the pyarrow "columns".
        credentials (SharepointCredentials): Entra ID app credentials and the site id.

    Returns:
        DltResource: The "<table_name>" rows, merged on `file_url`.

    Raises:
        ValueError: If folder_path contains glob characters.
    """
    files = sharepoint_folder(folder_path, library, file_glob, credentials=credentials)
    rows = read_files(file_type, reader_kwargs).with_name(table_name)
    return files.with_name(f"{table_name}_files") | rows
