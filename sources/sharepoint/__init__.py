"""Loads SharePoint lists and the files of document libraries through Microsoft Graph.

`sharepoint_lists` and `sharepoint_list` sync list items with Graph's delta query, `sharepoint_files`
reads the rows of csv, jsonl, parquet and excel files. API reference:
https://learn.microsoft.com/graph/api/resources/sharepoint
"""

from typing import Any, Dict, Iterable, Iterator, List, Optional
from urllib.parse import quote

import dlt
from dlt.common.configuration.specs import CredentialsConfiguration, configspec
from dlt.common.typing import TDataItems, TSecretStrValue
from dlt.sources import DltResource
from dlt.sources.filesystem import FileItemDict
from dlt.sources.filesystem import filesystem as list_files
from dlt.sources.helpers.requests import HTTPError
from dlt.sources.helpers.rest_client import RESTClient

from .filesystem import SharepointFileSystem, validate_folder_path
from .graph import entra_auth, graph_client
from .readers import TFileType, file_rows, validate_reader_kwargs

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
    `max_table_nesting = 0` on the source keeps them as json.

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
    select = ",".join(included_columns) if included_columns else None
    expand = f"fields($select={select})" if select else "fields"
    settings = {"list_title": list_title, "expand": expand}
    state = dlt.current.resource_state()
    # a delta link continues the list and columns the table was created with
    if state.get("settings", settings) != settings:
        raise ValueError(
            f"The table of list '{list_title}' was loaded with another list or columns. Run"
            ' the pipeline with refresh="drop_resources" to load it again.'
        )
    state["settings"] = settings
    delta_link = state.get("delta_link")
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
                state["delta_link"] = next_link
    except HTTPError as e:
        status = e.response.status_code if e.response is not None else None
        if delta_link and status in (404, 410):
            raise ValueError(
                f"The stored delta link of list '{list_title}' is no longer valid"
                f' (Graph {status}). Run the pipeline with refresh="drop_resources" to'
                " read the whole list again."
            ) from e
        if status == 404:
            titles = [site_list["displayName"] for site_list in site_lists(client)]
            raise ValueError(
                f"List '{list_title}' not found in the site. Available lists: {titles}"
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
    """Loads the rows of the files matching a glob in a folder; only new or modified files are read.

    A modified or emptied file replaces its own rows, a renamed file counts as a new file and a
    file deleted in SharePoint keeps its rows. `refresh="drop_resources"` reads every file again.

    Args:
        folder_path (str): Folder relative to the library root; "" loads the root itself.
        file_type (TFileType): One of "csv", "jsonl", "parquet" or "excel".
        table_name (str): Destination table. Excel workbooks load one table per sheet,
            "<table_name>_<sheet>", unless `reader_kwargs` names a `sheet_name`.
        library (Optional[str]): Display name of the document library, the site's default
            library when not set.
        file_glob (str): Glob matched against paths under the folder; "**/*.csv" also
            searches subfolders.
        reader_kwargs (Optional[Dict[str, Any]]): Arguments for the pandas reader, such as
            "sep" or "sheet_name"; for parquet only "columns" is allowed.
        credentials (SharepointCredentials): Entra ID app credentials and the site id.

    Returns:
        DltResource: The file listing piped into the "<table_name>" rows, merged on `_file_url`.

    Raises:
        ValueError: If folder_path, file_type or reader_kwargs is not valid.
    """
    folder_path = validate_folder_path(folder_path)
    kwargs = dict(reader_kwargs or {})
    validate_reader_kwargs(file_type, kwargs)
    fs = SharepointFileSystem(site_client(credentials), library)

    files = list_files(
        bucket_url=f"sharepoint://{library or ''}/{folder_path}",
        credentials=fs,
        file_glob=file_glob,
        incremental=dlt.sources.incremental("modification_date"),
    ).with_name(f"{table_name}_files")

    @dlt.transformer(
        name=table_name,
        write_disposition="merge",
        merge_key="_file_url",
        columns={"_deleted": {"data_type": "bool", "hard_delete": True}},
    )
    def rows(items: Iterable[FileItemDict]) -> Iterator[TDataItems]:
        yield from file_rows(items, file_type, kwargs, table_name)

    return files | rows
