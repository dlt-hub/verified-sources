"""Loads SharePoint list items and files from a document library via Microsoft Graph."""

from typing import Any, Dict, Iterable, Iterator, List, Optional
from urllib.parse import quote

import dlt
from dlt.common.configuration.specs import (
    BaseConfiguration,
    CredentialsConfiguration,
    configspec,
)
from dlt.common.typing import TDataItems, TSecretStrValue
from dlt.sources import DltResource
from dlt.sources.filesystem import FileItemDict
from dlt.sources.filesystem import filesystem as list_files
from dlt.sources.helpers.requests import HTTPError
from dlt.sources.helpers.rest_client import RESTClient

from .filesystem import SharepointFileSystem, validate_folder_path
from .graph import entra_auth, graph_client
from .readers import TFileType, file_rows, validate_reader_kwargs


@configspec
class SharepointCredentials(CredentialsConfiguration):
    """Entra ID app credentials and the site to read.

    Attributes:
        tenant_id: Directory (tenant) id of the Entra ID app.
        client_id: Application (client) id of the Entra ID app.
        client_secret: Client secret value of the Entra ID app.
        site_id: Composite Graph site id "<host>.sharepoint.com,<site-collection-guid>,<web-guid>".
    """

    tenant_id: str = None
    client_id: str = None
    client_secret: TSecretStrValue = None
    site_id: str = None


@configspec
class SharepointListConfig(BaseConfiguration):
    """Which list to load and into which table.

    Attributes:
        list_title: Display name of the list.
        table_name: Destination table, the list title when not set.
        select: Comma-separated internal column names to load. Without it, Graph's default
            columns are loaded, minus the ones SharePoint uses only for its own UI.
    """

    list_title: str = None
    table_name: Optional[str] = None
    select: Optional[str] = None


@configspec
class SharepointListsConfig(BaseConfiguration):
    """Which lists of the site to load, one table per list.

    Attributes:
        list_titles: Display names of the lists; every visible generic list of the site when not set.
    """

    list_titles: Optional[List[str]] = None


@configspec
class SharepointFilesConfig(BaseConfiguration):
    """Which files of a document library to load, how to parse them and into which table.

    Attributes:
        folder_path: Folder relative to the library root; "" loads the root itself.
        file_type: One of "csv", "jsonl", "parquet" or "excel".
        table_name: Destination table.
        library: Display name of the document library, the site's default library when not set.
        file_glob: Glob matched against paths under the folder; "**/*.csv" also searches subfolders.
        reader_kwargs: Arguments for the pandas reader, such as "sep" or "sheet_name"; for parquet
            only "columns" is allowed.
    """

    folder_path: str = None
    file_type: TFileType = None
    table_name: str = None
    library: Optional[str] = None
    file_glob: str = "*"
    reader_kwargs: Optional[Dict[str, Any]] = None


@dlt.source(name="sharepoint_list", max_table_nesting=0)
def sharepoint_list(
    sharepoint_list_config: SharepointListConfig = dlt.config.value,
    credentials: SharepointCredentials = dlt.secrets.value,
) -> DltResource:
    """Loads the items of one SharePoint list and keeps the table in sync with Graph's delta query.

    The first run reads every item. Later runs read only the items added, changed or deleted
    since the delta link kept in the resource state; deleted items are deleted from the table,
    which needs a destination that supports merge and leaves a `_deleted` column that is NULL
    on live rows. `refresh="drop_resources"` reads the whole list again. Multi-value columns
    load as json columns; for child tables set `source.max_table_nesting = 2` on the source.

    Args:
        sharepoint_list_config: The list to load and the destination table.
        credentials: Entra ID app credentials and the site id.

    Returns:
        DltResource: Resource named after the table, merged on the item "id".
    """
    config = sharepoint_list_config
    return list_resource(
        site_client(credentials),
        config.list_title,
        config.table_name or config.list_title,
        config.select,
    )


@dlt.source(name="sharepoint_lists", max_table_nesting=0)
def sharepoint_lists(
    sharepoint_lists_config: SharepointListsConfig = dlt.config.value,
    credentials: SharepointCredentials = dlt.secrets.value,
) -> List[DltResource]:
    """Loads several lists of the site, each into a table named after the list.

    Each list is loaded like `sharepoint_list` does. Without `list_titles`, the site's lists are
    read when the source is created, and every visible list of the generic list template (the
    kind made with "Blank list" or "From CSV") is loaded.

    Args:
        sharepoint_lists_config: The lists to load.
        credentials: Entra ID app credentials and the site id.

    Returns:
        List[DltResource]: One resource per list.

    Raises:
        ValueError: If there is no list to load, or two lists would load into the same table.
    """
    client = site_client(credentials)
    titles = sharepoint_lists_config.list_titles
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
                " Load one of them with sharepoint_list and another table_name."
            )
        tables[table] = title
    return [list_resource(client, title, title, None) for title in titles]


def site_client(credentials: SharepointCredentials) -> RESTClient:
    auth = entra_auth(
        credentials.tenant_id, credentials.client_id, credentials.client_secret
    )
    return graph_client(credentials.site_id, auth)


def site_lists(client: RESTClient) -> List[Dict[str, Any]]:
    return [site_list for page in client.paginate("lists") for site_list in page]


def list_resource(
    client: RESTClient, list_title: str, table_name: str, select: Optional[str]
) -> DltResource:
    """Returns the resource that syncs one list through Graph's delta query."""
    expand = f"fields($select={select})" if select else "fields"
    settings = {"list_title": list_title, "expand": expand}

    @dlt.resource(
        name=table_name,
        write_disposition="merge",
        primary_key="id",
        columns={"_deleted": {"data_type": "bool", "hard_delete": True}},
    )
    def list_items() -> Iterator[TDataItems]:
        state = dlt.current.resource_state()
        if state.get("settings", settings) != settings:
            raise ValueError(
                f"Table '{table_name}' was loaded with another list or select. Run the pipeline"
                ' with refresh="drop_resources" to load it again.'
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

    return list_items


UI_FIELDS = {
    "Edit",
    "LinkTitle",
    "LinkTitleNoMenu",
    "DocIcon",
    "FolderChildCount",
    "ItemChildCount",
}


def item_fields(item: Dict[str, Any], keep_all: bool) -> Dict[str, Any]:
    """Returns the fields of a list item plus its id, or a deletion marker for a removed item.

    Unless `keep_all` is set, SharePoint's UI-only and `_`-prefixed system fields are left out.
    """
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
    sharepoint_files_config: SharepointFilesConfig = dlt.config.value,
    credentials: SharepointCredentials = dlt.secrets.value,
) -> DltResource:
    """Loads the rows of the matching files in a folder; only new or modified files are read.

    Files are matched by `file_glob` under `folder_path`, so subfolders are read only with a
    pattern like "**/*.csv". A modified file replaces its own rows, keyed by `_file_url`, which
    needs a destination that supports merge; a file emptied in SharePoint loses its rows. A
    renamed file counts as a new file, and files deleted in SharePoint keep their rows.
    `refresh="drop_resources"` reads every file again. Excel workbooks load one table per
    sheet, "<table_name>_<sheet>", unless `reader_kwargs` names a `sheet_name`.

    Args:
        sharepoint_files_config: The folder, file filter, parser settings and destination table.
        credentials: Entra ID app credentials and the site id.

    Returns:
        DltResource: The file listing piped into the "<table_name>" rows.

    Raises:
        ValueError: If folder_path, file_type or reader_kwargs is not valid.
    """
    config = sharepoint_files_config
    folder_path = validate_folder_path(config.folder_path)
    reader_kwargs = dict(config.reader_kwargs or {})
    validate_reader_kwargs(config.file_type, reader_kwargs)
    fs = SharepointFileSystem(site_client(credentials), config.library)

    files = list_files(
        bucket_url=f"sharepoint://{config.library or ''}/{folder_path}",
        credentials=fs,
        file_glob=config.file_glob,
        incremental=dlt.sources.incremental("modification_date"),
    ).with_name(f"{config.table_name}_files")

    @dlt.transformer(
        name=config.table_name,
        write_disposition="merge",
        merge_key="_file_url",
        columns={"_deleted": {"data_type": "bool", "hard_delete": True}},
    )
    def rows(items: Iterable[FileItemDict]) -> Iterator[TDataItems]:
        yield from file_rows(items, config.file_type, reader_kwargs, config.table_name)

    return files | rows
