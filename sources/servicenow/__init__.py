"""Loads ServiceNow tables with the Table API or the CSV export.

`servicenow_source` loads the tables in `DEFAULT_TABLES` or the tables you pass.
`servicenow_table` loads any table, including custom tables.

ServiceNow REST API docs: https://www.servicenow.com/docs/r/api-reference/rest-apis/c_TableAPI.html
"""

from typing import Any, Dict, Iterable, List, Optional, Sequence

import dlt
from dlt.common.configuration import with_config
from dlt.common.configuration.specs import ConfigSectionContext, known_sections
from dlt.common.schema.typing import TWriteDispositionConfig
from dlt.common.typing import TAnyDateTime, TColumnNames
from dlt.sources import DltResource
from dlt.sources.helpers.requests import Session

from .helpers.client import ServiceNowClient
from .helpers.credentials import ServiceNowAuth
from .helpers.schema import LazyDictionary, read_dictionary
from .helpers.table import SOURCE_SECTION, TReadApi, TTableBackend, table_resource

# IT service management, service catalog, knowledge, users, organization and CMDB tables
DEFAULT_TABLES = (
    "incident",
    "problem",
    "change_request",
    "change_task",
    "sc_request",
    "sc_req_item",
    "sc_task",
    "task_sla",
    "kb_knowledge",
    "sys_user",
    "sys_user_group",
    "sys_user_grmember",
    "core_company",
    "cmn_location",
    "cmn_department",
    "cmdb_ci",
    "sys_choice",
)


@dlt.source(name="servicenow")
def servicenow_source(
    credentials: ServiceNowAuth = dlt.secrets.value,
    tables: Sequence[str] = DEFAULT_TABLES,
    session: Optional[Session] = None,
    start_date: Optional[TAnyDateTime] = None,
    write_disposition: Optional[TWriteDispositionConfig] = None,
    encoded_queries: Optional[Dict[str, str]] = None,
    include_deleted: bool = True,
    api: TReadApi = "table",
    backend: TTableBackend = "auto",
    page_size: int = 10000,
    allow_partitioned_load: bool = True,
    max_partitions: int = 10,
    defer_table_reflect: bool = False,
) -> Iterable[DltResource]:
    """Loads ServiceNow tables, by default the most common tables in `DEFAULT_TABLES`.

    Every resource is a `servicenow_table` named after its table, and all resources get the same
    arguments. Resources read their tables in parallel. The fields of all tables are read at once,
    with one query of `sys_db_object` and one of `sys_dictionary`. `with_resources` selects
    tables. `apply_hints` replaces the incremental of a resource, together with its lag and
    deduplication.

    Args:
        credentials (ServiceNowAuth): Instance and OAuth, API key or basic auth credentials.
        tables (Sequence[str]): Names of the tables to load.
        session (Optional[Session]): Requests session. The default session retries HTTP 429 and
            5xx and does not keep cookies. The instance runs the requests that share a session
            cookie one at a time. A session that keeps cookies does not read tables in parallel.
        start_date (Optional[TAnyDateTime]): Loads records changed since this date on the first
            run, all records if not set. See `servicenow_table`.
        write_disposition (Optional[TWriteDispositionConfig]): Write disposition of all
            resources. "replace" reloads all records, or the records changed since `start_date`,
            on each run. Derived per table if not set.
        encoded_queries (Optional[Dict[str, str]]): Encoded queries by table name, for example
            `{"incident": "active=true^priority=1"}`.
        include_deleted (bool): Deletes records deleted in ServiceNow from the destination when
            merging on `sys_updated_on`. Requires read access to `sys_audit_delete`.
        api (TReadApi): "table" reads the Table API. "csv" reads the CSV export, which dlt
            parses with less CPU. The export requires `pyarrow` and basic or OAuth credentials.
            Characters outside of Windows-1252 also require the `glide.export.csv.charset`
            system property set to UTF-8.
        backend (TTableBackend): "pyarrow" yields Arrow tables. "python" yields lists of dicts
            and does not require `pyarrow`. "auto" uses `pyarrow` if installed.
        page_size (int): Maximum number of records in a request. See `servicenow_table`.
        allow_partitioned_load (bool): Reads tables of more than one page in `sys_id` ranges in
            parallel. See `servicenow_table`.
        max_partitions (int): Maximum number of `sys_id` ranges of a table. See
            `servicenow_table`.
        defer_table_reflect (bool): Reads the fields of all tables only when extracting, with
            one query of `sys_dictionary`, so creating the source makes no API calls. Enable when
            running on Airflow. See `servicenow_table`.

    Returns:
        Iterable[DltResource]: Resources loading the tables.
    """
    encoded_queries = encoded_queries or {}
    unknown = set(encoded_queries) - set(tables)
    if unknown:
        raise ValueError(
            f"`encoded_queries` contains unknown tables that are not in `tables`: {sorted(unknown)}"
        )
    client = ServiceNowClient(credentials, session)
    if defer_table_reflect:
        dictionary = None
        lazy_dictionary: Optional[LazyDictionary] = LazyDictionary(client, tables)
    else:
        dictionary = read_dictionary(client, list(tables))
        lazy_dictionary = None
    return [
        table_resource(
            client,
            table,
            dictionary=dictionary,
            start_date=start_date,
            encoded_query=encoded_queries.get(table),
            include_deleted=include_deleted,
            api=api,
            backend=backend,
            page_size=page_size,
            allow_partitioned_load=allow_partitioned_load,
            max_partitions=max_partitions,
            write_disposition=write_disposition,
            parallelized=True,
            defer_table_reflect=defer_table_reflect,
            lazy_dictionary=lazy_dictionary,
        )
        for table in tables
    ]


@with_config(
    sections=(known_sections.SOURCES, SOURCE_SECTION),
    section_arg_name="table",
    sections_merge_style=ConfigSectionContext.resource_merge_style,
)
def servicenow_table(
    credentials: ServiceNowAuth = dlt.secrets.value,
    table: str = dlt.config.value,
    session: Optional[Session] = None,
    start_date: Optional[TAnyDateTime] = None,
    incremental: Optional[dlt.sources.incremental[Any]] = None,
    lag: Optional[float] = None,
    encoded_query: Optional[str] = None,
    include_deleted: bool = True,
    api: TReadApi = "table",
    backend: TTableBackend = "auto",
    page_size: int = 10000,
    allow_partitioned_load: bool = True,
    max_partitions: int = 10,
    included_columns: Optional[List[str]] = None,
    excluded_columns: Optional[List[str]] = None,
    write_disposition: Optional[TWriteDispositionConfig] = None,
    primary_key: TColumnNames = None,
    merge_key: TColumnNames = None,
    defer_table_reflect: bool = False,
) -> DltResource:
    """Loads a single table, including custom tables, ordered by `sys_id`.

    Fields and their types are taken from `sys_dictionary`, including fields inherited from the
    tables it extends, and passed as column hints. Durations are loaded as seconds. Journal and
    password fields are skipped. Requests are retried on HTTP 429 and 5xx.

    Args:
        credentials (ServiceNowAuth): Instance and OAuth, API key or basic auth credentials.
        table (str): Name of the table, for example `incident` or `u_custom_table`. Config is
            resolved under `sources.servicenow.<table>`.
        session (Optional[Session]): Requests session. See `servicenow_source`.
        start_date (Optional[TAnyDateTime]): Loads records changed since this date on the first
            run, all records if not set. It is the initial value of the default incremental, on
            `sys_updated_on` or else on `sys_created_on`. Records without a cursor value are read
            on the first run, when no start date is set. The "replace" write disposition loads
            all records, or the records changed since `start_date`, on each run. Records changed
            during the run are read on the next run.
        incremental (Optional[dlt.sources.incremental[Any]]): Your own incremental instead of the
            default one, with its lag and deduplication. An incremental set with `apply_hints`
            replaces the default one in the same way. Cursor types and chunked loads with
            `row_order` and `add_limit` are described in the dlt docs.
        lag (Optional[float]): Lag of the default incremental in seconds. Each run reads again
            the records changed in this time before the last cursor value, to catch records
            committed late. 600 when merging, None otherwise.
        encoded_query (Optional[str]): Encoded query that filters the records, for example
            `active=true^priority=1`. ServiceNow ignores invalid parts of encoded queries.
        include_deleted (bool): When merging with a `max` incremental on `sys_updated_on`, reads
            the records deleted in the cursor range from `sys_audit_delete` and deletes them from
            the destination. Requires read access to `sys_audit_delete` and auditing of deletes
            of the table.
        api (TReadApi): "table" reads the Table API. "csv" reads the CSV export, which dlt
            parses with less CPU. The export requires `pyarrow` and basic or OAuth credentials.
            Characters outside of Windows-1252 also require the `glide.export.csv.charset`
            system property set to UTF-8.
        backend (TTableBackend): "pyarrow" yields Arrow tables. "python" yields lists of dicts
            and does not require `pyarrow`. "auto" uses `pyarrow` if installed. Both give the
            same types.
        page_size (int): Maximum number of records in a request. Halved down to 100 when a
            request exceeds a transaction quota of the instance. The CSV export returns at most
            10000 records, so with `api="csv"` a larger page size is lowered to 10000, with a
            warning.
        allow_partitioned_load (bool): When the first page is full, counts the records after it
            and reads them in `sys_id` ranges in parallel with the dlt extract workers. Not used
            with `row_order` or `add_limit`. False reads all pages one after another.
        max_partitions (int): Maximum number of `sys_id` ranges of a table. The dlt extract
            workers read the ranges, 5 by default, set with `workers` in the `[extract]` config
            section. About twice as many ranges as workers keep all workers busy.
        included_columns (Optional[List[str]]): Fields to load, all fields if not set. `sys_id`,
            the cursor field, the primary key and the merge key are always loaded, unless
            `excluded_columns` skips them.
        excluded_columns (Optional[List[str]]): Fields to skip.
        write_disposition (Optional[TWriteDispositionConfig]): If not set: "merge" for a cursor
            on `sys_updated_on` or a custom field, "append" for tables with only a
            `sys_created_on` cursor, "replace" without incremental. `{"disposition": "replace"}`
            keeps the incremental state, so the table is replaced with the records changed
            since the last run.
        primary_key (TColumnNames): Primary key, `sys_id` if not set.
        merge_key (TColumnNames): Merge key of the resource.
        defer_table_reflect (bool): Reads the fields of the table only when extracting, so
            creating the resource makes no API calls. Enable when running on Airflow. Column
            hints are then set when extracting, and the default incremental expects
            `sys_updated_on`, which nearly all tables have.

    Returns:
        DltResource: The dlt resource loading the table.
    """
    if start_date is not None and incremental is not None:
        raise ValueError("Pass `start_date` or `incremental`, not both")
    return table_resource(
        ServiceNowClient(credentials, session),
        table,
        start_date=start_date,
        incremental=incremental,
        lag=lag,
        encoded_query=encoded_query,
        include_deleted=include_deleted,
        api=api,
        backend=backend,
        page_size=page_size,
        allow_partitioned_load=allow_partitioned_load,
        max_partitions=max_partitions,
        included_columns=included_columns,
        excluded_columns=excluded_columns,
        write_disposition=write_disposition,
        primary_key=primary_key,
        merge_key=merge_key,
        defer_table_reflect=defer_table_reflect,
    )
