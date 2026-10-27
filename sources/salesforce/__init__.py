"""Loads Salesforce sObjects with the Bulk API 2.0.

`salesforce_source` loads the sObjects in `SOURCE_SOBJECTS`, `salesforce_table` loads any sObject,
including custom objects.

Salesforce API docs: https://developer.salesforce.com/docs/apis
"""

from functools import partial
from typing import Any, Dict, Iterable, List, Literal, Optional, Union

import dlt
from dlt.common.schema.typing import TWriteDispositionConfig
from dlt.common.typing import TColumnNames
from dlt.sources import DltResource
from dlt.sources.helpers.requests import Client, Session

from .helpers.connection import SalesforceConnection
from .helpers.credentials import SalesforceAuth, make_salesforce_client
from .helpers.query import TQueryApi
from .helpers.table import (
    SalesforceTableResourceConfiguration,
    TTableBackend,
    table_resource,
)

# resource names and the sObjects they load
SOURCE_SOBJECTS = {
    "sf_user": "User",
    "user_role": "UserRole",
    "opportunity": "Opportunity",
    "opportunity_line_item": "OpportunityLineItem",
    "opportunity_contact_role": "OpportunityContactRole",
    "account": "Account",
    "contact": "Contact",
    "lead": "Lead",
    "campaign": "Campaign",
    "campaign_member": "CampaignMember",
    "product_2": "Product2",
    "pricebook_2": "Pricebook2",
    "pricebook_entry": "PricebookEntry",
    "task": "Task",
    "event": "Event",
}


def _connection(
    credentials: SalesforceAuth, session: Optional[Session]
) -> SalesforceConnection:
    # retries 429 and 5xx, other errors are raised by simple_salesforce
    session = session or Client(raise_for_status=False).session
    return SalesforceConnection(partial(make_salesforce_client, credentials, session))


@dlt.source(name="salesforce")
def salesforce_source(
    credentials: SalesforceAuth = dlt.secrets.value,
    session: Optional[Session] = None,
    incremental: Optional[Literal["auto"]] = "auto",
    write_disposition: Optional[TWriteDispositionConfig] = None,
    soql_filters: Optional[Dict[str, str]] = None,
    include_deleted: bool = True,
    double_as_decimal: bool = False,
    backend: TTableBackend = "auto",
    api: TQueryApi = "auto",
    chunk_size: int = 50000,
    defer_table_reflect: bool = False,
) -> Iterable[DltResource]:
    """Loads the most common Salesforce sObjects, see `SOURCE_SOBJECTS`.

    Every resource is a `salesforce_table` and the arguments are passed to all of them. Resources
    run their queries in parallel. Use `with_resources` to select sObjects and `apply_hints` to
    change e.g. the incremental of a single resource.

    Args:
        credentials (SalesforceAuth): Salesforce credentials.
        session (Optional[Session]): Requests session used by `simple_salesforce`, by default
            a session retrying HTTP 429 and 5xx.
        incremental (Optional[Literal["auto"]]): "auto" loads records changed since the last run,
            None loads all records. See `salesforce_table`.
        write_disposition (Optional[TWriteDispositionConfig]): Write disposition of all resources,
            "replace" reloads all records on each run. Derived per sObject if not set.
        soql_filters (Optional[Dict[str, str]]): SOQL conditions by resource name, e.g.
            `{"opportunity": "IsClosed = false"}`.
        include_deleted (bool): Also loads deleted records still in the recycle bin and archived
            activities, and deletes them from the destination when merging.
        double_as_decimal (bool): Loads `double` and `percent` fields as decimals.
        backend (TTableBackend): "pyarrow" yields Arrow tables, "python" lists of dicts and does
            not require `pyarrow`. "auto" uses `pyarrow` if installed.
        api (TQueryApi): "bulk" or "rest" API, "auto" uses the Bulk API 2.0 and the REST API for
            sObjects not supported by it.
        chunk_size (int): Maximum number of records in a result page.
        defer_table_reflect (bool): Logs in and describes the sObjects only when extracting, so
            creating the source makes no API calls. Enable when running on Airflow.

    Returns:
        Iterable[DltResource]: Resources loading the sObjects.
    """
    soql_filters = soql_filters or {}
    unknown = set(soql_filters) - set(SOURCE_SOBJECTS)
    if unknown:
        raise ValueError(f"`soql_filters` for unknown resources: {sorted(unknown)}")
    connection = _connection(credentials, session)
    return [
        table_resource(
            connection,
            sobject,
            name=name,
            incremental=incremental,
            soql_filter=soql_filters.get(name),
            include_deleted=include_deleted,
            double_as_decimal=double_as_decimal,
            backend=backend,
            api=api,
            chunk_size=chunk_size,
            write_disposition=write_disposition,
            parallelized=True,
            defer_table_reflect=defer_table_reflect,
        )
        for name, sobject in SOURCE_SOBJECTS.items()
    ]


@dlt.resource(
    name=lambda args: args["sobject"], spec=SalesforceTableResourceConfiguration
)
def salesforce_table(
    credentials: SalesforceAuth = dlt.secrets.value,
    sobject: str = dlt.config.value,
    # "auto" goes first so dlt does not bind the incremental to this factory and replace "auto"
    # with None. the incremental is bound by the resource returned below
    incremental: Union[Literal["auto"], dlt.sources.incremental[Any], None] = "auto",
    lag: Optional[float] = None,
    soql_filter: Optional[str] = None,
    include_deleted: bool = True,
    double_as_decimal: bool = False,
    backend: TTableBackend = "auto",
    api: TQueryApi = "auto",
    chunk_size: int = 50000,
    included_columns: Optional[List[str]] = None,
    excluded_columns: Optional[List[str]] = None,
    write_disposition: Optional[TWriteDispositionConfig] = None,
    primary_key: TColumnNames = None,
    merge_key: TColumnNames = None,
    defer_table_reflect: bool = False,
) -> DltResource:
    """Loads a single sObject, including custom objects, with a SOQL query.

    Fields and their types are taken from the sObject `describe()` and passed as column hints.
    Requests are retried on HTTP 429 and 5xx, an expired session is renewed by logging in again.

    Args:
        credentials (SalesforceAuth): Salesforce credentials.
        sobject (str): API name of the sObject, e.g. `Account` or `Invoice__c`.
        incremental (Union[Literal["auto"], dlt.sources.incremental[Any], None]): Generates
            the WHERE clause of the query. "auto" picks the first of `SystemModstamp`,
            `LastModifiedDate`, `CreatedDate` and `LoginTime` present in the sObject, None loads
            all records. "auto" with the "replace" write disposition loads all records too.
            Setting `row_order` adds ORDER BY, which disables PK chunking of Bulk API jobs.
        lag (Optional[float]): Seconds re-read on each run by the "auto" incremental, to catch
            records committed late. 600 when merging, None otherwise.
        soql_filter (Optional[str]): SOQL condition added to the WHERE clause, e.g.
            `IsClosed = false AND Type = 'Customer'`.
        include_deleted (bool): Also loads deleted records still in the recycle bin and archived
            activities (`queryAll`). When merging, `IsDeleted` is the `hard_delete` column so
            records deleted in Salesforce are deleted from the destination.
        double_as_decimal (bool): Loads `double` and `percent` fields, e.g. custom number
            fields, as decimals with the field precision and scale instead of floats.
        backend (TTableBackend): "pyarrow" yields Arrow tables, "python" lists of dicts and does
            not require `pyarrow`. "auto" uses `pyarrow` if installed. Both give the same types.
        api (TQueryApi): "bulk" runs a Bulk API 2.0 query job, "rest" pages through the REST API
            query results. "auto" uses the REST API when the Bulk API does not support the
            sObject, e.g. `TaskStatus`, or is not enabled in the org.
        chunk_size (int): Maximum number of records in a result page.
        included_columns (Optional[List[str]]): Fields to load, all queryable fields if not set.
        excluded_columns (Optional[List[str]]): Fields to skip.
        write_disposition (Optional[TWriteDispositionConfig]): If not set: "merge" for a cursor
            on `SystemModstamp`, `LastModifiedDate` or a custom field, "append" for insert-only
            sObjects with `CreatedDate` or `LoginTime` cursor, "replace" without incremental.
            From dlt 1.32 `{"disposition": "replace"}` keeps the incremental state, so the table
            is replaced with the records changed since the last run.
        primary_key (TColumnNames): Primary key, the `Id` field of the sObject if not set.
        merge_key (TColumnNames): Merge key of the resource.
        defer_table_reflect (bool): Logs in and describes the sObject only when extracting, so
            creating the resource makes no API calls. Enable when running on Airflow. Column
            hints are then set when extracting and the "auto" incremental expects
            `SystemModstamp`, which all custom and nearly all standard sObjects have.

    Returns:
        DltResource: The dlt resource loading the sObject.
    """
    return table_resource(
        _connection(credentials, None),
        sobject,
        incremental=incremental,
        lag=lag,
        soql_filter=soql_filter,
        include_deleted=include_deleted,
        double_as_decimal=double_as_decimal,
        backend=backend,
        api=api,
        chunk_size=chunk_size,
        included_columns=included_columns,
        excluded_columns=excluded_columns,
        write_disposition=write_disposition,
        primary_key=primary_key,
        merge_key=merge_key,
        defer_table_reflect=defer_table_reflect,
    )
