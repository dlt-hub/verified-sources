"""Salesforce source helpers"""

import pendulum

from typing import Optional, Iterable, Dict, Set, Any, cast

from simple_salesforce.exceptions import SalesforceMalformedRequest
from simple_salesforce import Salesforce
from dlt.common.typing import TDataItem


from ..settings import IS_PRODUCTION


def _process_record(
    record: Dict[str, Any], date_fields: Set[str], api_type: str
) -> Dict[str, Any]:
    """
    Process a single Salesforce record by removing attributes and converting date fields.

    Args:
        record: The record to process
        date_fields: Set of field names that contain date/datetime values
        api_type: Whether this record came from Bulk API (timestamps), or standard API (ISO strings)

    Returns:
        The processed record
    """
    # Strip out the attributes field
    record.pop("attributes", None)

    for field in date_fields:
        if record.get(field):
            if api_type == "bulk":
                # Bulk API returns timestamps, convert to ISO 8601
                record[field] = pendulum.from_timestamp(
                    record[field] / 1000,
                ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            else:
                # Standard API returns ISO strings, ensure consistent format
                if record[field]:
                    # pendulum.parse can also return Date, Time or Duration
                    # Salesforce date/datetime fields are always DateTime
                    dt = cast(pendulum.DateTime, pendulum.parse(record[field]))
                    record[field] = dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    return record


def get_records(
    sf: Salesforce,
    sobject: str,
    last_state: Optional[str] = None,
    replication_key: Optional[str] = None,
) -> Iterable[TDataItem]:
    """
    Retrieves records from Salesforce for a specified sObject.

    Args:
        sf (Salesforce): An instance of the Salesforce API client.
        sobject (str): The name of the sObject to retrieve records from.
        last_state (str, optional): The last known state for incremental loading. Defaults to None.
        replication_key (str, optional): The replication key for incremental loading. Defaults to None.

    Yields:
        Dict[TDataItem]: A dictionary representing a record from the Salesforce sObject.
    """
    # Get all fields for the sobject
    desc = getattr(sf, sobject).describe()
    # Salesforce returns compound fields as separate fields, so we need to filter them out
    compound_fields = {
        f["compoundFieldName"]
        for f in desc["fields"]
        if f["compoundFieldName"] is not None
    } - {"Name"}

    # Fields of type "datetime" (Bulk API returns them as timestamps,
    # Standard API returns ISO strings). We normalize them later.
    date_fields = {
        f["name"] for f in desc["fields"] if f["type"] in ("datetime",) and f["name"]
    }

    # If no fields are specified, use all fields except compound fields
    fields = [f["name"] for f in desc["fields"] if f["name"] not in compound_fields]

    # Generate a predicate to filter records by the replication key
    predicate, order_by, n_records = "", "", 0
    if replication_key:
        if last_state:
            predicate = f"WHERE {replication_key} > {last_state}"
        order_by = f"ORDER BY {replication_key} ASC"
    query = f"SELECT {', '.join(fields)} FROM {sobject} {predicate} {order_by}"
    if not IS_PRODUCTION:
        query += " LIMIT 100"

    # Try Bulk API first, fallback to standard SOQL if not available
    try:
        for page in getattr(sf.bulk, sobject).query_all(query, lazy_operation=True):
            processed_page = [
                _process_record(record, date_fields, api_type="bulk") for record in page
            ]
            yield from processed_page
            n_records += len(processed_page)
    except SalesforceMalformedRequest as e:
        if "FeatureNotEnabled" in str(e) and "Async API not enabled" in str(e):
            # Fallback to standard SOQL API
            result = sf.query(query)
            while True:
                for record in result["records"]:
                    processed_record = _process_record(
                        record, date_fields, api_type="standard"
                    )
                    yield processed_record
                    n_records += 1

                # Check if there are more records to fetch
                if result["done"]:
                    break
                result = sf.query_more(result["nextRecordsUrl"])
        else:
            raise
