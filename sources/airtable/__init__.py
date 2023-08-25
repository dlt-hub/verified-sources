"""Source that loads tables form Airtable.
Supports whitelisting of tables or loading of all tables from a specified base.
Supported write dispositions: replace
"""
from typing import Optional, Iterable, Iterator, List, Dict, Any

import dlt
from dlt.extract.source import DltResource
from dlt.common.typing import TDataItem

import pyairtable


@dlt.source
def airtable_source(
    base_id: str,
    table_ids: Optional[List[str]] = None,
    table_names: Optional[List[str]] = None,
    access_token: str = dlt.secrets.value,
) -> Iterable[DltResource]:
    """
    Represents tables for a single Airtable base.
    Args:
        base_id (str): The id of the base. Obtain it e.g. from the URL in your webbrowser. It starts with "app". See https://support.airtable.com/docs/finding-airtable-ids
        table_ids (Optional[List[str]]): A list of table ids to load. By default, all tables in the schema are loaded. Starts with "tbl". See https://support.airtable.com/docs/finding-airtable-ids
        table_names (Optional[List[str]]): A list of table names to load. By default, all tables in the schema are loaded. Table names can change and thus filtering on names is less reliable than on ids.
        access_token (str): The personal access token. See https://support.airtable.com/docs/creating-and-using-api-keys-and-access-tokens#personal-access-tokens-basic-actions
    """
    api = pyairtable.Api(access_token)
    all_tables_url = api.build_url(f"meta/bases/{base_id}/tables")
    tables = api.request(method="GET", url=all_tables_url).get("tables")
    for t in tables:
        if table_ids:
            if t.get("id") in table_ids:
                yield airtable_resource(access_token, base_id, t)
        elif table_names:
            if t.get("name") in table_names:
                yield airtable_resource(access_token, base_id, t)
        else:
            yield airtable_resource(access_token, base_id, t)


def airtable_resource(
    access_token: str,
    base_id: str,
    table: Dict[str, Any],
) -> DltResource:
    """
    Represents a single airtable.
    Args:
        base_id (str): The id of the base. Obtain it e.g. from the URL in your webbrowser. It starts with "app". See https://support.airtable.com/docs/finding-airtable-ids
        access_token (str): The personal access token. See https://support.airtable.com/docs/creating-and-using-api-keys-and-access-tokens#personal-access-tokens-basic-actions
        table (TDataItem): The iterable created by pyairtable representing the data of an airtable
    """
    primary_key_id = table["primaryFieldId"]
    primary_key_field = [
        field for field in table["fields"] if field["id"] == primary_key_id
    ][0]
    table_name: str = table["name"]
    primary_key: List[str] = [primary_key_field["name"]]
    air_table = pyairtable.Table(access_token, base_id, table["id"])
    air_table_generator: Iterator[List[Any]] = air_table.iterate()
    return dlt.resource(
        air_table_generator,
        name=table_name,
        primary_key=primary_key,
        write_disposition="replace",  # using a typed parameter crashes the typing
    )
