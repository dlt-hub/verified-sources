"""A source that extracts data from Notion API"""

from typing import List, Dict, Optional, Iterator

import dlt
from dlt.common.typing import TDataItems
from dlt.sources import DltResource

from .helpers.client import NotionClient
from .helpers.database import NotionDatabase


@dlt.resource
def notion_pages(
    page_ids: Optional[List[str]] = None, api_key: str = dlt.secrets.value
) -> Iterator[TDataItems]:
    """
    Retrieves pages from Notion.

    Args:
        page_ids (Optional[List[str]]): A list of page ids.
            Defaults to None. If None, the function will generate all pages
            in the workspace that are accessible to the integration.
        api_key (str): The Notion API secret key.

    Yields:
        Iterator[TDataItems]: Pages from Notion.
    """
    client = NotionClient(api_key)
    pages = client.search(filter_criteria={"value": "page", "property": "object"})
    for page in pages:
        if page_ids and page["id"] not in page_ids:
            continue
        yield page


@dlt.source
def notion_databases(
    database_ids: Optional[List[Dict[str, str]]] = None,
    api_key: str = dlt.secrets.value,
) -> Iterator[DltResource]:
    """
    Retrieves data from Notion databases.

    Args:
        database_ids (List[Dict[str, str]], optional): A list of dictionaries
            each containing a database id and a name.
            Defaults to None. If None, the function will generate all databases
            in the workspace that are accessible to the integration.
        api_key (str): The Notion API secret key.

    Yields:
        DltResource: Data resources from Notion databases.
    """
    notion_client = NotionClient(api_key)

    if database_ids is None:
        search_results = notion_client.search(
            filter_criteria={"value": "database", "property": "object"}
        )
        database_ids = [
            {"id": result["id"], "use_name": result["title"][0]["plain_text"]}
            for result in search_results
        ]

    for database in database_ids:
        if "use_name" not in database:
            # Fetch the database details from Notion
            details = notion_client.get_database(database["id"])

            # Extract the name/title from the details
            database["use_name"] = details["title"][0]["plain_text"]

        notion_database = NotionDatabase(database["id"], notion_client)
        yield dlt.resource(  # type: ignore
            notion_database.query(),
            primary_key="id",
            name=database["use_name"],
            write_disposition="replace",
        )
