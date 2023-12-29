"""A source that extracts data from Notion API"""

from typing import List, Dict, Optional, Iterator

import dlt
from dlt.sources import DltResource
from ..api_client import APIClient, BearerTokenAuth
from .settings import API_URL, DEFAULT_HEADERS
from .helpers.paginator import NotionPaginator

from .helpers.client import NotionClient
from .helpers.database import NotionDatabase


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
    notion_client = APIClient(
        base_url=API_URL,
        headers=DEFAULT_HEADERS,
        auth=BearerTokenAuth(api_key),
        paginator=NotionPaginator(),
    )

    if database_ids is None:
        search_results = notion_client.paginate(
            "/search",
            json={"filter": {"value": "database", "property": "object"}},
            method="post",
        )

        database_ids = [
            {"id": result["id"], "use_name": result["title"][0]["plain_text"]}
            for page in search_results
            for result in page
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
