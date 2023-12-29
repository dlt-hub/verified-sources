from typing import Any, Dict, Iterable, Optional

from dlt.common.typing import TDataItem

from ...api_client import APIClient


class NotionDatabase:
    """
    A class to represent a Notion database.

    Attributes:
        database_id (str): The ID of the Notion database.
        notion_client (NotionClient): A client to interact with the Notion API.
    """

    def __init__(self, database_id: str, notion_client: APIClient):
        self.database_id = database_id
        self.notion_client = notion_client

    def get_structure(self) -> Any:
        """Retrieves the structure of the database.

        Notion API Reference. Retrieve a database:
            https://developers.notion.com/reference/retrieve-a-database

        Returns:
            Any: The structure of the database.
        """
        return self.notion_client.get(f"databases/{self.database_id}")

    def query(
        self,
        filter_properties: Optional[Dict[str, Any]] = None,
        filter_criteria: Optional[Dict[str, Any]] = None,
        sorts: Optional[Dict[str, Any]] = None,
        start_cursor: Optional[str] = None,
        page_size: Optional[int] = None,
    ) -> Iterable[TDataItem]:
        """Queries the database for records.

        Notion API Reference. Query a database:
            https://developers.notion.com/reference/post-database-query

        Args:
            filter_properties (Dict[str, Any], optional): A dictionary of
                properties to filter the records by. Defaults to None.
            filter_criteria (Dict[str, Any], optional): A dictionary of filters
                to apply to the records. Defaults to None.
            sorts (Dict[str, Any], optional): A dictionary of sorts to apply
                to the records. Defaults to None.
            start_cursor (str, optional): The cursor to start the query at.
                Defaults to None.
            page_size (int, optional): The number of records to return.
                Defaults to None.

        Yields:
            List[Dict[str, Any]]: A record from the database.
        """
        payload = {
            "filter": filter_criteria,
            "sorts": sorts,
            "start_cursor": start_cursor,
            "page_size": page_size,
        }

        filtered_payload = {k: v for k, v in payload.items() if v is not None}

        return self.notion_client.paginate(
            f"databases/{self.database_id}/query",
            params=filter_properties,
            json=filtered_payload,
            method="post",
        )
