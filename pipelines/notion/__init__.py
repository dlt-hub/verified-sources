"""A pipeline that extracts data from Notion API"""

from typing import List, Dict, Optional, Iterator

import dlt
from dlt.extract.source import DltResource

from .client import NotionClient
from .database import NotionDatabase


@dlt.source
def notion_databases(
    database_ids: Optional[List[Dict[str, str]]] = None,
    api_key: str = dlt.secrets.value,
) -> Iterator[DltResource]:
    """A source that generates specific databases or all databases in a Notion
    workspace, based on the provided arguments.

    Args:
        database_ids (List[Dict[str, str]], optional): A list of dictionaries
        each containing a database id and a name.
        Defaults to None. If None, the function will generate all databases
        in the workspace that are accessible to the integration.
        api_key (str): The Notion API secret key.
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
        notion_database = NotionDatabase(database["id"], notion_client)
        yield dlt.resource(
            notion_database.query(),
            primary_key="id",
            name=database["use_name"],
            write_disposition="replace",
        )


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="notion",
        destination="duckdb",
        dataset_name="notion_data",
    )

    data = notion_databases()

    info = pipeline.run(data)
    print(info)
