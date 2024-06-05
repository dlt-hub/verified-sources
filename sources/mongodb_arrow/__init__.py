from typing import Optional

import dlt

from mongodb.helpers import client_from_credentials
from .helpers import collection_arrow_documents


@dlt.source
def mongodb_arrow_collection(
    connection_url: str = dlt.secrets.value,
    database: Optional[str] = dlt.config.value,
    collection: str = dlt.config.value,
    incremental: Optional[dlt.sources.incremental] = None,  # type: ignore[type-arg]
    write_disposition: Optional[str] = dlt.config.value,
    limit: Optional[int] = None,
    chunk_size: Optional[int] = 10000,
):
    """
    A DLT source which loads data from a mongo database
    using Apache Arrow for data types conversion.

    Args:
        connection_url (str): Database connection_url.
        database (Optional[str]): Selected database name, it
            will use the default database if not passed.
        collection (Optional[str]): A collection name to load.
        incremental (Optional[dlt.sources.incremental]): Option
            to enable incremental loading for the collection.
            E.g., `incremental=dlt.sources.incremental('updated_at',
            pendulum.parse('2022-01-01T00:00:00Z'))`
        write_disposition (str): Write disposition of the resource.
        limit (Optional[int]):
            The maximum number of documents to load. The limit is
            applied to each requested collection separately.
        chunk_size (Optional[int]): The number of documents load in
            each batch.

    Returns:
        DltResource: A DLT resource to load MongoDB collection.
    """
    client = client_from_credentials(connection_url)

    if not database:
        mongo_db = client.get_default_database()
    else:
        mongo_db = client[database]

    collection = mongo_db[collection]

    return dlt.resource(  # type: ignore
        collection_arrow_documents,
        name=collection.name,
        primary_key="_id",
        write_disposition=write_disposition,
    )(
        client,
        collection,
        incremental=incremental,
        limit=limit,
        chunk_size=chunk_size,
    )
