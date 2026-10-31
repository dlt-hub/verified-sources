"""Loads collections of a MongoDB database, also incrementally."""

from typing import Any, Dict, Iterable, List, Optional, Union, Mapping

import dlt
from dlt.common import logger
from dlt.common.data_writers import TDataItemFormat
from dlt.sources import DltResource

from .helpers import (
    MongoDbCollectionConfiguration,
    MongoDbCollectionResourceConfiguration,
    TBackend,
    TTypeMismatch,
    client_from_credentials,
    collection_documents,
)
from .partition import DEFAULT_PARTITION_SIZE_BYTES


@dlt.source
def mongodb(
    connection_url: str = dlt.secrets.value,
    database: Optional[str] = dlt.config.value,
    collection_names: Optional[List[str]] = dlt.config.value,
    incremental: Optional[dlt.sources.incremental] = None,  # type: ignore[type-arg]
    write_disposition: Optional[str] = dlt.config.value,
    parallel: Optional[bool] = dlt.config.value,
    limit: Optional[int] = None,
    filter_: Optional[Dict[str, Any]] = None,
    projection: Optional[Union[Mapping[str, Any], Iterable[str]]] = None,
    pymongoarrow_schema: Optional[Any] = None,
    chunk_size: Optional[int] = 10000,
    backend: TBackend = "python",
    on_inferred_type_mismatch: TTypeMismatch = "ignore",
    partition_size_bytes: int = DEFAULT_PARTITION_SIZE_BYTES,
) -> Iterable[DltResource]:
    """Loads collections of a MongoDB database, one resource per collection.

    Args:
        connection_url (str): Connection URL of the database.
        database (Optional[str]): Database name. The default database of `connection_url` if
            not set.
        collection_names (Optional[List[str]]): Collections to load. All collections except
            `system.*` if not set.
        incremental (Optional[dlt.sources.incremental]): Incremental of all collections, for
            example `dlt.sources.incremental("updated_at")`.
        write_disposition (Optional[str]): Write disposition of all resources.
        parallel (Optional[bool]): Reads `_id` partitions of a collection in parallel, as many
            as `workers` in the `extract` config.
        limit (Optional[int]): Maximum number of documents loaded from each collection.
        filter_ (Optional[Dict[str, Any]]): Filter of the documents.
        projection (Optional[Union[Mapping[str, Any], Iterable[str]]]): List of fields to load
            or a MongoDB projection. The cursor field of `incremental` is always loaded.
        pymongoarrow_schema (Optional[Any]): `pymongoarrow.schema.Schema` with the field types of
            the "pyarrow" backend. Fields not in the schema are not loaded.
        chunk_size (Optional[int]): Number of documents in a batch. `parallel` reads load each
            partition as one batch.
        backend (TBackend): "python" yields lists of dicts, "pyarrow" yields Arrow tables and
            requires `pymongoarrow`.
        on_inferred_type_mismatch (TTypeMismatch): Without `pymongoarrow_schema`, the "pyarrow"
            backend converts values to the type of the first value of their field and loads
            other values as null. "ignore" does this silently, "warn" logs a warning, "raise"
            raises `FieldTypeMismatchError`. "warn" and "raise" parse each batch twice.
        partition_size_bytes (int): Maximum size of a partition of `parallel` reads.

    Returns:
        Iterable[DltResource]: Resources of the collections.
    """
    client = client_from_credentials(connection_url)
    if not database:
        mongo_database = client.get_default_database()
    else:
        mongo_database = client[database]

    if not collection_names:
        collection_names = [
            name
            for name in mongo_database.list_collection_names()
            if not name.startswith("system.")
        ]

    collection_list = [mongo_database[collection] for collection in collection_names]

    for collection in collection_list:
        yield dlt.resource(  # type: ignore
            collection_documents,
            name=collection.name,
            primary_key="_id",
            write_disposition=write_disposition,
            spec=MongoDbCollectionConfiguration,
        )(
            client,
            collection,
            incremental=incremental,
            parallel=parallel,
            limit=limit,
            chunk_size=chunk_size,
            backend=backend,
            filter_=filter_ or {},
            projection=projection,
            pymongoarrow_schema=pymongoarrow_schema,
            on_inferred_type_mismatch=on_inferred_type_mismatch,
            connection_url=connection_url,
            partition_size_bytes=partition_size_bytes,
        )


@dlt.resource(
    name=lambda args: args["collection"],
    standalone=True,
    spec=MongoDbCollectionResourceConfiguration,
)
def mongodb_collection(
    connection_url: str = dlt.secrets.value,
    database: Optional[str] = dlt.config.value,
    collection: str = dlt.config.value,
    incremental: Optional[dlt.sources.incremental] = None,  # type: ignore[type-arg]
    write_disposition: Optional[str] = dlt.config.value,
    parallel: Optional[bool] = False,
    limit: Optional[int] = None,
    chunk_size: Optional[int] = 10000,
    backend: TBackend = "python",
    filter_: Optional[Dict[str, Any]] = None,
    projection: Optional[Union[Mapping[str, Any], Iterable[str]]] = dlt.config.value,
    pymongoarrow_schema: Optional[Any] = None,
    on_inferred_type_mismatch: TTypeMismatch = "ignore",
    partition_size_bytes: int = DEFAULT_PARTITION_SIZE_BYTES,
    data_item_format: Optional[TDataItemFormat] = None,
) -> DltResource:
    """Loads a collection of a MongoDB database.

    Args:
        connection_url (str): Connection URL of the database.
        database (Optional[str]): Database name. The default database of `connection_url` if
            not set.
        collection (str): Collection to load.
        incremental (Optional[dlt.sources.incremental]): Incremental of the collection, for
            example `dlt.sources.incremental("updated_at")`.
        write_disposition (Optional[str]): Write disposition of the resource.
        parallel (Optional[bool]): Reads `_id` partitions of the collection in parallel, as many
            as `workers` in the `extract` config.
        limit (Optional[int]): Maximum number of documents to load.
        chunk_size (Optional[int]): Number of documents in a batch. `parallel` reads load each
            partition as one batch.
        backend (TBackend): "python" yields lists of dicts, "pyarrow" yields Arrow tables and
            requires `pymongoarrow`.
        filter_ (Optional[Dict[str, Any]]): Filter of the documents.
        projection (Optional[Union[Mapping[str, Any], Iterable[str]]]): List of fields to load
            or a MongoDB projection. The cursor field of `incremental` is always loaded.
        pymongoarrow_schema (Optional[Any]): `pymongoarrow.schema.Schema` with the field types of
            the "pyarrow" backend. Fields not in the schema are not loaded.
        on_inferred_type_mismatch (TTypeMismatch): Without `pymongoarrow_schema`, the "pyarrow"
            backend converts values to the type of the first value of their field and loads
            other values as null. "ignore" does this silently, "warn" logs a warning, "raise"
            raises `FieldTypeMismatchError`. "warn" and "raise" parse each batch twice.
        partition_size_bytes (int): Maximum size of a partition of `parallel` reads.
        data_item_format (Optional[TDataItemFormat]): Deprecated. "object" is the "python"
            backend and "arrow" is the "pyarrow" backend.

    Returns:
        DltResource: The resource of the collection.
    """
    if data_item_format:
        logger.warning(
            '`data_item_format` is deprecated. Use `backend="python"` or `backend="pyarrow"`.'
        )
        backend = "pyarrow" if data_item_format == "arrow" else "python"

    client = client_from_credentials(connection_url)
    if not database:
        mongo_database = client.get_default_database()
    else:
        mongo_database = client[database]

    collection_obj = mongo_database[collection]

    return dlt.resource(  # type: ignore[call-overload,no-any-return]
        collection_documents,
        name=collection_obj.name,
        primary_key="_id",
        write_disposition=write_disposition,
    )(
        client,
        collection_obj,
        incremental=incremental,
        parallel=parallel,
        limit=limit,
        chunk_size=chunk_size,
        backend=backend,
        filter_=filter_ or {},
        projection=projection,
        pymongoarrow_schema=pymongoarrow_schema,
        on_inferred_type_mismatch=on_inferred_type_mismatch,
        connection_url=connection_url,
        partition_size_bytes=partition_size_bytes,
    )
