"""Source that loads collections form any a mongo database, supports incremental loads."""

from typing import Any, Dict, Iterable, List, Optional, Union, Mapping

import dlt
from dlt.common.data_writers import TDataItemFormat
from dlt.sources import DltResource
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext

from .helpers import (
    MongoDbCollectionConfiguration,
    MongoDbCollectionResourceConfiguration,
    TTypeMismatch,
    client_from_credentials,
    collection_documents,
)


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
    data_item_format: Optional[TDataItemFormat] = "object",
    on_inferred_type_mismatch: TTypeMismatch = "ignore",
) -> Iterable[DltResource]:
    """
    A DLT source which loads data from a mongo database using PyMongo.
    Resources are automatically created for each collection in the database or from the given list of collection.

    Args:
        connection_url (str): Database connection_url.
        database (Optional[str]): Selected database name, it will use the default database if not passed.
        collection_names (Optional[List[str]]): The list of collections `pymongo.collection.Collection` to load.
            All collections except `system.*` are loaded if not given.
        incremental (Optional[dlt.sources.incremental]): Option to enable incremental loading for the collection.
            E.g., `incremental=dlt.sources.incremental('updated_at', pendulum.parse('2022-01-01T00:00:00Z'))`
        write_disposition (str): Write disposition of the resource.
        parallel (Optional[bool]): Read `_id` partitions of the collection in parallel, as many at
            once as `workers` in the `extract` config: in threads in the object format, in
            processes in the Arrow format. Default is False.
        limit (Optional[int]):
            The maximum number of documents to load. The limit is
            applied to each requested collection separately.
        filter_ (Optional[Dict[str, Any]]): The filter to apply to the collection.
        projection: (Optional[Union[Mapping[str, Any], Iterable[str]]]): The projection to select fields of a collection
            when loading the collection. Supported inputs:
                include (list) - ["year", "title"]
                include (dict) - {"year": True, "title": True}
                exclude (dict) - {"released": False, "runtime": False}
            Note: Can't mix include and exclude statements '{"title": True, "released": False}`
        pymongoarrow_schema (pymongoarrow.schema.Schema): Mapping of expected field types of a collection to convert BSON to Arrow
        on_inferred_type_mismatch (TTypeMismatch): Applies when `pymongoarrow_schema` is not passed. The
            Arrow format then infers the type of a field from its first value in a batch and loads
            values of other types as null: "ignore" (default, fastest) does that silently, "warn" and
            "raise" detect such values with a second parse of each batch. Values that do not match
            `pymongoarrow_schema` always raise `FieldTypeMismatchError`.
        chunk_size (Optional[int]): The number of documents load in each batch.
        data_item_format (Optional[TDataItemFormat]): The data format to use for loading.
            Supported formats:
                object - Python objects (dicts, lists).
                arrow - Apache Arrow tables. Nested documents are flattened into columns up to
                    `max_table_nesting` like in the object format, arrays load as json.

    Returns:
        Iterable[DltResource]: A list of DLT resources for each collection to be loaded.
    """

    # set up mongo client
    client = client_from_credentials(connection_url)
    if not database:
        mongo_database = client.get_default_database()
    else:
        mongo_database = client[database]

    # use provided collection or all conllections
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
            data_item_format=data_item_format,
            filter_=filter_ or {},
            projection=projection,
            pymongoarrow_schema=pymongoarrow_schema,
            on_inferred_type_mismatch=on_inferred_type_mismatch,
            connection_url=connection_url,
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
    data_item_format: Optional[TDataItemFormat] = "object",
    filter_: Optional[Dict[str, Any]] = None,
    projection: Optional[Union[Mapping[str, Any], Iterable[str]]] = dlt.config.value,
    pymongoarrow_schema: Optional[Any] = None,
    on_inferred_type_mismatch: TTypeMismatch = "ignore",
) -> DltResource:
    """
    A DLT source which loads a collection from a mongo database using PyMongo.

    Args:
        connection_url (str): Database connection_url.
        database (Optional[str]): Selected database name, it will use the default database if not passed.
        collection (str): The collection name to load.
        incremental (Optional[dlt.sources.incremental]): Option to enable incremental loading for the collection.
            E.g., `incremental=dlt.sources.incremental('updated_at', pendulum.parse('2022-01-01T00:00:00Z'))`
        write_disposition (str): Write disposition of the resource.
        parallel (Optional[bool]): Read `_id` partitions of the collection in parallel, as many at
            once as `workers` in the `extract` config: in threads in the object format, in
            processes in the Arrow format. Default is False.
        limit (Optional[int]): The number of documents load.
        chunk_size (Optional[int]): The number of documents load in each batch.
        data_item_format (Optional[TDataItemFormat]): The data format to use for loading.
            Supported formats:
                object - Python objects (dicts, lists).
                arrow - Apache Arrow tables. Nested documents are flattened into columns up to
                    `max_table_nesting` like in the object format, arrays load as json.
        filter_ (Optional[Dict[str, Any]]): The filter to apply to the collection.
        projection: (Optional[Union[Mapping[str, Any], Iterable[str]]]): The projection to select fields
            when loading the collection. Supported inputs:
                include (list) - ["year", "title"]
                include (dict) - {"year": True, "title": True}
                exclude (dict) - {"released": False, "runtime": False}
            Note: Can't mix include and exclude statements '{"title": True, "released": False}`
        pymongoarrow_schema (pymongoarrow.schema.Schema): Mapping of expected field types to convert BSON to Arrow
        on_inferred_type_mismatch (TTypeMismatch): Applies when `pymongoarrow_schema` is not passed. The
            Arrow format then infers the type of a field from its first value in a batch and loads
            values of other types as null: "ignore" (default, fastest) does that silently, "warn" and
            "raise" detect such values with a second parse of each batch. Values that do not match
            `pymongoarrow_schema` always raise `FieldTypeMismatchError`.

    Returns:
        DltResource: The resource loading the collection.
    """
    # set up mongo client
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
        data_item_format=data_item_format,
        filter_=filter_ or {},
        projection=projection,
        pymongoarrow_schema=pymongoarrow_schema,
        on_inferred_type_mismatch=on_inferred_type_mismatch,
        connection_url=connection_url,
    )
