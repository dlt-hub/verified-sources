import dlt
from dlt.common import pendulum
from dlt.common.data_writers import TDataItemFormat
from dlt.common.pipeline import LoadInfo
from dlt.common.typing import TDataItems
from dlt.pipeline.pipeline import Pipeline

# As this pipeline can be run as standalone script or as part of the tests, we need to handle the import differently.
try:
    from .mongodb import mongodb, mongodb_collection  # type: ignore
except ImportError:
    from mongodb import mongodb, mongodb_collection


def load_select_collection_db(pipeline: Pipeline = None) -> LoadInfo:
    """Use the mongodb source to reflect an entire database schema and load select tables from it.

    This example sources data from a sample mongo database data from [mongodb-sample-dataset](https://github.com/neelabalan/mongodb-sample-dataset).
    """
    if pipeline is None:
        # Create a pipeline
        pipeline = dlt.pipeline(
            pipeline_name="local_mongo",
            destination="duckdb",
            dataset_name="mongo_select",
        )

    # Configure the source to load a few select collections incrementally
    mflix = mongodb(incremental=dlt.sources.incremental("date")).with_resources(
        "comments"
    )

    # Run the pipeline. The merge write disposition merges existing rows in the destination by primary key
    info = pipeline.run(mflix, write_disposition="merge")

    return info


def load_select_collection_db_items(parallel: bool = False) -> TDataItems:
    """Get the items from a mongo collection in parallel or not and return a list of records"""
    comments = mongodb(
        incremental=dlt.sources.incremental("date"), parallel=parallel
    ).with_resources("comments")
    return list(comments)


def load_select_collection_db_items_parallel(
    data_item_format: TDataItemFormat, parallel: bool = False
) -> TDataItems:
    comments = mongodb_collection(
        incremental=dlt.sources.incremental("date"),
        parallel=parallel,
        data_item_format=data_item_format,
        collection="comments",
    )
    return list(comments)


def load_select_collection_db_filtered(pipeline: Pipeline = None) -> LoadInfo:
    """Use the mongodb source to reflect an entire database schema and load select tables from it.

    This example sources data from a sample mongo database data from [mongodb-sample-dataset](https://github.com/neelabalan/mongodb-sample-dataset).
    """
    if pipeline is None:
        # Create a pipeline
        pipeline = dlt.pipeline(
            pipeline_name="local_mongo",
            destination="duckdb",
            dataset_name="mongo_select_incremental",
        )

    # Configure the source to load a few select collections incrementally
    movies = mongodb_collection(
        collection="movies",
        incremental=dlt.sources.incremental(
            "lastupdated", initial_value=pendulum.DateTime(2016, 1, 1, 0, 0, 0)
        ),
    )

    # Run the pipeline. The merge write disposition merges existing rows in the destination by primary key
    info = pipeline.run(movies, write_disposition="merge")

    return info


def load_select_collection_hint_db(pipeline: Pipeline = None) -> LoadInfo:
    """Use the mongodb source to reflect an entire database schema and load select tables from it.

    This example sources data from a sample mongo database data from [mongodb-sample-dataset](https://github.com/neelabalan/mongodb-sample-dataset).
    """
    if pipeline is None:
        # Create a pipeline
        pipeline = dlt.pipeline(
            pipeline_name="local_mongo",
            destination="duckdb",
            dataset_name="mongo_select_hint",
        )

    # Load a table incrementally with append write disposition
    # this is good when a table only has new rows inserted, but not updated
    airbnb = mongodb().with_resources("listingsAndReviews")
    airbnb.listingsAndReviews.apply_hints(
        incremental=dlt.sources.incremental("last_scraped")
    )

    info = pipeline.run(airbnb, write_disposition="append")

    return info


def load_entire_database(pipeline: Pipeline = None) -> LoadInfo:
    """Use the mongo source to completely load all collection in a database"""
    if pipeline is None:
        # Create a pipeline
        pipeline = dlt.pipeline(
            pipeline_name="local_mongo",
            destination="duckdb",
            dataset_name="mongo_database",
        )

    # By default the mongo source reflects all collections in the database
    source = mongodb()

    # Run the pipeline. For a large db this may take a while
    info = pipeline.run(source, write_disposition="replace")

    return info


def load_collection_with_arrow(pipeline: Pipeline = None) -> LoadInfo:
    """
    Load a MongoDB collection, using Apache
    Error as the data processor.
    """
    if pipeline is None:
        # Create a pipeline
        pipeline = dlt.pipeline(
            pipeline_name="local_mongo",
            destination="postgres",
            dataset_name="mongo_select_incremental",
            full_refresh=True,
        )

    # Configure the source to load data with Arrow
    comments = mongodb_collection(
        collection="comments",
        incremental=dlt.sources.incremental(
            "date",
            initial_value=pendulum.DateTime(
                2005, 1, 1, tzinfo=pendulum.timezone("UTC")
            ),
            end_value=pendulum.DateTime(2005, 6, 1, tzinfo=pendulum.timezone("UTC")),
        ),
        data_item_format="arrow",
    )

    info = pipeline.run(comments)
    return info


if __name__ == "__main__":
    # Credentials for the sample database.
    # Load selected tables with different settings
    print(load_select_collection_db())
    # print(load_select_collection_db_filtered())

    # Load all tables from the database.
    # Warning: The sample database is large
    # print(load_entire_database())

    # Load data with Apache Arrow.
    # print(load_collection_with_arrow())
