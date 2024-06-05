from pendulum import timezone

import dlt
from dlt.common import pendulum
from dlt.common.pipeline import LoadInfo
from dlt.common.typing import TDataItems
from dlt.pipeline.pipeline import Pipeline


# As this pipeline can be run as standalone script or as part of the tests, we need to handle the import differently.
try:
    from .mongodb_arrow import mongodb_arrow_collection  # type: ignore
except ImportError:
    from mongodb_arrow import mongodb_arrow_collection


def load_select_collection_db_filtered_arrow(pipeline: Pipeline = None) -> LoadInfo:
    """Use the mongodb source to reflect an entire database schema and load select tables from it.

    This example sources data from a sample mongo database data from [mongodb-sample-dataset](https://github.com/neelabalan/mongodb-sample-dataset).
    """
    if pipeline is None:
        # Create a pipeline
        pipeline = dlt.pipeline(
            pipeline_name="local_mongo",
            destination="postgres",
            dataset_name="mongo_select_incremental",
            full_refresh=True,
        )

    # Configure the source to load a few select collections incrementally
    comments = mongodb_arrow_collection(
        collection="comments",
        incremental=dlt.sources.incremental(
            "date",
            initial_value=pendulum.DateTime(2005, 1, 1, tzinfo=timezone("UTC")),
            end_value=pendulum.DateTime(2005, 6, 1, tzinfo=timezone("UTC")),
        ),
    )

    info = pipeline.run(comments)
    return info


if __name__ == "__main__":
    print(load_select_collection_db_filtered_arrow())
