"""Very simple pipeline, to be used as a starting point for pandas or arrow pipelines.

"""

import dlt
from arrow_pandas.example_resources import orders, customers


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        "orders_pipeline", destination="duckdb", dataset_name="orders_dataset"
    )
    # run both resources
    pipeline.run([orders, customers])
