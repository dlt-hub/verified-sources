import dlt

from csv_reader import csv_reader


def csv_with_duck_db():
    pipeline = dlt.pipeline(
        pipeline_name="csv_to_duckdb",
        destination="postgres",
        dataset_name="files",
        full_refresh=True,
    )

    reader = csv_reader("protocol:///bucket_url", ("*file*.csv",))
    load_info = pipeline.run(reader)
    print(load_info)


def csv_with_duck_db_hints():
    pipeline = dlt.pipeline(
        pipeline_name="csv_to_duckdb",
        destination="postgres",
        dataset_name="files",
        full_refresh=True,
    )

    reader = csv_reader("protocol:///bucket_url", ("*file*.csv",))
    reader.resources["read_location"].apply_hints(primary_key="col1")
    load_info = pipeline.run(reader)
    print(load_info)


if __name__ == "__main__":
    # csv_with_duck_db()
    csv_with_duck_db_hints()
