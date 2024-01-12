import dlt

try:
    from .csv_reader import csv_reader  # type: ignore
except ImportError:
    from csv_reader import csv_reader


def csv_with_duck_db():
    pipeline = dlt.pipeline(
        pipeline_name="csv_to_duckdb",
        destination="postgres",
        dataset_name="files",
    )

    res = csv_reader("/home/ilya/test_files/", ("*ddb*.csv",))
    load_info = pipeline.run(res)
    print(load_info)


if __name__ == "__main__":
    csv_with_duck_db()
