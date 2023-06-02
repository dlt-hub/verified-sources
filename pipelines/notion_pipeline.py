import dlt

from notion import notion_databases


def load_databases():
    """Loads all databases from a Notion workspace which have been shared with
    an integration.
    """
    pipeline = dlt.pipeline(
        pipeline_name="notion",
        destination="duckdb",
        dataset_name="notion_data",
    )

    data = notion_databases()

    info = pipeline.run(data)
    print(info)


if __name__ == "__main__":
    load_databases()
