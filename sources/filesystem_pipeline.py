import dlt

from filesystem import google_drive, local_folder


def load_from_local_folder() -> None:
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name="local_folder",
        destination="duckdb",
        dataset_name="data_local_folder",
        full_refresh=True,
    )

    # use extensions to filter files as 'extensions=(".txt", ".pdf", ...)'
    data_resource = local_folder()
    # run the pipeline with your parameters
    load_info = pipeline.run(data_resource)
    # pretty print the information on data that was loaded
    print(load_info)


def load_from_google_drive() -> None:
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name="google_drive",
        destination="duckdb",
        dataset_name="data_google_drive",
        full_refresh=True,
    )

    # use extensions to filter files as 'extensions=(".txt", ".pdf", ...)'
    data_resource = google_drive(download=False, extensions=(".txt", ".pdf", ".jpg"))
    # run the pipeline with your parameters
    load_info = pipeline.run(data_resource)
    # pretty print the information on data that was loaded
    print(load_info)


if __name__ == "__main__":
    # load_from_local_folder()
    load_from_google_drive()
