from typing import Dict

import dlt
from unstructured_data import unstructured_to_structured_resource


def from_local_folder_to_structured(data_dir: str, queries: Dict[str, str]) -> None:
    from unstructured_data.local_folder import local_folder_source
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name="unstructured_local_folder",
        destination="duckdb",
        dataset_name="unstructured_data_local_folder",
        full_refresh=True,
    )

    # use extensions to filter files as 'extensions=(".txt", ".pdf", ...)'
    data_resource = local_folder_source(data_dir, extensions=(".txt", ".pdf"))
    # run the pipeline with your parameters
    load_info = pipeline.run(
        data_resource
        | unstructured_to_structured_resource(
            queries,
            table_name=f"unstructured_from_{data_resource.name}",
            run_async=False,
        )
    )
    # pretty print the information on data that was loaded
    print(load_info)


def from_google_drive_to_structured(queries: Dict[str, str]) -> None:
    from unstructured_data.google_drive import google_drive_source
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name="unstructured_google_drive",
        destination="duckdb",
        dataset_name="unstructured_data_google_drive",
        full_refresh=True,
    )

    # use extensions to filter files as 'extensions=(".txt", ".pdf", ...)'
    data_resource = google_drive_source(download=True, extensions=(".txt", ".pdf"))
    # run the pipeline with your parameters
    load_info = pipeline.run(
        data_resource
        | unstructured_to_structured_resource(
            queries, table_name=f"unstructured_from_{data_resource.name}"
        )
    )
    # pretty print the information on data that was loaded
    print(load_info)


def from_inbox_to_structured() -> None:
    from unstructured_data.inbox import inbox_source
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name="unstructured_inbox",
        destination="duckdb",
        dataset_name="unstructured_inbox_data",
        full_refresh=True,
    )

    data_source = inbox_source(attachments=True)
    data_resource = data_source.resources["attachments"].add_filter(
        lambda item: item["content_type"] == "application/pdf"
    )
    # run the pipeline with your parameters
    load_info = pipeline.run(
        data_resource
        | unstructured_to_structured_resource(
            table_name=f"unstructured_from_{data_resource.name}"
        )
    )
    # pretty print the information on data that was loaded
    print(load_info)


if __name__ == "__main__":
    # from_local_folder_to_structured(data_dir=".", queries=queries)
    # from_google_drive_to_structured(queries)
    from_inbox_to_structured()
