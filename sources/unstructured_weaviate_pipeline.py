import dlt
from dlt.destinations.weaviate import weaviate_adapter

from unstructured_weaviate import pdf_to_text


def from_local_folder_to_structured(data_dir: str) -> None:
    from unstructured_data.local_folder import local_folder_resource

    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name="pdf_to_text_local_folder",
        destination="weaviate",
        dataset_name="data",
        full_refresh=True,
    )

    data_resource = local_folder_resource(data_dir)
    filtered_data_resource = data_resource.add_filter(
        lambda item: item["content_type"] == "application/pdf"
    ) | pdf_to_text(separate_pages=True)

    # use weaviate_adapter to tell destination to vectorize "text" column
    load_info = pipeline.run(
        weaviate_adapter(filtered_data_resource, vectorize="text")
    )
    # pretty print the information on data that was loaded
    row_counts = pipeline.last_trace
    print(row_counts)
    print("------")
    print(load_info)


def from_google_drive_to_structured() -> None:
    from unstructured_data.google_drive import google_drive_source

    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name="pdf_to_text_google_drive",
        destination="weaviate",
        dataset_name="data",
        full_refresh=True,
    )

    data_source = google_drive_source(
        download=True, filter_by_mime_type=("application/pdf",)
    )
    data_resource = data_source.resources["attachments"] | pdf_to_text(separate_pages=True)

    # use weaviate_adapter to tell destination to vectorize "text" column
    load_info = pipeline.run(
        weaviate_adapter(data_resource, vectorize="text")
    )
    # pretty print the information on data that was loaded
    print(load_info)


def from_inbox_to_structured() -> None:
    from unstructured_data.inbox import inbox_source

    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name="pdf_to_text_inbox",
        destination="weaviate",
        dataset_name="data",
        full_refresh=True,
    )

    data_source = inbox_source(
        attachments=True, filter_by_mime_type=("application/pdf",)
    )
    data_resource = data_source.resources["attachments"] | pdf_to_text(separate_pages=True)

    # use weaviate_adapter to tell destination to vectorize "text" column
    load_info = pipeline.run(
        weaviate_adapter(data_resource, vectorize="text")
    )
    # pretty print the information on data that was loaded
    row_counts = pipeline.last_trace.last_normalize_info
    print(row_counts)
    print("------")
    print(load_info)


if __name__ == "__main__":
    from_local_folder_to_structured(data_dir="")
    # from_google_drive_to_structured()
    # from_inbox_to_structured()
