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


def from_inbox(queries: Dict[str, str]) -> None:
    from unstructured_data.inbox import inbox_source
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name="unstructured_inbox",
        destination="duckdb",
        dataset_name="unstructured_inbox_data",
        full_refresh=True,
    )

    data_source = inbox_source(attachments=True)
    data_resource = data_source.resources["attachments"]
    # run the pipeline with your parameters
    load_info = pipeline.run(
        data_resource
        | unstructured_to_structured_resource(
            queries, table_name=f"unstructured_from_{data_resource.name}"
        )
    )
    # pretty print the information on data that was loaded
    print(load_info)


if __name__ == "__main__":
    queries = {
        "recipient_company_name": "Who is the recipient of the invoice? Just return the name. If you don't know, then return None",
        "invoice_amount": "What is the total amount of the invoice? Just return the amount as decimal number, no currency or text. If you don't know, then return None",
        "invoice_date": "What is the date of the invoice? Just return the date. If you don't know, then return None",
        "invoice_number": "What is the invoice number? Just return the number. If you don't know, then return None",
        "service_description": "What is the description of the service that this invoice is for? Just return the description. If you don't know, then return None",
        "phone_number": "What is the company phone number? Just return the phone number. If you don't know, then return None",
    }
    # from_local_folder_to_structured(data_dir=".", queries=queries)
    # from_google_drive_to_structured(queries)
    from_inbox(queries)
