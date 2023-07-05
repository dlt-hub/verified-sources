from typing import Dict

import dlt
from filesystem import google_drive, local_folder
from unstructured_data import unstructured_to_structured_source


def from_local_folder_to_structured(queries: Dict[str, str]) -> None:
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name="unstructured_local_folder",
        destination="duckdb",
        dataset_name="unstructured_data_local_folder",
        full_refresh=True,
    )

    # use extensions to filter files as 'extensions=(".txt", ".pdf", ...)'
    data_resource = local_folder(extensions=(".txt", ".pdf"))
    data_extractor = unstructured_to_structured_source(data_resource, queries)
    # run the pipeline with your parameters
    load_info = pipeline.run(data_extractor)
    # pretty print the information on data that was loaded
    print(load_info)


def from_google_drive_to_structured(queries: Dict[str, str]) -> None:
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name="unstructured_google_drive",
        destination="duckdb",
        dataset_name="unstructured_data_google_drive",
        full_refresh=True,
    )

    # use extensions to filter files as 'extensions=(".txt", ".pdf", ...)'
    data_resource = google_drive(download=True, extensions=(".txt", ".pdf", ".jpg"))
    data_extractor = unstructured_to_structured_source(data_resource, queries)
    # run the pipeline with your parameters
    load_info = pipeline.run(data_extractor)
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
    from_local_folder_to_structured(queries)
    # from_google_drive_to_structured(queries)
