import dlt
from unstructured_data import unstructured_source, filesystem_source


if __name__ == "__main__":
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name='unstructured', destination='duckdb', dataset_name='unstructured_data'
    )

    data = filesystem_source().with_resources("local_folder")

    queries = {
        "recipient_company_name": "Who is the recipient of the invoice? Just return the name",
        "invoice_amount": "What is the total amount of the invoice? Just return the amount as decimal number, no currency or text",
        "invoice_date": "What is the date of the invoice? Just return the date",
        "invoice_number": "What is the invoice number? Just return the number",
        "service_description": "What is the description of the service that this invoice is for? Just return the description",
    }
    for resource_name, resource in data.selected_resources.items():
        data_extractor = unstructured_source(resource, queries)
        # run the pipeline with your parameters
        load_info = pipeline.run(data_extractor)

        # pretty print the information on data that was loaded
        print(load_info)
