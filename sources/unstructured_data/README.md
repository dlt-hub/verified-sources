# Unstructured data to structured
 Converts unstructured data from a specified data resource to structured data using provided queries.

## Prerequisites

- Python 3.x
- `dlt` library (you can install it using `pip install dlt`)
- destination dependencies, e.g. `duckdb` (`pip install duckdb`)

## Installation

Make sure you have Python 3.x installed on your system.

Install the required library by running the following command:

```shell
pip install dlt[duckdb]
```

## Init the pipeline
```sh
dlt init unstructured_data duckdb --branch features/ustructured-source
```

## Install requirements

```shell
pip install -r requirements.txt
```

## Set credentials
1. Open `.dlt/secrets.toml`.
2. Enter the OpenAI secrets:

    ```toml
    [sources.unstructured_data]
    openai_api_key = "openai_api_key"
    ```

## Configure unstructured source
### Define queries
You must provide a dictionary of queries to be applied to the unstructured
data during processing. Each query maps a field name to a query string
that specifies how to process the field.

Queries example:
```python
queries = {
    "recipient_company_name": "Who is the recipient of the invoice? Just return the name. If you don't know, then return None",
    "invoice_amount": "What is the total amount of the invoice? Just return the amount as decimal number, no currency or text. If you don't know, then return None",
    "invoice_date": "What is the date of the invoice? Just return the date. If you don't know, then return None",
    "invoice_number": "What is the invoice number? Just return the number. If you don't know, then return None",
    "service_description": "What is the description of the service that this invoice is for? Just return the description. If you don't know, then return None",
    "phone_number": "What is the company phone number? Just return the phone number. If you don't know, then return None",
}
```

Customize the INVOICE_QUERIES dict in the `unstructured_data/settings.py` file if you want to extract
other information, or if your invoices have different structure.

## Configure data sources

### Local Folder
Read how to configure Local Folder Source in [README.md](local_folder/README.md)
### Google Drive
Read how to configure Google Drive Source in [README.md](google_drive/README.md)
### Inbox
Read how to configure Inbox Source in [README.md](inbox/README.md)


## Run the pipeline
### Choose the function you want to run:
1. The function `from_local_folder_to_structured` iterates through all the files
in the specified local folder and saves their filepaths to the destination
(e.g., a database). It then processes each filepath individually.

2. The function `from_google_drive_to_structured` downloads data from a folder
in Google Drive to the local folder and saves the filepaths to the destination (e.g., a database).
It then processes each filepath individually.

3. The function `from_inbox_to_structured` provides functionalities to collect inbox emails, download attachments to a local
folder, and store all relevant email information in a destination.
It then processes each filepath individually.

### Run the command
```python
python unstructured_data_pipeline.py
```

The `unstructured_to_structured_source` function includes a transformer-type
resource that processes each item using the `unstructured` library.
The processing results are retrieved as a dictionary, which has a structure similar to the following:
```python
{
    'file_name': '/test_data/invoice_1.pdf',
    'recipient_company_name': 'XYZ Corporation',
    'invoice_amount': '11235.00',
    'invoice_date': 'June 30, 2023',
    'invoice_number': 'INV-549283',
    'service_description': 'Premium widget delivery and installation services',
    'phone_number': 'None'
    'metadata': {...},
}
```
Then `dlt` saves all processed structured data to the database (e.g. duckdb).

### Streamlit
To ensure that everything loads as expected, use the command:
```bash
dlt pipeline <pipeline_name> show
```
For example, the pipeline_name for the above pipeline example is `unstructured_inbox`,
you can use any custom name instead. So your command would look like:

```bash
dlt pipeline unstructured_inbox show
```