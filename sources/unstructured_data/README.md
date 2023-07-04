# Unstructured data to structured
 Converts unstructured data from a specified data resource to structured data using provided queries.
## Init the pipeline
1. Init the main source:
    ```sh
    dlt init unstructured_data duckdb
    ```
2. Init dependent sources with the original unstructured data:
    ```sh
    dlt init filesystem duckdb
    ```
3. Set credentials for **filesystem**.
   1. If you plan to load files from a *local folder*, then set the path to the local folder in `.dlt/secrets.toml`:
       ```toml
       [sources.unstructured_data.local_folder]
       data_dir = "/path/to/your/local/data/folder"
       ```
   2. In case of *Google Drive* folder:
      ```toml
      [sources.unstructured_data.google_drive]
      credentials_path = '/path/to/your/credentials.json' # path to your google drive credentials
      folder_id = 'folder_id' # the google drive folder id
      storage_folder_path = './temp' # the folder where you want to store your downloaded files
      token_path = "./token.json" # the filepath where you want to store you token
      ```
4. Set credentials for **unstructured_data**.
    ```toml
    [sources.unstructured_data]
    openai_api_key = "openai_api_key"
    ```

## Run the pipeline
### Choose the function you want to run:
1. The function `from_local_folder_to_structured` iterates through all the files
in the specified local folder and saves their filepaths to the destination
(e.g., a database). It then processes each filepath individually.

2. The function `from_google_drive_to_structured` downloads data from a folder
in Google Drive to the local folder and saves the filepaths to the destination (e.g., a database).
It then processes each filepath individually.

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
}
```
Then `dlt` saves all processed structured data to the database (e.g. duckdb).

### Streamlit
To ensure that everything loads as expected, use the command:
```bash
dlt pipeline <pipeline_name> show
```
For example, the pipeline_name for the above pipeline example is `unstructured_local_folder`, you can use any custom name instead.
