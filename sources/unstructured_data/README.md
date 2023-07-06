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
   1. If you plan to load files from a *local folder*, then set the path to the local data folder in `filesystem/settings.py`:
       ```python
       # Local folder
       DATA_DIR = "/path/to/your/local/data/folder"
       ```
   2. In case of *Google Drive* folder:

      [Read Quick Start with Google Drive:](https://developers.google.com/drive/api/quickstart/python?hl=en)

      1. Enable Google Drive API.
      2. Configure the OAuth consent screen.
      3. Create credentials json.

      Save the path to this json in `filesystem/settings.py`:
      ```python
      ClIENT_SECRET_PATH = "client_secret.json"
      ```

      If you already have the **authorized** user json file "token.json", then put it in a `filesystem/settings.py` file:
      ```python
      AUTHORIZED_USER_PATH = "/path/to/token.json"
      ```
      or you can use the authorized user info from this json directly, copy info from json to `.dlt/secrets.toml`:
      ```toml
      [sources.unstructured_data.google_drive.credentials]
      token = "<token>"
      refresh_token = "<refresh_token>"
      token_uri = "<token_uri>"
      client_id = "<client_id>"
      client_secret = "<client_secret>"
      scopes = ["<scopes>"]
      expiry = "<expiry>"
      ```

      Set in `filesystem/settings.py` the storage folder path, it is the local folder where the downloaded files will be stored:
      ```python
      STORAGE_FOLDER_PATH = "temp"
      ```
      List all Google Drive folders you want to extract files from:
      ```python
      FOLDER_IDS = ["1-yiloGjyl9g40VguIE1QnY5tcRPaF0Nm"]
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
