# SharePoint

Loads SharePoint lists and the files of document libraries through the Microsoft Graph API.

- `sharepoint_list` loads the items of one list. The first run reads the whole list; later runs read only what changed through Graph's delta query, and items deleted in SharePoint are deleted from the table.
- `sharepoint_lists` does the same for several lists at once, by default every visible user-created list of the site, one table per list.
- `sharepoint_files` loads the rows of CSV, JSONL, Parquet or Excel files from a folder of a document library. Only new or modified files are read, and a modified file replaces its own rows. The folder can be searched with a glob such as `**/*.csv`, so subfolders are included when wanted.

The files source runs on dlt's [filesystem source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/filesystem) over a read-only fsspec filesystem for SharePoint, `SharepointFileSystem`, which you can also use directly. The list sources use Graph's [delta query](https://learn.microsoft.com/graph/api/listitem-delta) to read only what changed.

## Initialize the pipeline with the SharePoint verified source

```bash
dlt init sharepoint duckdb
```

Here, we chose DuckDB as the destination. Alternatively, you can also choose any of the other [destinations](https://dlthub.com/docs/dlt-ecosystem/destinations/).

## Setup verified source and pipeline example

To initialize the verified source, please refer to the [full documentation here](https://dlthub.com/docs/dlt-ecosystem/verified-sources/sharepoint).

## Add credentials and configs

1. Register an app in Microsoft Entra ID with the Microsoft Graph application permission `Sites.Read.All` (or `Sites.Selected` with access granted to the site) and admin consent.
2. Open `.dlt/secrets.toml` and fill in the section that `dlt init` created:

    ```toml
    [sources.sharepoint.credentials]
    tenant_id = "please set me up!"     # Directory (tenant) ID of the app
    client_id = "please set me up!"     # Application (client) ID of the app
    client_secret = "please set me up!" # the client secret value
    site_id = "please set me up!"       # "<host>.sharepoint.com,<site-collection-guid>,<web-guid>"
    ```

3. Set the list and the files to load, as arguments of the sources like `sharepoint_pipeline.py` does, or in `.dlt/config.toml`:

    ```toml
    [sources.sharepoint.sharepoint_list]
    list_title = "please set me up!"    # display name of the list, also the table name

    [sources.sharepoint.sharepoint_files]
    folder_path = "please set me up!"   # relative to the library root, "" for the root
    file_type = "please set me up!"     # csv | jsonl | parquet | excel
    table_name = "please set me up!"
    ```

4. Enter credentials for your chosen destination as per the [docs](https://dlthub.com/docs/dlt-ecosystem/destinations/).

## Run the pipeline example

1. Install the necessary dependencies by running the following command:

    ```bash
    pip install -r requirements.txt
    ```

2. Now the pipeline can be run by using the command:

    ```bash
    python sharepoint_pipeline.py
    ```

3. To make sure that everything is loaded as expected, use the command:

    ```bash
    dlt pipeline sharepoint show
    ```

💡 To explore additional customizations for this pipeline, we recommend referring to the official dlt SharePoint verified source documentation. It provides comprehensive information and guidance on how to further customize and tailor the pipeline to suit your specific needs. You can find the dlt SharePoint documentation in [Setup Guide: SharePoint](https://dlthub.com/docs/dlt-ecosystem/verified-sources/sharepoint).
