
import dlt
from sharepoint import sharepoint_list, sharepoint_files, SharepointCredentials
from sharepoint.sharepoint_files_config import SharepointFilesConfig, SharepointListConfig

if __name__ == "__main__":
    # --- 1. Define SharePoint credentials ---
    credentials = SharepointCredentials(
        client_id="your-client-id",
        tenant_id="your-tenant-id",
        site_id="your-site-id",
        client_secret="your-client-secret",
        sub_site_id=""
    )

    # --- 2. Configure SharePoint list extraction ---
    list_config = SharepointListConfig(
        list_title="test_list",
        select="Title,ins",
        table_name="sharepoint_list_table"
    )

    # --- 3. Configure SharePoint file extraction ---
    files_config = SharepointFilesConfig(
        folder_path="General/sharepoint_test",
        file_name_startswith="test_",
        pattern=r".*\.csv$",
        file_type="csv",
        table_name="sharepoint_reports",
        is_file_incremental=True,
        pandas_kwargs={}
    )

    # --- 4. Create the DLT pipeline (destination = DuckDB) ---
    pipeline = dlt.pipeline(
        pipeline_name="sharepoint_to_duckdb",
        destination="duckdb",
        dataset_name="sharepoint_data",
        full_refresh=False
    )

    # --- 5. Run both sources and load to DuckDB ---
    print("Loading SharePoint List data...")
    list_load_info = pipeline.run(
        sharepoint_list(sharepoint_list_config=list_config, credentials=credentials)
    )
    print(list_load_info)
    with pipeline.sql_client() as client:
        df = client.execute("SELECT * FROM sharepoint_list_table LIMIT 10").df()
        print(df)


    print("Loading SharePoint Files data...")
    files_load_info = pipeline.run(
        sharepoint_files(sharepoint_files_config=files_config, credentials=credentials)
    )
    print(files_load_info)

    with pipeline.sql_client() as client:
        df = client.execute("SELECT * FROM sharepoint_reports LIMIT 10").df()
        print(df)
