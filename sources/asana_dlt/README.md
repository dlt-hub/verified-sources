# Asana

This verified source enables data loading from the Asana API to your preferred destination. It supports loading data from various endpoints, providing flexibility in the data you can retrieve.

Resources that can be loaded using this verified source are:

| S.No. | Name | Description |
| --- | --- | --- |
| 1 | workspaces | people, materials, or assets required to complete a task or project successfully |
| 2 | projects | collections of tasks and related information |
| 3 | sections | used to organize tasks within a project into smaller groups or categories |
| 4 | tags | labels that can be attached to tasks, projects, or conversations to help categorize and organize them |
| 5 | stories | updates or comments that team members can add to a task or project |
| 6 | teams | groups of individuals who work together to complete projects and tasks |
| 7 | users | individuals who have access to the Asana platform |

## Initialize the pipeline with Asana source
```bash
dlt init asana_dlt duckdb
```

Here, we chose DuckDB as the destination. Alternatively, you can also choose redshift, bigquery, or any of the other [destinations](https://dlthub.com/docs/dlt-ecosystem/destinations/).

## Grab Asana credentials

To grab the Asana credentials please refer to the [full documentation here.](https://dlthub.com/docs/dlt-ecosystem/verified-sources/asana)

## **Add credential**

1. Open .dlt/secrets.toml.
2. Enter the access token:

    ```toml
    [sources.asana_dlt]
    access_token = "access_token" # please set me up!
    ```

3. Enter credentials for your chosen destination as per the [docs](https://dlthub.com/docs/dlt-ecosystem/destinations/).

## Run the pipeline

1. Before running the pipeline, ensure that you have installed all the necessary dependencies by running the command:

    ```bash
    pip install -r requirements.txt

    ```

2. You're now ready to run the pipeline! To get started, run the following command:

    ```bash
    python asana_dlt_pipeline.py

    ```

3. Once the pipeline has finished running, you can verify that everything loaded correctly by using the following command:

    ```bash
    dlt pipeline <pipeline_name> show
    ```

    Note that in the above command, replace `<pipeline_name>` with the name of your pipeline. For example, if you named your pipeline "asana" you would run:

    ```bash
    dlt pipeline asana show
    ```


💡 To explore additional customizations for this pipeline, I recommend referring to the official Asana documentation. It provides comprehensive information and guidance on how to further customize and tailor the pipeline to suit your specific needs. You can find the Asana documentation in [Setup Guide: Asana](https://dlthub.com/docs/dlt-ecosystem/verified-sources/asana)
