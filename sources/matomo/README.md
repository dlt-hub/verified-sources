# Matomo

> **Warning!**
>
> This source is a Community source and was tested only once. Currently, we **don't test** it on a regular basis.
> If you have any problem with this source, ask for help in our [Slack Community](https://dlthub.com/community).


Matomo is a free and open source web analytics platform that allows website owners and businesses to gain detailed insights into the performance of their websites and applications. With this verified source you can easily extract data from Matomo and seamlessly load it into your preferred destination. This verified source supports the following endpoints:

| Endpoint | Description |
| --- | --- |
| matomo_visits | Loads a list of visits. Initially loads the current day's visits and any visits in initial_past_days. |
| matomo_reports | The data is retrieved according to the specified queries. For example, in the pipeline example. |
| get_last_visits | Retrieves the visits to the given site ID for the given time period. |
| get_unique_visitors | Retrieves the information about the visitors from get_last_visits. |

## Initialize the pipeline with Matomo verified source
To get started with your data pipeline, follow these steps:
```bash
dlt init matomo duckdb
```

Here, we chose DuckDB as the destination. Alternatively, you can also choose redshift, bigquery, or any of the other [destinations.](https://dlthub.com/docs/dlt-ecosystem/destinations/)

## Grab Matomo credentials

To obtain the Matomo credentials, please refer to the [full documentation here.](https://dlthub.com/docs/dlt-ecosystem/verified-sources/matomo)

## **Add credential**

1. Open `.dlt/secrets.toml`.
2. Enter the API key:

    ```toml
    # put your secret values and credentials here. do not share this file and do not push it to github
    [sources.matomo]
    api_token= "access_token" # please set me up!
    ```

3. Enter credentials for your chosen destination as per the [docs](https://dlthub.com/docs/dlt-ecosystem/destinations/).
4. Inside the **`.dlt`** folder, you'll find a file called **`config.toml`**, where you can securely store your pipeline configuration details.

    Here's what the config.toml looks like:

    ```toml
    [sources.matomo]
    url = "Please set me up !" # please set me up!
    queries = ["a", "b", "c"] # please set me up!
    site_id = 0 # please set me up!
    live_events_site_id = 0 # please set me up!
    ```

5. Replace the value of `url` and `site_id` with the one that you copied above. This will ensure that your data pipeline can access the required Matomo resources.
6. In order to track live events for a website, the `live_event_site_id` parameter must be set to the same value as the `site_id` parameter for that website.

## Run the pipeline

1. Install the necessary dependencies by running the following command:

    ```bash
    pip install -r requirements.txt
    ```

2. Now the pipeline can be run by using the command:

    ```bash
    python matomo_pipeline.py
    ```

3. To make sure that everything is loaded as expected, use the command:

    ```bash
    dlt pipeline <pipeline_name> show
    ```

    For example, the pipeline_name for the above pipeline is `matomo`, you may also use any custom name instead.



💡 To explore additional customizations for this pipeline, we recommend referring to the official DLT Matomo documentation. It provides comprehensive information and guidance on how to further customize and tailor the pipeline to suit your specific needs. You can find the DLT Matomo documentation in [Setup Guide: Matomo.](https://dlthub.com/docs/dlt-ecosystem/verified-sources/matomo)
