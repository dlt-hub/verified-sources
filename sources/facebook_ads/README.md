# Facebook Ads README.md

This Facebook dlt verified source and pipeline example loads data to a preferred destination using the Facebook Marketing API. It supports loading data from multiple endpoints, providing flexibility in the data you can retrieve. The following endpoints are available for loading data with this verified source:
| Endpoint | Description |
| --- | --- |
| campaigns | a structured marketing initiative that focuses on a specific objective or goal |
| ad_sets | a subset or group of ads within a campaign |
| ads | an individual advertisement that is created and displayed within an ad set |
| creatives | visual and textual elements that make up an advertisement |
| ad_leads | information collected from users who have interacted with lead-generation ads |

## Initialize the Facebook Ads verified source and pipeline example[](https://dlthub.com/docs/dlt-ecosystem/verified-sources/facebook_ads#initialize-the-facebook-ads-verified-source-and-pipeline-example)
```bash
dlt init facebook_ads bigquery
```

Here, we chose BigQuery as the destination. Alternatively, you can also choose `redshift`, `duckdb`, or any of the other [destinations](https://dlthub.com/docs/dlt-ecosystem/destinations/).

## Grab Facebook Ads credentials

To read about grabbing the Facebook Ads credentials and configuring the verified source, please refer to the [full documentation here.](https://dlthub.com/docs/dlt-ecosystem/verified-sources/facebook_ads#grab-credentials)

## **Add credential**

1. Open `.dlt/secrets.toml`.
2. Enter the `access_token` :
    
    ```toml
    # put your secret values and credentials here. do not share this file and do not push it to github
    [sources.facebook_ads]
    access_token="set me up!"
    ```
    
3. Enter credentials for your chosen destination as per the [docs](https://dlthub.com/docs/dlt-ecosystem/destinations/).
4. Inside the `.dlt` folder, you'll find a file called `config.toml`, where you can securely store your pipeline configuration details.
    
    Here's what the `config.toml` looks like:
    
    ```toml
    [sources.facebook_ads]
    account_id = "1430280281077689"
    ```
    
5. Replace the value of the account id.

## Run the pipeline example

1. Install the necessary dependencies by running the following command:
    
    ```bash
    pip install -r requirements.txt
    ```
    
2. Now the pipeline can be run by using the command:
    
    ```bash
    python3 facebook_ads_pipeline.py
    ```
    
3. To make sure that everything is loaded as expected, use the command:
    
    ```bash
    dlt pipeline <pipeline_name> show
    ```
    
    For example, the pipeline_name for the above pipeline example is `facebook_ads`, you may also use any custom name instead
    

<aside>
💡 To explore additional customizations for this pipeline, We recommend referring to the official `dlt` Facebook Ads documentation. It provides comprehensive information and guidance on how to further customize and tailor the pipeline to suit your specific needs. You can find the Facebook Ads verified source documentation in [Setup Guide: Facebook Ads](https://dlthub.com/docs/dlt-ecosystem/verified-sources/facebook_ads)

</aside>
