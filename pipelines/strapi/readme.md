## Strapi

Strapi is a CMS that helps store documents in collections and makes them avaialable behind its api

## Strapi pipeline
This pipeline can call all the data from a strapi endpoint and do a full load (write disposition = replace)

## Setup

1. get credentials from Strapi. You will need your api token, and the domain name. The domain is found in your strapi url, as `https://{domain}/api/{endpoint}`. Fill them in your credentials file
2. init the pipeline on your local environment and decide which endpoints you want to get data for. Since Strapi is a CMS, you define your own endpoints
3. pass them to the pipeline as in the example pipeline

```python

import dlt
from strapi import strapi_source

def load(endpoints=None):
    endpoints = ['athletes'] or endpoints
    pipeline = dlt.pipeline(pipeline_name='strapi', destination='bigquery', dataset_name='strapi_data')

    # run the pipeline with your parameters
    load_info = pipeline.run(strapi_source(endpoints=endpoints))
    # pretty print the information on data that was loaded
    print(load_info)


if __name__ == "__main__" :
    # pass your own endpoints
    endpoints = ['athletes']
    load(endpoints)
```