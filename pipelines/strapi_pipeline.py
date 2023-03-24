
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
    # add your desired endpoints to the list
    endpoints = ['athletes']
    load(endpoints)