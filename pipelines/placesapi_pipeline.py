import dlt
from placesapi import places_api_source
from placesapi import places_info

def load_text_search_places_info():
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(pipeline_name='places_api', destination='bigquery', dataset_name='places_api_data')

    # run the pipeline with your parameters
    load_text_search = pipeline.run(places_api_source())

    load_place_info = pipeline.run(places_info)

    # pretty print the information on data that was loaded
    print(load_text_search)
    print(load_place_info)

if __name__=='__main__':
    load_text_search_places_info()
