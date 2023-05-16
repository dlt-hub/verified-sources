import dlt
from placesapi import places_api_source
from collections import namedtuple

def load_text_search_places_info(query: str, radius: int = None, location: namedtuple('Coordinates',['lat','long']) = None):
    """
    Constructs the pipeline that loads location information for specified search.

    @:params: query - The text against which a search will be made.
    @:params: radius (optional)  - Defines the distance (in meters) within which to return place results. You may bias results to a specified circle by passing a location and a radius parameter. Default value is 50,000 meters for text search.
    @:params: location (optional) - Coordinates (latitude and longitude) can be used with the radius parameter to bias result.
    """
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(pipeline_name='places_api', destination='bigquery', dataset_name='places_api_data')
    
    # run the pipeline with your parameters
    load_text_search = pipeline.run(places_api_source(query=query, radius=radius, location=location))

    # pretty print the information on data that was loaded
    print(load_text_search)


if __name__=='__main__':

    # Add Search Parameters 
    search_query = 'Add Search Text Here!'
    search_radius =  'Add Search Radius Here!'
    search_location = ('Latitude, Longitude')


    load_text_search_places_info(query=search_query, radius=search_radius, location=search_location)
 
