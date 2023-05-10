"""
 Uses the Google Places API to get detail information (Ratings, Reviews, Business Hours) for specified locations (Businesses / Public Places etc).
 For more Info on Places API: "https://developers.google.com/maps/documentation/places/web-service/overview"  
"""

import dlt
from dlt.sources.helpers import requests
from dlt.extract.source import DltResource


@dlt.source
def places_api_source(api_secret_key=dlt.secrets.value) ->  DltResource:
    """
    The main source for dlt pipeline returns the text_search_resource.

    @:params: api_secret_key - The api key for the Places API defined in dlt.secrets.
    """
    return text_search(api_secret_key)


@dlt.resource(write_disposition="append")
def text_search(api_secret_key=dlt.secrets.value):
    """
    Creates a text search resource. A Text Search returns information about a set of places based on a text string provided.

    For more Info: "https://developers.google.com/maps/documentation/places/web-service/search-text"
    
    """

    # API Endpoint.
    text_search_url = "https://maps.googleapis.com/maps/api/place/textsearch/json?"

    '''
    The Parameters to be passsed to endpoint:
    @:params: query - The text against which a search will be made.
    @:params: radius  - Defines the distance (in meters) within which to return place results.
    @:params: api_secret_key - The api key for the Places API defined in dlt.secrets.
    '''
    payload = {
        'query' : 'FitX',
        'radius' : '',
        'key': {api_secret_key}
    }


    # make an api call here
    response = requests.get(text_search_url, params=payload)
    response.raise_for_status()
    yield response.json().get('results')


@dlt.resource(write_disposition="append")
def search_find_place(api_secret_key=dlt.secrets.value):
    """
    A Find Place request takes a text input and returns a place. The input can be any kind of Places text data, such as a name, address, or phone number.
    For more Info: "https://developers.google.com/maps/documentation/places/web-service/search-find-place"

    """
    pass


@dlt.resource(write_disposition="append")
def nearby_search(api_secret_key=dlt.secrets.value):
    """
    A Nearby Search lets you search for places within a specified area. You can refine your search request by supplying keywords or specifying the type of place you are searching for.
    For more Info: "https://developers.google.com/maps/documentation/places/web-service/search-nearby"
    
    """
    pass


@dlt.transformer(data_from=text_search) 
def places_info(results, api_secret_key=dlt.secrets.value):
    """
    Uses the Place Detail endpoint to get detail information about particular place. It uses the place_ids returned by text_search as input.
    For more Info: "https://developers.google.com/maps/documentation/places/web-service/details"

    @:params: results - The json data (results) returned by text_search().
    @:params: api_secret_key - The api key for the Places API defined in dlt.secrets.     
    """

    # Place Detail Endpoint
    place_info_url = "https://maps.googleapis.com/maps/api/place/details/json"

    for ids in list(results):
        payload = {
        'place_id' : ids['place_id'],
        'key': api_secret_key
        }

        # make an api call here
        response = requests.get(place_info_url, params=payload)
        response.raise_for_status()
        yield response.json().get('result')