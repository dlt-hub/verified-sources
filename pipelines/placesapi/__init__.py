"""
 Loads location information (Ratings, Reviews, Business Hours) from Google Maps. 
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

    The Parameters to be passed to endpoint:
    @:params: query - The text against which a search will be made.
    @:params: radius (optional)  - Defines the distance (in meters) within which to return place results. You may bias results to a specified circle by passing a location and a radius parameter. Default value is 50,000 meters for text search.
    @:params: location (optional) - Can be used with the radius parameter to bias result.
    @:params: api_secret_key - The api key for the Places API defined in dlt.secrets.
    """

    # API Endpoint.
    text_search_url = "https://maps.googleapis.com/maps/api/place/textsearch/json?"

    payload = {
        'query' : 'ADD SEARCH TEXT HERE',
        'radius': 'ADD RADIUS FOR SEARCH',
        'location': 'LATITUDE, LONGITUDE',
        'key': api_secret_key
    }


    # make an api call here
    response = requests.get(text_search_url, params=payload)
    response.raise_for_status()
    yield response.json().get('results')


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