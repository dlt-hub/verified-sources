import dlt
import requests
import math
from typing import List, Sequence
from dlt.extract.source import DltResource, DltSource

@dlt.source
def strapi_source(endpoints: List[str],
                  api_secret_key: str = dlt.secrets.value,
                  domain: str = dlt.secrets.value,
                  ) -> Sequence[DltResource]:
    """ strapi behaves like a mongo db, with topics and documents
    endpoints represents a list of collections
    """


    endpoint_resources = [dlt.resource(_get_endpoint(api_secret_key, domain, endpoint),
                                       name=endpoint,
                                       write_disposition="replace")
                          for endpoint in endpoints]

    return endpoint_resources


def _get_endpoint(token: str, domain: str, endpoint: str) -> DltResource:
    """
    A generator that yields data from a paginated API endpoint.

    :param token: The access token for the API.
    :type token: str
    :param domain: The domain name of the API.
    :type domain: str
    :param endpoint: The API endpoint to query, defaults to ''.
    :type endpoint: str
    :yield: A list of data from the API endpoint.
    :rtype: list
    """
    api_endpoint = f'https://{domain}/api/{endpoint}'
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }
    page_size = 25
    params = {
        'pagination[start]': 0,
        'pagination[limit]': page_size,
        'pagination[withCount]': 1
    }

    # get the total number of pages
    response = requests.get(api_endpoint, headers=headers, params=params)
    response.raise_for_status()
    total_results = response.json()['meta']['pagination']['total']
    pages_total = math.ceil(total_results / page_size)

    # yield page by page
    for page_number in range(pages_total):
        params['pagination[start]'] = page_number * page_size
        response = requests.get(api_endpoint, headers=headers, params=params)
        response.raise_for_status()
        data = response.json().get('data')
        if data:
            yield data


if __name__=='__main__':
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(pipeline_name='strapi', destination='bigquery', dataset_name='strapi_data')
    endpoints = ['athletes']
    # run the pipeline with your parameters
    load_info = pipeline.run(strapi_source(endpoints=endpoints))
    # pretty print the information on data that was loaded
    print(load_info)
