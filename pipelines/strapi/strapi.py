import dlt
import requests
import math

@dlt.source
def strapi_source(api_secret_key=dlt.secrets.value, domain=dlt.secrets.value, endpoints=None):
    """ strapi behaves like a mongo db, with topics and documents
    endpoints represents a list of collections
    """

    if not endpoints:
        endpoints = ['athletes']

    endpoint_resources = [dlt.resource(_get_endpoint(api_secret_key, domain, endpoint),
                                       name=endpoint,
                                       write_disposition="replace")
                          for endpoint in endpoints]

    return endpoint_resources


def _get_endpoint(token, domain, endpoint=''):
    """
    A generator that yields data from a paginated API endpoint.

    :param token: The access token for the API.
    :param domain: The domain name of the API.
    :param endpoint: The API endpoint to query.
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
