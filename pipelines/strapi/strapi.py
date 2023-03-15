import dlt
import requests
import math

@dlt.source
def strapi_source(api_secret_key=dlt.secrets.value, endpoints=None):
    """ strapi behaves like a mongo db, with topics and documents
    endpoints represents a list of collections
    """

    if not endpoints:
        endpoints = ['athletes']

    endpoint_resources = [dlt.resource(_get_endpoint(api_secret_key, endpoint),
                                       name=endpoint,
                                       write_disposition="replace")
                          for endpoint in endpoints]

    return endpoint_resources


def _get_endpoint(token, endpoint=''):

    domain = "fantium-strapi.up.railway.app"
    api_endpoint = f'https://{domain}/api/{endpoint}'
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }
    page_size = 25
    params = {'pagination[start]': 0,
              "pagination[limit]": page_size,
              "pagination[withCount]": 1}

    # yield page by page
    pages_dowloaded = 0
    pages_total = 1
    while pages_dowloaded <= pages_total:

        response = requests.get(api_endpoint, headers=headers, params=params)
        response.raise_for_status()
        print(response.json())
        data = response.json().get('data')
        if data:
            yield data

        # keep track of pages left
        pages_dowloaded +=1
        params['pagination[start]'] = pages_dowloaded

        total_results = response.json().get('meta').get('pagination').get('total')
        pages_total = math.floor(total_results/page_size)


if __name__=='__main__':
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(pipeline_name='strapi', destination='bigquery', dataset_name='strapi_data')
    endpoints = ['athletes']
    # run the pipeline with your parameters
    load_info = pipeline.run(strapi_source(endpoints=endpoints))
    # pretty print the information on data that was loaded
    print(load_info)
