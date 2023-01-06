import dlt
import requests





@dlt.source(max_table_nesting=1)
def pipedrive_source(pipedrive_api_key=dlt.secrets.value):
    endpoints = ['persons', 'deals', 'stages', 'productFields', 'products', 'pipelines', 'personFields',
                 'users', 'organizations', 'activityFields', 'dealFields']


    endpoint_resources = [dlt.resource(get_endpoint(endpoint, pipedrive_api_key), name=endpoint, write_disposition="replace") for endpoint in endpoints]
    # add activities
    activities_resource = dlt.resource(get_endpoint('activities', pipedrive_api_key, extra_params={'user_id': 0}), name='activities', write_disposition="replace")
    endpoint_resources.append(activities_resource)
    # add resources that need 2 requests
    endpoint_resources.append(deals_flow)
    endpoint_resources.append(deals_participants)
    return endpoint_resources

def _paginated_get(url, headers, params):
    """Requests and yields up to `max_pages` pages of results as per twitter api documentation: https://developer.twitter.com/en/docs/twitter-api/pagination"""
    # pagination start and page limit
    is_next_page = True
    params['start'] = 0
    params['limit'] = 500
    while is_next_page:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        page = response.json()
        # yield data only
        yield page['data']
        # check if next page exists
        is_next_page = page.get("additional_data", {}).get("pagination", {}).get("more_items_in_collection")
        # if it does, we will need this start index in the next params
        params['start'] = response.json().get("additional_data", {}).get("pagination", {}).get("next_start")


def get_endpoint(entity, pipedrive_api_key, extra_params=None):
    headers = {"Content-Type": "application/json"}
    params = {'api_token': pipedrive_api_key}
    if extra_params:
        params.update(extra_params)
    url = f'https://app.pipedrive.com/v1/{entity}'
    pages = _paginated_get(url, headers=headers, params=params)
    for page in pages:
        yield page

@dlt.resource(write_disposition="replace")
def deals_participants(pipedrive_api_key=dlt.secrets.value):
    gen = get_endpoint("deals", pipedrive_api_key)
    if len([gen])>1:
        for page in gen:
            for row in page:
                endpoint = f"deals/{row['id']}/participants"
                data = get_endpoint(endpoint, pipedrive_api_key)
                for page in data:
                    yield page

@dlt.resource(write_disposition="replace")
def deals_flow(pipedrive_api_key=dlt.secrets.value):
    gen = get_endpoint("deals", pipedrive_api_key)
    if len([gen])>1:
        for page in gen:
            for row in page:
                endpoint = f"deals/{row['id']}/flow"
                data = get_endpoint(endpoint, pipedrive_api_key)
                for page in data:
                    yield page

if __name__=='__main__':
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(pipeline_name='pipedrive', destination='bigquery', dataset_name='pipedrive_data_nest_1')

    #data = list(deals_participants())
    #print(data)
    # run the pipeline with your parameters
    load_info = pipeline.run(pipedrive_source())

    # pretty print the information on data that was loaded
    print(load_info)
