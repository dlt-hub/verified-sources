"""
Pipedrive api docs: https://developers.pipedrive.com/docs/api/v1

Pipedrive changes or deprecates fields and endpoints without versioning the api.
If something breaks, it's a good idea to check the changelog.
Api changelog: https://developers.pipedrive.com/changelog

To get an api key: https://pipedrive.readme.io/docs/how-to-find-the-api-token
"""

import dlt
import functools
import requests

from .custom_fields_munger import munge_push_func, pull_munge_func, parsed_mapping


@dlt.source(name="pipedrive")
def pipedrive_source(pipedrive_api_key=dlt.secrets.value):
    """

    Args:
    pipedrive_api_key: https://pipedrive.readme.io/docs/how-to-find-the-api-token

    Returns resources:
        activityFields
        dealFields
        deals
        deals_flow
        deals_participants
        organizationFields
        organizations
        personFields
        persons
        pipelines
        productFields
        products
        stages
        users

    Resources that depend on another resource are implemented as tranformers
    so they can re-use the original resource data without re-downloading.
    Examples:  deals_participants, deals_flow

    """

    # we need to add entity fields' resources before entities' resources in order to let custom fields data munging work properly
    entity_fields_endpoints = ['activityFields', 'dealFields', 'organizationFields', 'personFields', 'productFields']
    endpoints_resources = [dlt.resource(_get_endpoint(endpoint, pipedrive_api_key), name=endpoint, write_disposition='replace') for endpoint in entity_fields_endpoints]

    entities_endpoints = ['organizations', 'pipelines', 'persons', 'products', 'stages', 'users']
    endpoints_resources += [dlt.resource(_get_endpoint(endpoint, pipedrive_api_key), name=endpoint, write_disposition='replace') for endpoint in entities_endpoints]
    # add activities
    activities_resource = dlt.resource(_get_endpoint('activities', pipedrive_api_key, extra_params={'user_id': 0}), name='activities', write_disposition='replace')
    endpoints_resources.append(activities_resource)
    # add resources that need 2 requests

    # we make the resource explicitly and put it in a variable
    deals = dlt.resource(_get_endpoint('deals', pipedrive_api_key), name='deals', write_disposition='replace')

    endpoints_resources.append(deals)

    # in order to avoid calling the deal endpoint 3 times (once for deal, twice for deal_entity)
    # we use a transformer instead of a resource.
    # The transformer can use the deals resource from cache instead of having to get the data again.
    # We use the pipe operator to pass the deals to

    endpoints_resources.append(deals | deals_participants(pipedrive_api_key=pipedrive_api_key))
    endpoints_resources.append(deals | deals_flow(pipedrive_api_key=pipedrive_api_key))

    # add custom fields' mapping
    endpoints_resources.append(parsed_mapping())

    return endpoints_resources


def _paginated_get(url, headers, params):
    """
    Requests and yields data 500 records at a time
    Documentation: https://pipedrive.readme.io/docs/core-api-concepts-pagination
    """
    # pagination start and page limit
    is_next_page = True
    params['start'] = 0
    params['limit'] = 500
    while is_next_page:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        page = response.json()
        # yield data only
        data = page['data']
        if data:
            yield data
        # check if next page exists
        pagination_info = page.get("additional_data", {}).get("pagination", {})
        # is_next_page is set to True or False
        is_next_page = pagination_info.get("more_items_in_collection", False)
        if is_next_page:
            params['start'] = pagination_info.get("next_start")


ENTITY_FIELDS_SUFFIX = "Fields"


def _get_endpoint(entity, pipedrive_api_key, extra_params=None, munge_custom_fields=True):
    """
    Generic method to retrieve endpoint data based on the required headers and params.

    Args:
        entity: the endpoint you want to call
        pipedrive_api_key:
        extra_params: any needed request params except pagination.
        munge_custom_fields: whether to perform custom fields' data munging or not

    Returns:

    """
    headers = {"Content-Type": "application/json"}
    params = {'api_token': pipedrive_api_key}
    if extra_params:
        params.update(extra_params)
    url = f'https://app.pipedrive.com/v1/{entity}'
    pages = _paginated_get(url, headers=headers, params=params)
    if munge_custom_fields:
        if ENTITY_FIELDS_SUFFIX in entity:  # checks if it's an entity fields' endpoint (e.g.: activityFields)
            munging_func = munge_push_func
        else:
            munging_func = pull_munge_func
            entity = entity[:-1] + ENTITY_FIELDS_SUFFIX  # converts entities' endpoint into entity fields' endpoint
        pages = map(functools.partial(munging_func, endpoint=entity), pages)
    yield from pages


@dlt.transformer(write_disposition="replace")
def deals_participants(deals_page, pipedrive_api_key=dlt.secrets.value):
    """
    This transformer builds on deals resource data.
    The data is passed to it page by page (as the upstream resource returns it)
    """
    for row in deals_page:
        endpoint = f"deals/{row['id']}/participants"
        data = _get_endpoint(endpoint, pipedrive_api_key, munge_custom_fields=False)
        if data:
            yield from data


@dlt.transformer(write_disposition="replace")
def deals_flow(deals_page, pipedrive_api_key=dlt.secrets.value):
    """
    This transformer builds on deals resource data.
    The data is passed to it page by page (as the upstream resource returns it)
    """
    for row in deals_page:
        endpoint = f"deals/{row['id']}/flow"
        data = _get_endpoint(endpoint, pipedrive_api_key, munge_custom_fields=False)
        if data:
            yield from data
