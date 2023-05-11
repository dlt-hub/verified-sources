import dlt
from dlt.sources.helpers import requests
from dlt.common.typing import TDataItem, DictStrAny
from typing import Iterator, List, Optional, Union, Dict
from dlt.extract.source import DltResource

@dlt.source
def jira(subdomain: str = dlt.secrets.value, email: str = dlt.secrets.value, api_token: str = dlt.secrets.value) -> Iterator[DltResource]:
    """
    Jira source function that generates a list of resource functions based on endpoints.

    :param subdomain: The subdomain for the Jira instance.
    :param email: The email to authenticate with.
    :param api_token: The API token to authenticate with.
    :return: List of resource functions.
    """
    # Define endpoints
    endpoints = {'issues': {'data_path': 'issues',
                            'api_path': 'rest/api/3/search',
                            'params': {"fields": "*all",
                                       "expand": "fields,changelog,operations,transitions,names",
                                       "validateQuery": "strict",
                                       "jql": ''}
                            },
                 'users': {'api_path': "rest/api/3/users/search", 'params': {"includeInactiveUsers": True}},
                 'workflows': {'data_path': 'values', 'api_path': "/rest/api/3/workflow/search", 'params': {}},
                 'projects': {'data_path': 'values', 'api_path': "rest/api/3/project/search", 'params': {'expand': 'description,lead,issueTypes,url,projectKeys,permissions,insight'}},
                 }


    resources = []
    for endpoint_name, endpoint_parameters in endpoints.items():
        res_function = dlt.resource(get_paginated_data,
                                    name=endpoint_name,
                                    write_disposition="replace")(**endpoint_parameters, subdomain=subdomain, email=email, api_token=api_token)
        resources.append(res_function)

    return resources


@dlt.source
def jira_search(subdomain: str = dlt.secrets.value, email: str = dlt.secrets.value, api_token: str = dlt.secrets.value) -> Iterator[DltResource]:
    """
    Jira search source function that generates a resource function for searching issues.

    :param subdomain: The subdomain for the Jira instance.
    :param email: The email to authenticate with.
    :param api_token: The API token to authenticate with.
    :return: Resource function for searching issues.
    """
    @dlt.resource(write_disposition='replace')
    def issues(jql_queries:List[str],
               subdomain=subdomain,
               email=email,
               api_token=api_token):

        api_path = 'rest/api/3/search'

        for jql in jql_queries:
            params = {"fields": "*all",
                      "expand": "fields,changelog,operations,transitions,names",
                      "validateQuery": "strict",
                      "jql": jql}

            yield from get_paginated_data(api_path=api_path, params=params, subdomain=subdomain, email=email, api_token=api_token)

    return issues


def get_paginated_data(subdomain: str, email: str, api_token: str, api_path: str = "rest/api/2/search", data_path: Optional[str] = None, params: DictStrAny = {}) -> Iterator[Dict]:
    """
    Function to fetch paginated data from a Jira API endpoint.

    :param subdomain: The subdomain for the Jira instance.
    :param email: The email to authenticate with.
    :param api_token: The API token to authenticate with.
    :param api_path: The API path for the Jira endpoint.
    :param data_path: Optional data path to extract from the response.
    :param params: Optional parameters for the API request.
    :return: Yields pages of data from the API.
    """
    url = f"https://{subdomain}.atlassian.net/{api_path}"
    headers = {
        "Accept": "application/json"
    }
    auth = (email, api_token)
    start_at, max_results = 0, 50
    while True:
        params['startAt'], params['maxResults'] = start_at, max_results
        response = requests.get(url, auth=auth, headers=headers, params=params)
        response.raise_for_status()

        if data_path:
            results_page = response.json()[data_path]
        else:
            results_page = response.json()

        yield results_page
        if len(results_page) < max_results:
            break
        start_at += max_results
