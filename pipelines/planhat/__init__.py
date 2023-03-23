"""
Planhat api docs: https://docs.planhat.com/#introduction

Planhat Api doesn't offer any changelog

API Access Token can be generated using Service Accounts 
under the Settings section in Planhat. Once a token is 
created, it will appear once and last forever:
https://docs.planhat.com/#authentication
"""

import dlt
import requests


@dlt.source(name="planhat")
def planhat_source(planhat_api_key=dlt.secrets.value):
    """

    Returns resources:
        churn - default=100 | max=1,000
        companies - default=100 | max=5,000
        conversations - default=30 | max=2,000
        customfields - default=100 | max=2,000
        endusers - default=100 | max=2,000
        invoices - default=100 | max=2,000
        licenses - default=100 | max=2,000
        nps - default=100 | max=10,000
        tasks - default=500 | max=10,000
        users - default=10,000 | max=10,000

    """

    endpoints = [
        "churn",
        "companies",
        "conversations",
        "customfields",
        "endusers",
        "invoices",
        "licenses",
        "nps",
        "tasks",
        "users",
    ]

    endpoint_resources = [
        dlt.resource(
            _get_endpoint(endpoint, planhat_api_key),
            name=endpoint,
            write_disposition="replace",
        )
        for endpoint in endpoints
    ]

    return endpoint_resources


def _paginated_get(url, headers):
    """
    Limits are different from endpoint to endpoint. An example can be
    found here: https://docs.planhat.com/#get_churn_list
    """

    offset = 0
    endpoint_limits = [
        {"url": "https://api.planhat.com/churn", "limit": 1000, "offset": offset},
        {"url": "https://api.planhat.com/companies", "limit": 5000, "offset": offset},
        {
            "url": "https://api.planhat.com/conversations",
            "limit": 2000,
            "offset": offset,
        },
        {
            "url": "https://api.planhat.com/customfields",
            "limit": 2000,
            "offset": offset,
        },
        {"url": "https://api.planhat.com/endusers", "limit": 2000, "offset": offset},
        {"url": "https://api.planhat.com/invoices", "limit": 2000, "offset": offset},
        {"url": "https://api.planhat.com/licenses", "limit": 2000, "offset": offset},
        {"url": "https://api.planhat.com/nps", "limit": 10000, "offset": offset},
        {"url": "https://api.planhat.com/tasks", "limit": 10000, "offset": offset},
        {"url": "https://api.planhat.com/users", "limit": 10000, "offset": offset},
    ]

    more_data = True

    while more_data:
        for endpoint_limit in endpoint_limits:
            if url == endpoint_limit["url"]:
                params = {
                    "offset": offset,
                    "limit": endpoint_limit["limit"],
                }

                response = requests.get(url, headers=headers, params=params)
                data = response.json()

                if not data:
                    more_data = False
                else:
                    yield data

                offset += endpoint_limit["limit"]


def _get_endpoint(entity, planhat_api_key):
    """
    Generic method to retrieve endpoint data based on the required headers and params.

    Args:
        entity: the endpoint you want to call
        planhat_api_key:

    Returns:

    """
    headers = {"Authorization": f"Bearer {planhat_api_key}"}
    url = f"https://api.planhat.com/{entity}"
    pages = _paginated_get(url, headers=headers)
    yield from pages


if __name__ == "__main__":
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name="planhat",
        destination="bigquery",
        dataset_name="planhat_data",
    )

    # print credentials by running the resource
    data = list(planhat_source())

    # print the data yielded from resource
    print(data)

    # run the pipeline with your parameters
    load_info = pipeline.run(planhat_source())

    # pretty print the information on data that was loaded
    print(load_info)
