"""
Active Campaign api docs: https://developers.activecampaign.com/reference/overview

Active Campaign changes or deprecates fields and endpoints without versioning the api.
If something breaks, it's a good idea to check the changelog.
Api changelog: https://developers.activecampaign.com/changelog

To get an api key: https://developers.activecampaign.com/reference/authentication (account > settings > developer)
"""

import dlt
import requests


@dlt.source(name="active_campaign")
def active_campaign_source(active_campaign_api_key=dlt.secrets.value):
    """

    Returns resources:
        accounts -- https://yourAccountName.api-us1.com/api/3/accounts
        accountContacts -- https://yourAccountName.api-us1.com/api/3/accountContacts
        addresses -- https://yourAccountName.api-us1.com/api/3/addresses
        automations -- https://youraccountname.api-us1.com/api/3/automations
        campaigns -- https://youraccountname.api-us1.com/api/3/campaigns
        contacts -- https://youraccountname.api-us1.com/api/3/contacts
        contactAutomations -- https://youraccountname.api-us1.com/api/3/contactAutomations (List all automations the contact is in)
        groups -- https://youraccountname.api-us1.com/api/3/groups
        lists -- https://youraccountname.api-us1.com/api/3/lists
        messages -- https://youraccountname.api-us1.com/api/3/messages
        siteTrackingDomains -- https://youraccountname.api-us1.com/api/3/siteTrackingDomains
        tags -- https://youraccountname.api-us1.com/api/3/tags
        users -- https://youraccountname.api-us1.com/api/3/users
        deals -- https://youraccountname.api-us1.com/api/3/deals

    """

    endpoints = [
        "accounts",
        "accountContacts",
        "addresses",
        "automations",
        "campaigns",
        "contacts",
        "contactAutomations",
        "groups",
        "lists",
        "messages",
        "siteTrackingDomains",
        "tags",
        "users",
        "deals",
    ]

    endpoint_resources = [
        dlt.resource(
            _get_endpoint(endpoint, active_campaign_api_key),
            name=endpoint,
            write_disposition="replace",
        )
        for endpoint in endpoints
    ]

    return endpoint_resources


def _paginated_get(url, entity, headers):
    """
    Requests and yields data 20 records by default and the maximum can be set to 100.
    Documentation: https://developers.activecampaign.com/reference/pagination
    """

    limit = 100
    offset = 0

    more_data = True

    while more_data:
        params = {
            "offset": offset,
            "limit": limit,
        }
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        page = response.json()
        data = page[entity]
        if data:
            yield data

        # get next page
        total_pages = page["meta"]["total"]

        offset += limit

        if offset > int(total_pages):
            more_data = False


def _get_endpoint(entity, active_campaign_api_key):
    """
    Generic method to retrieve endpoint data based on the required headers and params.

    Args:
        entity: the endpoint you want to call
        active_campaign_api_key:
        extra_params: any needed request params except pagination.

    Returns:

    """
    headers = {
        "Content-Type": "application/json",
        "Api-Token": str(active_campaign_api_key),
    }
    url = f"https://youraccountname.api-us1.com/api/3/{entity}"
    pages = _paginated_get(url, entity, headers=headers)
    yield from pages


if __name__ == "__main__":
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name="active_campaign",
        destination="bigquery",
        dataset_name="active_campaign_data",
    )

    # print credentials by running the resource
    data = list(active_campaign_source())

    # print the data yielded from resource
    print(data)
    # exit()

    # run the pipeline with your parameters
    load_info = pipeline.run(active_campaign_source())

    # pretty print the information on data that was loaded
    print(load_info)
