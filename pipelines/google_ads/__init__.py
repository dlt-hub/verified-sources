"""This example illustrates how to get all campaigns.

To add campaigns, run add_campaigns.py.
"""

from google.ads.googleads.client import GoogleAdsClient


def main(client, customer_id):
    ga_service = client.get_service("GoogleAdsService")

    query = """
        SELECT
          campaign.id,
          campaign.name
        FROM campaign
        ORDER BY campaign.id"""

    # Issues a search request using streaming.
    stream = ga_service.search_stream(customer_id=customer_id, query=query)

    for batch in stream:
        for row in batch.results:
            print(
                f"Campaign with ID {row.campaign.id} and name "
                f'"{row.campaign.name}" was found.'
            )


if __name__ == "__main__":

    credentials = {
        "developer_token": "",
        "refresh_token": "",
        "client_id": "",
        "client_secret": ""}

    client = GoogleAdsClient.load_from_dict(config_dict=credentials)
    main(client=client, customer_id="")
