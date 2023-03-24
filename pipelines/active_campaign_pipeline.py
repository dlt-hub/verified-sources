import dlt
from active_campaign import active_campaign


def load_active_campaign():
    """Constructs a pipeline that will load all pipedrive data"""
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name="active_campaign",
        destination="bigquery",
        dataset_name="active_campaign_data",
    )
    load_info = pipeline.run(active_campaign())
    print(load_info)


def load_selected_data():
    """Shows how to load just selected tables using `with_resources`"""
    pipeline = dlt.pipeline(
        pipeline_name="active_campaign",
        destination="bigquery",
        dataset_name="active_campaign_data",
    )
    load_info = pipeline.run(
        active_campaign().with_resources(
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
        )
    )

    print(load_info)
    # just to show how to access resources within source


if __name__ == "__main__":
    # run our main example
    load_active_campaign()
    # or load selected tables and display resource info
    # for that, uncomment the below
    # load_selected_data()
