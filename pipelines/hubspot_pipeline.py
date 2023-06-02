import dlt

from hubspot import hubspot, hubspot_events_for_objects, THubspotObjectType


def load_crm_data():
    """
    This function loads all resources from HubSpot CRM

    Returns:
        None
    """

    # Create a DLT pipeline object with the pipeline name, dataset name, and destination database type
    p = dlt.pipeline(
        pipeline_name="hubspot_pipeline",
        dataset_name="hubspot",
        destination="redshift",
        full_refresh=True,
    )

    # Run the pipeline with the HubSpot source connector
    info = p.run(hubspot())

    # Print information about the pipeline run
    print(info)


def load_web_analytics_events(object_type: THubspotObjectType, object_ids):
    """
    This function loads web analytics events for a list objects in `object_ids` of type `object_type`

    Returns:
        None
    """

    # Create a DLT pipeline object with the pipeline name, dataset name, and destination database type
    p = dlt.pipeline(
        pipeline_name="hubspot",
        dataset_name="hubspot",
        destination="postgres",
        full_refresh=False,
    )

    # you can get many resources by calling this function for various object types
    resource = hubspot_events_for_objects(object_type, object_ids)
    # and load them together passing resources in the list
    info = p.run([resource])

    # Print information about the pipeline run
    print(info)


if __name__ == "__main__":
    # Call the functions to load HubSpot data into the database with and without company events enabled
    load_crm_data()
    # load_web_analytics_events("company", ["7086461639", "7086464459"])
