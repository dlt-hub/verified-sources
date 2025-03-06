from typing import List

import dlt

from hubspot import hubspot, hubspot_events_for_objects, THubspotObjectType


def load_crm_data() -> None:
    """
    This function loads all resources from HubSpot CRM

    Returns:
        None
    """

    # Create a DLT pipeline object with the pipeline name, dataset name, and destination database type
    # Add full_refresh=(True or False) if you need your pipeline to create the dataset in your destination
    p = dlt.pipeline(
        pipeline_name="hubspot",
        dataset_name="hubspot_dataset",
        destination="duckdb",
    )

    # Run the pipeline with the HubSpot source connector
    info = p.run(hubspot())

    # Print information about the pipeline run
    print(info)


def load_crm_data_with_history() -> None:
    """
    Loads all HubSpot CRM resources and property change history for each entity.
    The history entries are loaded to a tables per resource `{resource_name}_property_history`, e.g. `contacts_property_history`

    Returns:
        None
    """

    # Create a DLT pipeline object with the pipeline name, dataset name, and destination database type
    # Add full_refresh=(True or False) if you need your pipeline to create the dataset in your destination
    p = dlt.pipeline(
        pipeline_name="hubspot",
        dataset_name="hubspot_dataset",
        destination="duckdb",
    )

    # Configure the source with `include_history` to enable property history load, history is disabled by default
    data = hubspot(include_history=True)

    # Run the pipeline with the HubSpot source connector
    info = p.run(data)

    # Print information about the pipeline run
    print(info)


def load_crm_data_with_soft_delete() -> None:
    """
    Loads all HubSpot CRM resources, including soft-deleted (archived) records for each entity.
    By default, only the current state of the records is loaded; property change history is not included unless explicitly enabled.

    Soft-deleted records are retrieved and marked appropriately, allowing both active and archived data to be processed.
    """

    # Create a DLT pipeline object with the pipeline name, dataset name, and destination database type.
    # You can add `full_refresh=True` if the pipeline should recreate the dataset at the destination.
    p = dlt.pipeline(
        pipeline_name="hubspot",
        dataset_name="hubspot_dataset",
        destination="duckdb",
    )

    # Configure the source to load soft-deleted (archived) records.
    # Property change history is disabled by default unless configured separately.
    data = hubspot(soft_delete=True)

    # Run the pipeline with the HubSpot source connector.
    info = p.run(data)

    # Print information about the pipeline run.
    print(info)


def load_crm_objects_with_custom_properties() -> None:
    """
    Loads CRM objects, reading only properties defined by the user.
    """

    # Create a DLT pipeline object with the pipeline name,
    # dataset name, properties to read and destination database
    # type Add full_refresh=(True or False) if you need your
    # pipeline to create the dataset in your destination
    pipeline = dlt.pipeline(
        pipeline_name="hubspot",
        dataset_name="hubspot_dataset",
        destination="duckdb",
    )

    load_data = hubspot(
        properties={"contact": ("date_of_birth", "degree")}, include_custom_props=True
    )
    load_info = pipeline.run(load_data)
    print(load_info)


def load_pipelines() -> None:
    """
    This function loads web analytics events for a list objects in `object_ids` of type `object_type`

    Returns:
        None
    """

    # Create a DLT pipeline object with the pipeline name, dataset name, and destination database type
    p = dlt.pipeline(
        pipeline_name="hubspot",
        dataset_name="hubspot_dataset",
        destination="duckdb",
        dev_mode=False,
    )
    # To load data from pipelines in "deals" endpoint
    load_data = hubspot().with_resources("pipelines_deals", "stages_timing_deals")
    load_info = p.run(load_data)
    print(load_info)


def load_web_analytics_events(
    object_type: THubspotObjectType, object_ids: List[str]
) -> None:
    """
    This function loads web analytics events for a list objects in `object_ids` of type `object_type`

    Returns:
        None
    """

    # Create a DLT pipeline object with the pipeline name, dataset name, and destination database type
    p = dlt.pipeline(
        pipeline_name="hubspot",
        dataset_name="hubspot_dataset",
        destination="duckdb",
        dev_mode=False,
    )

    # you can get many resources by calling this function for various object types
    resource = hubspot_events_for_objects(object_type, object_ids)
    # and load them together passing resources in the list
    info = p.run([resource])

    # Print information about the pipeline run
    print(info)


if __name__ == "__main__":
    load_crm_data()
    load_crm_data_with_history()
    load_crm_objects_with_custom_properties()
    load_pipelines()
    load_crm_data_with_soft_delete()
    load_web_analytics_events("company", ["7086461639", "7086464459"])
