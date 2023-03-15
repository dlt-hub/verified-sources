import dlt

from hubspot import hubspot


def load_without_events():
    """
    This function loads data from HubSpot into a PostgreSQL database without enabling company events.

    Returns:
        None
    """

    # Create a DLT pipeline object with the pipeline name, dataset name, and destination database type
    p = dlt.pipeline(pipeline_name='hubspot',
                     dataset_name='hubspot',
                     destination='postgres',
                     full_refresh=False)

    # Run the pipeline with the HubSpot source connector
    info = p.run(
        hubspot()
    )

    # Print information about the pipeline run
    print(info)


def load_with_company_events():
    """
    This function loads data from HubSpot into a PostgreSQL database with company and contacts events selected.

    Returns:
        None
    """

    # Create a DLT pipeline object with the pipeline name, dataset name, and destination database type
    p = dlt.pipeline(pipeline_name='hubspot',
                     dataset_name='hubspot',
                     destination='postgres',
                     full_refresh=False)

    source = hubspot()

    source.companies_events.selected = True
    source.contacts_events.selected = True

    # Run the pipeline with the HubSpot source connector and enable company events
    info = p.run(source)

    # Print information about the pipeline run
    print(info)


if __name__ == "__main__":

    # Call the functions to load HubSpot data into the database with and without company events enabled
    load_without_events()
    load_with_company_events()
