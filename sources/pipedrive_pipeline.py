import dlt
from pipedrive import pipedrive_source


def load_pipedrive() -> None:
    """Constructs a pipeline that will load all pipedrive data"""
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name="pipedrive", destination="duckdb", dataset_name="pipedrive_data"
    )
    load_info = pipeline.run(pipedrive_source())
    print(load_info)


def load_selected_data() -> None:
    """Shows how to load just selected tables using `with_resources`"""
    pipeline = dlt.pipeline(
        pipeline_name="pipedrive", destination="postgres", dataset_name="pipedrive_data"
    )
    # Use with_resources to select which entities to load
    # Note: `custom_fields_mapping` must be included to translate custom field hashes to corresponding names
    load_info = pipeline.run(
        pipedrive_source().with_resources(
            "products", "deals", "deals_participants", "custom_fields_mapping"
        )
    )
    print(load_info)
    # just to show how to access resources within source
    pipedrive_data = pipedrive_source()
    # print source info
    print(pipedrive_data)
    print()
    # list resource names
    print(pipedrive_data.resources.keys())
    print()
    # print `persons` resource info
    print(pipedrive_data.resources["persons"])
    print()
    # alternatively
    print(pipedrive_data.persons)


def load_from_start_date() -> None:
    """Example to incrementally load activities limited to items updated after a given date"""
    pipeline = dlt.pipeline(
        pipeline_name="pipedrive", destination="duckdb", dataset_name="pipedrive_data"
    )

    # First source configure to load everything except activities from the beginning
    source = pipedrive_source()
    source.resources["activities"].selected = False

    # Another source configured to activities starting at the given date (custom_fields_mapping is included to translate custom field hashes to names)
    activities_source = pipedrive_source(
        since_timestamp="2023-03-01 00:00:00Z"
    ).with_resources("activities", "custom_fields_mapping")

    # Run the pipeline with both sources
    load_info = pipeline.run([source, activities_source])
    print(load_info)


if __name__ == "__main__":
    # run our main example
    # load_pipedrive()
    # load selected tables and display resource info
    # load_selected_data()
    # load activities updated since given date
    load_from_start_date()
