import dlt
from pipedrive import pipedrive_source


def load_pipedrive():
    """Constructs a pipeline that will load all pipedrive data"""
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(pipeline_name='pipedrive', destination='bigquery', dataset_name='pipedrive_data')
    load_info = pipeline.run(pipedrive_source(munge_custom_fields=True))
    print(load_info)


def load_selected_data():
    """Shows how to load just selected tables using `with_resources`"""
    pipeline = dlt.pipeline(pipeline_name='pipedrive', destination='postgres', dataset_name='pipedrive_data')
    load_info = pipeline.run(pipedrive_source().with_resources("products", "deals", "dealFields", "deals_participants"))
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
    print(pipedrive_data.resources["deals_participants"])
    print()
    # alternatively
    print(pipedrive_data.deals_participants)


if __name__ == "__main__" :
    # run our main example
    # load_pipedrive()
    # load selected tables and display resource info
    load_selected_data()

