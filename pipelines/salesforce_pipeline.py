import dlt
from salesforce import salesforce_source


def load_salesforce():
    """Constructs a pipeline that will load all salesforce data"""
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(pipeline_name='salesforce', destination='bigquery', dataset_name='salesforce_data')
    load_info = pipeline.run(salesforce_source())
    print(load_info)


def load_selected_data():
    """Shows how to load just selected tables using 'with_resources'"""
    pipeline = dlt.pipeline(pipeline_name='salesforce', destination='postgres', dataset_name='salesforce_data')
    load_info = pipeline.run(salesforce_source().with_resources('account', 'organization'))
    print(load_info)
    # just to show how to access resources within source
    salesforce_data = salesforce_source()
    # print source info
    print(salesforce_data)
    print()
    # list resource names
    print(salesforce_data.resources.keys())
    print()
    # print 'order' resource info
    print(salesforce_data.resources['order'])
    print()
    # alternatively
    print(salesforce_data.order)


if __name__ == "__main__":
    # run our main example
    # load_salesforce()
    # load selected tables and display resource info
    load_selected_data()
