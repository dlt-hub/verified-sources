import dlt
from shopify import shopify_source


def load_shopify():
    """Constructs a pipeline that will load all shopify data"""
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(pipeline_name='shopify', destination='bigquery', dataset_name='shopify_data')
    load_info = pipeline.run(shopify_source())
    print(load_info)


def load_selected_data():
    """Shows how to load just selected tables using 'with_resources'"""
    pipeline = dlt.pipeline(pipeline_name='shopify', destination='postgres', dataset_name='shopify_data')
    load_info = pipeline.run(shopify_source().with_resources('customers', 'events', 'marketing_events', 'draft_orders'))
    print(load_info)
    # just to show how to access resources within source
    shopify_data = shopify_source()
    # print source info
    print(shopify_data)
    print()
    # list resource names
    print(shopify_data.resources.keys())
    print()
    # print 'locations' resource info
    print(shopify_data.resources['locations'])
    print()
    # alternatively
    print(shopify_data.locations)


if __name__ == "__main__" :
    # run our main example
    load_shopify()
    # load selected tables and display resource info
    # load_selected_data()
