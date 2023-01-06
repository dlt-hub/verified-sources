import dlt
from pipedrive import pipedrive_source


def load_pipedrive():
    """Constructs a pipeline that will load all pipedrive data"""
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(pipeline_name='pipedrive', destination='bigquery', dataset_name='pipedrive_data')
    load_info = pipeline.run(pipedrive_source())
    print(load_info)



if __name__ == "__main__" :
    # run our main example
    load_pipedrive()

