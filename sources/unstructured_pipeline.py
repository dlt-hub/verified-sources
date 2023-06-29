import dlt
from .unstructured import unstructured_source, unstructured_resource


if __name__ == "__main__":
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name='unstructured', destination='duckdb', dataset_name='unstructured_data'
    )

    # print credentials by running the resource
    data = list(unstructured_resource())

    # print the data yielded from resource
    print(data)
    exit()

    # run the pipeline with your parameters
    load_info = pipeline.run(unstructured_source())

    # pretty print the information on data that was loaded
    print(load_info)
