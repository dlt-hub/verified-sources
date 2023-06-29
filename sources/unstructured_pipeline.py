import dlt
from .unstructured import unstructured_source, filesystem_source


if __name__ == "__main__":
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name='unstructured', destination='duckdb', dataset_name='unstructured_data'
    )

    data = filesystem_source().with_resources("local_folder")
    data_extractor = unstructured_source(data)
    # run the pipeline with your parameters
    load_info = pipeline.run(data_extractor)

    # pretty print the information on data that was loaded
    print(load_info)
