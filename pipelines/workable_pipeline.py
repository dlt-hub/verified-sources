import dlt

from workable import workable_source


if __name__ == "__main__":
    pipeline = dlt.pipeline(pipeline_name='workable', destination='duckdb', dataset_name='workable_data')
    # run the pipeline with your parameters
    load_info = pipeline.run(workable_source(fetch=True))
    # pretty print the information on data that was loaded
    print(load_info)
