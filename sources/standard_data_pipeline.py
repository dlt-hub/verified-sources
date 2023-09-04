import dlt
from standard.inbox import inbox_source


def from_standard_inbox() -> None:
    
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name="standard_inbox",
        destination="duckdb",
        dataset_name="standard_inbox_data",
        full_refresh=True,
    )

    data_source = inbox_source(
        attachments=True, filter_by_mime_type=("application/pdf",)
    )
    data_resource = data_source.resources["attachments"]
    # run the pipeline with your parameters
    load_info = pipeline.run(data_resource)
    # pretty print the information on data that was loaded
    print(load_info)


if __name__ == "__main__":
    from_standard_inbox()
