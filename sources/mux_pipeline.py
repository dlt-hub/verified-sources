import dlt
from mux import mux_source


def load_yesterday_video_views() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="mux", destination="duckdb", dataset_name="mux_data"
    )
    load_info = pipeline.run(mux_source())
    print(load_info)


if __name__ == "__main__":
    load_yesterday_video_views()
