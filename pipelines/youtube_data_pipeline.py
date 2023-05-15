from datetime import date
from typing import List, Any

import dlt
from youtube_data import youtube_data

def load_youtube_data(
    channel_names: List[str],
    start_date: str,
    end_date: str,
    max_results: int
) -> None:
    """construct youtube data pipeline for duckdb"""
    pipeline = dlt.pipeline(
        pipeline_name="youtube_data",
        destination="duckdb",
        dataset_name="youtube_data"
    )

    data = youtube_data(
        channel_names,
        start_date,
        end_date,
        max_results
    )

    info = pipeline.run(data)
    print(info)

if __name__ == "__main__":
    today = date.today()
    channel_names = ["mrbeast"]
    start_date="2022-01-01T00:00:00Z"
    end_date=today.strftime("%Y-%m-%d")+"T00:00:00Z"
    max_results = 2
    load_youtube_data(channel_names, start_date, end_date, max_results)
