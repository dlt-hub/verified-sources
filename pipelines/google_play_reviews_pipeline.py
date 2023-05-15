from typing import List, Any

import dlt
from google_play_reviews import google_play_reviews_source

def load_google_play_review() -> None:
    """load google play review pipeline"""
    pipeline = dlt.pipeline(
        pipeline_name="google_play",
        destination="duckdb",
        dataset_name="google_play_dataset",
        full_refresh=True
    )
    info = pipeline.run(google_play_reviews_source(
        package_name="com.facebook.katana"
    ).with_resources("review_and_rating"))
    print(info)

if __name__ == "__main__":
    load_google_play_review()
