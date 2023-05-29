from typing import Any

import dlt

from firebase import firebase_source

def parse_data(data: dict[Any, Any]) -> dict[Any, Any]:
    # parse given data/API response
    for item in data["discography"]:
        for album in item["albums"]:
            yield {
                "band_name": item["band_name"],
                "album": album["album_name"],
                "year": album["year"]
            }

def load_realtime_database() -> None:
    """loads realtime database pipeline"""
    pipeline = dlt.pipeline(
        pipeline_name="firebase",
        destination="duckdb",
        dataset_name="firebase_realtime_database",
        full_refresh=True
    )
    info = pipeline.run(firebase_source().with_resources("realtime_db").add_map(parse_data))
    print(info)    

if __name__=='__main__':
    load_realtime_database()
