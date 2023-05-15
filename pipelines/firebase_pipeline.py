import dlt

from firebase import firebase_source

def load_realtime_database() -> None:
    """loads realtime database pipeline"""
    pipeline = dlt.pipeline(
        pipeline_name="firebase",
        destination="duckdb",
        dataset_name="firebase_realtime_database",
        full_refresh=True
    )
    info = pipeline.run(firebase_source().with_resources("realtime_db"))
    print(info)

if __name__=='__main__':
    load_realtime_database()
