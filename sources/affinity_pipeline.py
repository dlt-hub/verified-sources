import dlt
from affinity import ListReference, source


def load_players_online_status() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="affinity_pipeline",
        destination="duckdb",
        dev_mode=True
    )
    data = source(
        list_refs=[
            ListReference(248283),

            # Following yields: {"errors":[{"message":"Saved view must be a sheet saved view.","code":"not-found"}]}
            # ListReference(247888, 1696853)
            ListReference(247888, 1869904)

        ]
    )
    #data.add_limit(1)
    info = pipeline.run(data, refresh="drop_sources")
    print(info)

if __name__ == "__main__":
    # run our main example
    load_players_online_status()
