import dlt
from affinity import source


def load_players_online_status() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="affinity_pipeline",
        destination="duckdb",
        dev_mode=True
    )
    data = source()
    data.add_limit(2)
    info = pipeline.run(data)
    print(info)

if __name__ == "__main__":
    # run our main example
    load_players_online_status()
