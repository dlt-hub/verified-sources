import dlt
from affinity import ListReference, source


def load_affinity_data() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="affinity_pipeline", destination="duckdb", dev_mode=True
    )
    data = source(
        list_refs=[
            ListReference(248283),
            ListReference(247888, 1869904),
        ]
    )
    # data.add_limit(1)
    info = pipeline.run(data, refresh="drop_sources")
    print(info)


if __name__ == "__main__":
    load_affinity_data()
