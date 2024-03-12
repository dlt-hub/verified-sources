import dlt

from pg_replication import pg_replication_source, replicated_table


def replicate_single_table() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="pg_replication_pipeline",
        destination="duckdb",
        dataset_name="postgres_table",
    )

    table_resource = replicated_table(
        table_name="your_table",
        schema_name="your_schema",
        init_conf={"persist_snapshot": True},  # this enables an initial load
    )

    info = pipeline.run(table_resource)
    print(info)


def replicate_multiple_tables() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="pg_replication_pipeline",
        destination="duckdb",
        dataset_name="postgres_tables",
    )

    replication_source = pg_replication_source(
        table_names=["table_x", "table_y", "table_z"],
        schema_name="your_schema",
        conf={
            "table_x": {"include_columns": ["col_1", "col_2"]},
            "table_y": {"init_conf": {"publish": "insert"}},
            "table_z": {"init_conf": {"persist_snapshot": True}},
        },
    )

    info = pipeline.run(replication_source)
    print(info)


if __name__ == "__main__":
    replicate_single_table()
    # replicate_multiple_tables()
