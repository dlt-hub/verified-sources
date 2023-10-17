import json

import dlt
import pendulum
from dlt.common.pipeline import LoadInfo
from dlt.common.typing import TDataItem

try:
    from .kinesis import read_kinesis_stream  # type: ignore
except ImportError:
    from kinesis import read_kinesis_stream


def load_kinesis() -> LoadInfo:
    """Use the kinesis source to completely load all streams"""

    @dlt.transformer
    def transform_json(items: TDataItem) -> TDataItem:
        for item in items:
            item["Data"] = json.loads(item["Data"])
        yield items

    pipeline = dlt.pipeline(
        pipeline_name="kinesis_pipeline",
        destination="duckdb",
        dataset_name="kinesis",
    )

    info = pipeline.run(
        read_kinesis_stream(
            "dlt_ci_kinesis_source",
            last_ts=dlt.sources.incremental(
                "_kinesis_ts", initial_value=pendulum.now().subtract(days=1)
            ),
        )
        | transform_json
    )
    if len(info.loads_ids) == 0:
        print("No messages in kinesis")
    return info


if __name__ == "__main__":
    load_kinesis()
