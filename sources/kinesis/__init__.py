"""Kinesis source."""
import json
from typing import Any, Dict, Iterable, Optional

import dlt
from dlt.common import pendulum
from dlt.common.configuration.specs import AwsCredentials
from dlt.common.time import ensure_pendulum_datetime
from dlt.common.typing import TDataItem


@dlt.resource(
    primary_key=("_shard_id", "_seq_no"),
    selected=False,
)
def read_kinesis_stream(
    stream_name: Optional[str] = dlt.secrets.value,
    credentials: AwsCredentials = dlt.secrets.value,
    last_ts: Optional[dlt.sources.incremental] = None,  # type: ignore[type-arg]
    chunk_size: int = 1000,
) -> Iterable[TDataItem]:
    session = credentials._to_botocore_session()
    kinesis_client = session.create_client("kinesis")

    shards_list = kinesis_client.list_shards(StreamName=stream_name)
    shards = shards_list["Shards"]
    while next_token := shards_list.get("NextToken"):
        shards_list = kinesis_client.list_shards(NextToken=next_token)
        shards.extend(shards_list["Shards"])

    shard_iterators = []
    for shard in shards:
        shard_id = shard["ShardId"]
        iterator_params: Dict[str, Any]
        if last_ts is None or not last_ts.last_value:
            iterator_params = dict(
                ShardIteratorType="TRIM_HORIZON"
            )  # Fetch all records from the beginning
        else:
            last_ts_value = ensure_pendulum_datetime(last_ts.last_value)
            iterator_params = dict(
                ShardIteratorType="AT_TIMESTAMP", Timestamp=last_ts_value
            )

        shard_iterator = kinesis_client.get_shard_iterator(
            StreamName=stream_name, ShardId=shard_id, **iterator_params
        )
        shard_iterators.append((shard_id, shard_iterator["ShardIterator"]))

    records = []
    for shard_id, main_shard_iterator in shard_iterators:
        shard_iterator = main_shard_iterator
        while True:
            records_response = kinesis_client.get_records(
                ShardIterator=shard_iterator, Limit=10000
            )
            shard_iterator = records_response["NextShardIterator"]

            for record in records_response["Records"]:
                sequence_number = record["SequenceNumber"]
                timestamp = record["ApproximateArrivalTimestamp"]
                partition = record["PartitionKey"]
                content = record["Data"]

                records.append(
                    {
                        "_shard_id": shard_id,
                        "_seq_no": sequence_number,
                        "_kinesis_ts": timestamp,
                        "_kinesis_partition": partition,
                        "stream_name": stream_name,
                        "data": content,
                    }
                )
                if len(records) >= chunk_size:
                    yield records
                    records = []
            # If we are 0ms behind the latest record we can break out of the loop
            if not records_response.get("MillisBehindLatest", 0):
                break
    if records:
        yield records
