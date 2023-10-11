import boto3
import json
import dlt

from dlt.common import pendulum
from typing import Optional
from .helpers import KinesisResourceConfiguration


@dlt.resource(
    primary_key=("_shard_id", "_seq_no"),
    selected=False,
    spec=KinesisResourceConfiguration,
)
def read_kinesis_stream(
    stream_name,
    aws_access_key_id: Optional[str] = dlt.secrets.value,
    aws_secret_access_key: Optional[str] = dlt.secrets.value,
    aws_region: Optional[str] = dlt.secrets.value,
    last_ts=dlt.sources.incremental(
        "_kinesis_ts", initial_value=pendulum.now().subtract(seconds=2)
    ),
    chunk_size: int = 1000,
):
    kinesis_client = boto3.client(
        "kinesis",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region,
    )
    shards = kinesis_client.list_shards(StreamName=stream_name)["Shards"]
    shard_iterators = []

    for shard in shards:
        shard_id = shard["ShardId"]
        if timestamp := last_ts.last_value:
            iterator_params = dict(
                ShardIteratorType="AT_TIMESTAMP", Timestamp=timestamp
            )
        else:
            iterator_params = dict(
                ShardIteratorType="TRIM_HORIZON"
            )  # Fetch all records from the beginning
        shard_iterator = kinesis_client.get_shard_iterator(
            StreamName=stream_name, ShardId=shard_id, **iterator_params
        )
        shard_iterators.append((shard_id, shard_iterator["ShardIterator"]))

    records = []
    for shard_id, shard_iterator in shard_iterators:
        # while True:
        records_response = kinesis_client.get_records(
            ShardIterator=shard_iterator, Limit=10000
        )
        for record in records_response["Records"]:
            data = json.loads(record["Data"])
            if isinstance(data, str):
                data = json.loads(data)
            sequence_number = record["SequenceNumber"]
            timestamp = record["ApproximateArrivalTimestamp"]
            data["_shard_id"] = shard_id
            data["_seq_no"] = sequence_number
            data["_kinesis_ts"] = timestamp

            records.append(data)
            if len(records) >= chunk_size:
                yield records
                records = []
    if records:
        yield records
