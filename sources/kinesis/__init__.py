"""Kinesis source."""
import json
from typing import Iterable, Optional

import dlt
from dlt.common import pendulum
from dlt.common.configuration.specs import AwsCredentials
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
        if last_ts is None or not last_ts.last_value:
            iterator_params = dict(
                ShardIteratorType="TRIM_HORIZON"
            )  # Fetch all records from the beginning
        else:
            iterator_params = dict(
                ShardIteratorType="AT_TIMESTAMP", Timestamp=last_ts.last_value
            )

        shard_iterator = kinesis_client.get_shard_iterator(
            StreamName=stream_name, ShardId=shard_id, **iterator_params
        )
        shard_iterators.append((shard_id, shard_iterator["ShardIterator"]))

    records = []
    max_empty_responses = 10
    empty_responses = 0
    for shard_id, main_shard_iterator in shard_iterators:
        shard_iterator = main_shard_iterator
        while shard_iterator is not None:
            records_response = kinesis_client.get_records(
                ShardIterator=shard_iterator, Limit=10000
            )
            shard_iterator = records_response["NextShardIterator"]

            # The shard will allways return NextShardIterator, even if there are no records, as
            # kinesis is a stream and new records can be added at any time. We stop the loop if
            # there are no records for a while.
            if not records_response["Records"]:
                empty_responses += 1
                if empty_responses >= max_empty_responses:
                    break

            for record in records_response["Records"]:
                data = json.loads(record["Data"])
                if isinstance(data, str):
                    data = json.loads(data)
                sequence_number = record["SequenceNumber"]
                timestamp = record["ApproximateArrivalTimestamp"]
                data["_shard_id"] = shard_id
                data["_seq_no"] = sequence_number
                data["_kinesis_ts"] = timestamp
                data["stream_name"] = stream_name

                records.append(data)
                if len(records) >= chunk_size:
                    yield records
                    records = []
    if records:
        yield records
