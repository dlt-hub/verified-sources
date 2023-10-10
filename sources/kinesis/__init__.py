import time
import boto3
import json
import dlt

from dlt.common import pendulum

kinesis_client = boto3.client('kinesis')


# def last_seq_no_func(item, last_value=None):
#     if last_value is None:
#         last_value = {}
#     else:
#         last_value = dict(last_value)
#     # print(item["_shard_id"])
#     # print(item["_seq_no"])
#     last_value[item["_shard_id"]] = item["_seq_no"]
#     return last_value


# def get_shard_iterator(stream_name, shard_id, sequence_number=None):
#     if sequence_number:
#         print(f"SEQUENCE + ITER:{shard_id}:{sequence_number}")
#         return kinesis_client.get_shard_iterator(
#             StreamName=stream_name,
#             ShardId=shard_id,
#             ShardIteratorType='AFTER_SEQUENCE_NUMBER',
#             StartingSequenceNumber=sequence_number
#         )['ShardIterator']
#     else:
#         return kinesis_client.get_shard_iterator(
#             StreamName=stream_name,
#             ShardId=shard_id,
#             ShardIteratorType='TRIM_HORIZON'
#         )['ShardIterator']
    
def get_shard_iterator(stream_name, shard_id, timestamp=None):
    if timestamp:
        # print(f"SEQUENCE + ITER:{shard_id}:{sequence_number}")
        return kinesis_client.get_shard_iterator(
            StreamName=stream_name,
            ShardId=shard_id,
            ShardIteratorType='AT_TIMESTAMP',
            Timestamp=timestamp
        )['ShardIterator']
    else:
        return kinesis_client.get_shard_iterator(
            StreamName=stream_name,
            ShardId=shard_id,
            ShardIteratorType='TRIM_HORIZON'
        )['ShardIterator']

@dlt.resource(primary_key=("_shard_id", "_seq_no"), selected=False)
def read_kinesis_stream(stream_name, last_ts = dlt.sources.incremental("_kinesis_ts", initial_value=pendulum.now().subtract(seconds=2))):
    shards = kinesis_client.list_shards(StreamName=stream_name)['Shards']
    shard_iterators = []

    for shard in shards:
        shard_id = shard['ShardId']
        # sequence_number = last_seq.last_value.get(shard_id) # table.get_item(Key={'ShardId': shard_id})
        # sequence_number = response['Item']['SequenceNumber'] if 'Item' in response else None

        # shard_iterator = get_shard_iterator(stream_name, shard_id, sequence_number)
        shard_iterator = get_shard_iterator(stream_name, shard_id, last_ts.last_value)
        shard_iterators.append((shard_id, shard_iterator))

    while True:
        records = []
        for idx, (shard_id, shard_iterator) in enumerate(shard_iterators):
            records_response = kinesis_client.get_records(ShardIterator=shard_iterator, Limit=100)
            shard_iterator = records_response['NextShardIterator']

            for record in records_response['Records']:
                data = json.loads(record['Data'])
                if isinstance(data, str):
                    data = json.loads(data)
                # print("Received data from Kinesis stream:", data)
                sequence_number = record['SequenceNumber']
                timestamp = record["ApproximateArrivalTimestamp"]
                data["_shard_id"] = shard_id
                data["_seq_no"] = sequence_number
                data["_kinesis_ts"] = timestamp
                
                records.append(data)
            # Update shard_iterators list with the new shard iterator
            shard_iterators[idx] = (shard_id, shard_iterator)

        if not records:
            # no more data
            # print("no more records, not yielding")
            break
        # else:
        #     print(f"received {len(records)} records")
        yield records


@dlt.transformer(table_name=lambda i: i["event"])
def segment_stream(items):
    yield items


if __name__ == '__main__':
    p = dlt.pipeline(pipeline_name="telemetry_pipeline", destination="duckdb", dataset_name="dlt_telemetry")
    while True:
        info = p.run(read_kinesis_stream("dlt_telemetry") | segment_stream)
        if len(info.loads_ids) == 0:
            print("No messages in kinesis")
        else:
            print(info)
        time.sleep(5)

    # info = p.run(read_kinesis_stream("dlt_telemetry"))
    # print(info)

    # print(list(read_kinesis_stream("dlt_telemetry")))
