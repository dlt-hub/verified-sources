import uuid
import pytest
from typing import Any
from botocore.exceptions import ClientError
from time import sleep

import dlt
from dlt.common import pendulum, json
from dlt.common.utils import uniq_id
from dlt.common.configuration.specs import AwsCredentials
from dlt.common.configuration.container import Container
from dlt.common.typing import TDataItem
from dlt.common.pipeline import StateInjectableContext

from sources.kinesis import kinesis_stream
from sources.kinesis.helpers import get_shard_iterator, max_sequence_by_shard

from tests.utils import assert_load_info, load_table_counts

KINESIS_STREAM_NAME = "dlt_ci_kinesis_source"


@pytest.fixture(scope="module")
def kinesis_client() -> Any:
    credentials = dlt.secrets.get("sources.kinesis.credentials", AwsCredentials)
    session = credentials._to_botocore_session()
    return session.create_client("kinesis")


def test_kinesis_read(kinesis_client: Any) -> None:
    # parse incoming messages as json
    kinesis_resource = kinesis_stream(
        KINESIS_STREAM_NAME,
        parse_json=False,
        initial_at_timestamp=pendulum.now().subtract(seconds=5),
    )
    # assert resource name
    assert kinesis_resource.name == KINESIS_STREAM_NAME

    def _maybe_parse_json(item: TDataItem) -> TDataItem:
        try:
            item.update(json.loadb(item["data"]))
        except Exception:
            pass
        return item

    test_id = str(uuid.uuid4())
    kinesis_client.put_record(
        StreamName=KINESIS_STREAM_NAME,
        Data=json.dumpb({"key": test_id, "value": "value"}),
        PartitionKey="string_partition",
    )
    # look for message with required key
    messages = [
        msg
        for msg in kinesis_resource.add_map(_maybe_parse_json)
        if msg.get("key", None) == test_id
    ]
    assert len(messages) == 1
    # test message content
    assert "_kinesis" in messages[0]
    assert "_kinesis_msg_id" in messages[0]
    assert "ts" in messages[0]["_kinesis"]


@pytest.mark.skip("this test cannot run in parallel with other tests")
def test_kinesis_max_messages_read(kinesis_client: Any) -> None:
    # sleep for 2 secs so we do not take messages from previous tests
    sleep(2.0)

    test_id = str(uuid.uuid4())
    kinesis_client.put_record(
        StreamName=KINESIS_STREAM_NAME,
        Data=json.dumpb({"key": test_id, "index": 1}),
        PartitionKey="string_partition" + uniq_id(),
    )
    kinesis_client.put_record(
        StreamName=KINESIS_STREAM_NAME,
        Data=json.dumpb({"key": test_id, "index": 2}),
        PartitionKey="string_partition" + uniq_id(),
    )
    kinesis_resource = kinesis_stream(
        KINESIS_STREAM_NAME,
        parse_json=True,
        initial_at_timestamp=pendulum.now().subtract(seconds=2),
        max_number_of_messages=1,
        chunk_size=1,
    )

    # inject the state so enable incremental loading
    with Container().injectable_context(StateInjectableContext(state={})):
        # read 1 message
        messages = [msg for msg in kinesis_resource if msg.get("key", None) == test_id]
        assert len(messages) == 1
        # read 2 message
        messages = [msg for msg in kinesis_resource if msg.get("key", None) == test_id]
        assert len(messages) == 1
        # no more messages
        messages = [msg for msg in kinesis_resource if msg.get("key", None) == test_id]
        assert len(messages) == 0


def test_kinesis_incremental(kinesis_client: Any) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="kinesis_test",
        destination="duckdb",
        dataset_name="kinesis_test_data",
        dev_mode=True,
    )
    test_id = str(uuid.uuid4())

    # A binary record
    kinesis_client.put_record(
        StreamName=KINESIS_STREAM_NAME,
        Data=f"binary_stream data {test_id}".encode(),
        PartitionKey="binary_partition" + uniq_id(),
    )

    # A string record
    kinesis_client.put_record(
        StreamName=KINESIS_STREAM_NAME,
        Data=f"sting_stream data {test_id}",
        PartitionKey="string_partition" + uniq_id(),
    )

    # Some json records
    records = []
    for i in range(10):
        data = {"key": f"{test_id}_{i}"}
        data_str = json.dumpb(data)
        record = {
            "Data": data_str,
            "PartitionKey": "json_partition" + uniq_id(),
        }
        records.append(record)

    kinesis_client.put_records(
        StreamName=KINESIS_STREAM_NAME,
        Records=records,
    )

    # collect all messages ids to exclude duplicates
    msg_ids = []

    @dlt.transformer(name="kinesis_records")
    def filter_test_records(items: TDataItem, test_id: str) -> TDataItem:
        filtered = []
        for item in items:
            msg_ids.append(item["_kinesis_msg_id"])
            kinesis_info = item["_kinesis"]
            if kinesis_info["partition"].startswith("json_partition"):
                if test_id.encode() in item["data"]:
                    filtered.append(item)
            elif kinesis_info["partition"].startswith("string_partition"):
                if item["data"].endswith(test_id.encode()):
                    filtered.append(item)
            elif kinesis_info["partition"].startswith("binary_partition"):
                if item["data"].endswith(test_id.encode()):
                    filtered.append(item)
        yield filtered

    kinesis_resource = kinesis_stream(
        KINESIS_STREAM_NAME,
        initial_at_timestamp=pendulum.now().subtract(hours=1),
        parse_json=False,
    ) | filter_test_records(test_id=test_id)

    load_info = pipeline.run(kinesis_resource)

    assert_load_info(load_info)
    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)
    expected_tables = ["kinesis_records"]
    assert set(table_counts.keys()) >= set(expected_tables)
    assert table_counts["kinesis_records"] == 12

    # check if we have any duplicates in message ids
    assert len(msg_ids) == len(set(msg_ids))

    # run again - no messages
    load_info = pipeline.run(kinesis_resource)
    print(pipeline.last_trace)
    assert_load_info(load_info)
    table_counts = load_table_counts(pipeline, *table_names)
    assert table_counts["kinesis_records"] == 12
    assert len(msg_ids) == len(set(msg_ids))

    # put a single record
    # A string record
    kinesis_client.put_record(
        StreamName=KINESIS_STREAM_NAME,
        Data=f"sting_stream data {test_id}",
        PartitionKey="string_partition",
    )
    load_info = pipeline.run(kinesis_resource)
    assert_load_info(load_info, 1)
    assert pipeline.last_trace.last_normalize_info.row_counts["kinesis_records"] == 1
    table_counts = load_table_counts(pipeline, *table_names)
    assert table_counts["kinesis_records"] == 13
    assert len(msg_ids) == len(set(msg_ids))


def test_shard_iterator_types(kinesis_client: Any) -> None:
    shards_list = kinesis_client.list_shards(StreamName=KINESIS_STREAM_NAME)
    shard_id = shards_list["Shards"][0]["ShardId"]
    # trim horizon
    _, shard_params = get_shard_iterator(
        kinesis_client, KINESIS_STREAM_NAME, shard_id, None, None
    )
    assert shard_params["ShardIteratorType"] == "TRIM_HORIZON"
    _, shard_params = get_shard_iterator(
        kinesis_client,
        KINESIS_STREAM_NAME,
        shard_id,
        dlt.sources.incremental("_kinesis", end_value={}),
        None,
    )
    assert shard_params["ShardIteratorType"] == "TRIM_HORIZON"
    _, shard_params = get_shard_iterator(
        kinesis_client,
        KINESIS_STREAM_NAME,
        shard_id,
        dlt.sources.incremental(
            "_kinesis", initial_value={"unk_shard": "ABC"}, end_value={}
        ),
        None,
    )
    assert shard_params["ShardIteratorType"] == "TRIM_HORIZON"
    # we provide message sequence in the init that contains current shard
    # so AFTER_SEQUENCE_NUMBER will be created - the sequence number is invalid so we get an exception
    with pytest.raises(ClientError):
        _, shard_params = get_shard_iterator(
            kinesis_client,
            KINESIS_STREAM_NAME,
            shard_id,
            dlt.sources.incremental(
                "_kinesis", initial_value={shard_id: "123456"}, end_value={}
            ),
            None,
        )

    # at timestamp
    _, shard_params = get_shard_iterator(
        kinesis_client,
        KINESIS_STREAM_NAME,
        shard_id,
        None,
        pendulum.now().subtract(seconds=1),
    )
    assert shard_params["ShardIteratorType"] == "AT_TIMESTAMP"
    _, shard_params = get_shard_iterator(
        kinesis_client, KINESIS_STREAM_NAME, shard_id, None, pendulum.from_timestamp(0)
    )
    assert shard_params["ShardIteratorType"] == "LATEST"


def test_last_value_func() -> None:
    # no last value
    assert max_sequence_by_shard(
        ({"shard_id": "shard-0001", "seq_no": "0001234"}, None)
    ) == {"shard-0001": "0001234"}
    # item newer
    assert max_sequence_by_shard(
        ({"shard_id": "shard-0001", "seq_no": "0001234"}, {"shard-0001": "0001233"})
    ) == {"shard-0001": "0001234"}
    # item older
    assert max_sequence_by_shard(
        ({"shard_id": "shard-0001", "seq_no": "0001232"}, {"shard-0001": "0001233"})
    ) == {"shard-0001": "0001233"}
    # new shard
    assert max_sequence_by_shard(
        ({"shard_id": "shard-0002", "seq_no": "1001232"}, {"shard-0001": "0001233"})
    ) == {"shard-0001": "0001233", "shard-0002": "1001232"}

    # single element converts item into last value
    assert max_sequence_by_shard(
        ({"shard_id": "shard-0001", "seq_no": "0001234"},)
    ) == {"shard-0001": "0001234"}
