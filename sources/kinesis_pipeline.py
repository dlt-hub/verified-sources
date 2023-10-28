import os

import dlt
from dlt.common import pendulum, json
from dlt.sources import TDataItem

try:
    from .kinesis import kinesis_stream  # type: ignore
except ImportError:
    from kinesis import kinesis_stream


def load_kinesis() -> None:
    """Use the kinesis source to load all messages from all shards sent in last hour"""

    pipeline = dlt.pipeline(
        pipeline_name="kinesis_pipeline",
        destination="duckdb",
        dataset_name="kinesis",
        full_refresh=True,
    )
    # the resource below will take its name from the stream name, it can be used multiple times
    # by default it assumes that Data is json and parses it, here we disable that to just get bytes in **data** elements of the message
    dlt_ci_kinesis_stream = kinesis_stream(
        "dlt_ci_kinesis_source",
        parse_json=False,
        initial_at_timestamp=pendulum.now().subtract(hours=1),
    )
    info = pipeline.run(dlt_ci_kinesis_stream)
    print(info)
    message_counts = pipeline.last_trace.last_normalize_info.row_counts
    if "dlt_ci_kinesis_source" not in message_counts:
        print("No messages in kinesis")
    else:
        print(pipeline.last_trace.last_normalize_info)

    # kinesis is incremental. this will get only new messages
    info = pipeline.run(dlt_ci_kinesis_stream)
    message_counts = pipeline.last_trace.last_normalize_info.row_counts
    if "dlt_ci_kinesis_source" not in message_counts:
        print("No messages in kinesis")
    else:
        print(pipeline.last_trace.last_normalize_info)


def decode_kinesis_messages() -> None:
    """Adds transform with simple message decoder that parses json optionally"""

    pipeline = dlt.pipeline(
        pipeline_name="kinesis_pipeline",
        destination="duckdb",
        dataset_name="kinesis_parsed",
        full_refresh=True,
    )

    dlt_ci_kinesis_stream = kinesis_stream(
        "dlt_ci_kinesis_source",
        parse_json=False,
        initial_at_timestamp=pendulum.now().subtract(hours=3),
    )

    def _parse_json(item: TDataItem) -> TDataItem:
        try:
            item.update(json.loadb(item["data"]))
        except Exception:
            pass
        return item

    info = pipeline.run(dlt_ci_kinesis_stream.add_map(_parse_json))
    print(info)


def stream_kinesis_messages() -> None:
    """Read kinesis messages and send them somewhere without using pipeline."""

    # get 1 message in chunk to have transactional stream. dlt will update incremental state after each message
    dlt_ci_kinesis_stream = kinesis_stream(
        "dlt_ci_kinesis_source",
        parse_json=False,
        initial_at_timestamp=pendulum.now().subtract(hours=3),
        chunk_size=1,
    )

    # we enable the state management so we can have incremental loads
    from dlt.common.configuration.container import Container
    from dlt.common.pipeline import StateInjectableContext

    STATE_FILE = "dlt_ci_kinesis_source.state.json"

    # load the state if it exists
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "rb") as f:
            state = json.typed_loadb(f.read())
    else:
        # provide new state
        state = {}

    with Container().injectable_context(
        StateInjectableContext(state=state)
    ) as managed_state:
        # dlt resources/source is just an iterator
        for message in dlt_ci_kinesis_stream:
            # here you can send the message somewhere
            print(message)
            # save state after each message to have full transaction load
            # dynamodb is also OK
            with open(STATE_FILE, "wb") as f:
                json.typed_dump(managed_state.state, f)
            print(managed_state.state)


if __name__ == "__main__":
    # decode_kinesis_messages()
    stream_kinesis_messages()
