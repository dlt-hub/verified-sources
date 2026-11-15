from datetime import datetime, timezone
from typing import Any, Dict

import confluent_kafka
import dlt

try:
    from .kafka import AvroProcessor, JsonProcessor, kafka_consumer  # type: ignore
except ImportError:
    from kafka import AvroProcessor, JsonProcessor, kafka_consumer


def load_from_several_topics() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="kafka_pipeline",
        destination="postgres",
        dataset_name="kafka_messages",
    )

    data = kafka_consumer(["purchases", "books"])

    info = pipeline.run(data)
    print(info)


def load_with_custom_processor() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="kafka_pipeline",
        destination="postgres",
        dataset_name="kafka_messages",
    )

    def custom_msg_processor(msg: confluent_kafka.Message) -> Dict[str, Any]:
        """Convert the given message before loading into the destination.

        Args:
            msg (confluent_kafka.Message): Message read from the cluster.

        Returns:
            dict: converted message.
        """
        key, value = msg.key(), msg.value()
        return {
            "_kafka": {
                "topic": msg.topic(),  # required field
                "key": key.decode("utf-8") if key is not None else None,
                "partition": msg.partition(),
            },
            # tombstones have no value
            "data": value.decode("utf-8") if value is not None else None,
        }

    data = kafka_consumer("books", msg_processor=custom_msg_processor)

    info = pipeline.run(data)
    print(info)


def load_starting_from_date() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="kafka_pipeline",
        destination="postgres",
        dataset_name="kafka_messages",
    )

    # the first run starts at this date, the next runs continue after the last message read
    from_date = datetime(2023, 12, 15, tzinfo=timezone.utc)
    data = kafka_consumer("books", start_from=from_date)

    info = pipeline.run(data)
    print(info)


def load_json_with_schema_registry() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="kafka_pipeline",
        destination="duckdb",
        dataset_name="kafka_messages",
    )

    # JSON values load as columns, typed with their JSON Schemas when
    # `sources.kafka.schema_registry.url` is set in the config
    data = kafka_consumer("orders", msg_processor=JsonProcessor(backend="pyarrow"))

    info = pipeline.run(data)
    print(info)


def load_avro_with_schema_registry() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="kafka_pipeline",
        destination="duckdb",
        dataset_name="kafka_messages",
    )

    # Avro values load as columns typed with their schemas from the Schema Registry at
    # `sources.kafka.schema_registry.url`, a topic with several record types loads a table
    # per record type
    data = kafka_consumer(
        "orders",
        msg_processor=AvroProcessor(backend="pyarrow", table_per_schema=True),
    )

    info = pipeline.run(data)
    print(info)


if __name__ == "__main__":
    # load_from_several_topics()
    # load_with_custom_processor()
    # load_json_with_schema_registry()
    # load_avro_with_schema_registry()
    load_starting_from_date()
