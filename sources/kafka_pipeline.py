from typing import Any, Dict

import confluent_kafka  # type: ignore
import dlt

try:
    from .kafka import kafka_consumer  # type: ignore
except ImportError:
    from kafka import kafka_consumer


def load_from_several_topics() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="kafka_pipeline",
        destination="duckdb",
        dataset_name="kafka_messages",
    )

    data = kafka_consumer(["purchases", "books"])

    info = pipeline.run(data)
    print(info)


def load_with_custom_processor() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="kafka_pipeline",
        destination="duckdb",
        dataset_name="kafka_messages",
    )

    def custom_msg_processor(msg: confluent_kafka.Message) -> Dict[str, Any]:
        """Convert the given message before loading into the destination.

        Args:
            msg (confluent_kafka.Message): Message read from the cluster.

        Returns:
            dict: converted message.
        """
        return {
            "_kafka": {
                "topic": msg.topic(),  # required field
                "key": msg.key().decode("utf-8"),
                "partition": msg.partition(),
            },
            "data": msg.value().decode("utf-8"),
        }

    data = kafka_consumer("books", msg_processor=custom_msg_processor)

    info = pipeline.run(data)
    print(info)


def load_starting_from_date() -> None:
    from pendulum import datetime

    pipeline = dlt.pipeline(
        pipeline_name="kafka_pipeline",
        destination="duckdb",
        dataset_name="kafka_messages",
    )

    from_date = datetime(2023, 12, 15)
    data = kafka_consumer("books", start_from=from_date)

    info = pipeline.run(data)
    print(info)


if __name__ == "__main__":
    # load_from_several_topics()
    # load_with_custom_processor()
    load_starting_from_date()
