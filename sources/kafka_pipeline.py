# from datetime import datetime

import dlt

try:
    from .kafka import kafka_consumer  # type: ignore
except ImportError:
    from kafka import kafka_consumer


# def my_msg_proc(msg):
#     return dict(
#         topic=msg.topic(),
#         key=msg.key().decode("utf-8"),
#         value="no!",
#         partition=msg.partition(),
#     )


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="kafka_pipeline",
        destination="postgres",
        dataset_name="kafka_messages",
        # full_refresh=True,
    )

    # from_date = datetime(2023, 12, 15)

    data = kafka_consumer(["purchases", "books"])
    # data = kafka_consumer(["books"], start_from=from_date)
    # data = kafka_consumer(["purchases", "books"], message_processor=my_msg_proc)

    info = pipeline.run(data)
    print(info)
