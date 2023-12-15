from typing import Dict

from confluent_kafka import Consumer, Message, TopicPartition, OFFSET_BEGINNING

import dlt


def default_message_processor(msg: Message) -> Dict:
    """Basic Kafka message processor.

    Args:
        msg (confluent_kafka.Message): A single Kafka message.

    Returns:
        dict: Processed Kafka message.
    """
    return dict(
        key=msg.key().decode("utf-8"),
        value=msg.value().decode("utf-8"),
        partition=msg.partition(),
    )


def init_consumer(credentials: dict, group_id: str) -> Consumer:
    """Initiate a Kafka consumer with the given credentials.

    Args:
        credentials (dict): Kafka consumer credentials.
        group_id (str): Kafka consumer group.

    Returns:
        confluent_kafka.Consumer: A ready to go Kafka consumer.
    """
    config = {
        "bootstrap.servers": credentials["bootstrap"]["servers"],
        "group.id": group_id,
        "security.protocol": credentials["security"]["protocol"],
        "sasl.mechanisms": credentials["sasl"]["mechanisms"],
        "sasl.username": credentials["sasl"]["username"],
        "sasl.password": credentials["sasl"]["password"],
    }
    return Consumer(config)


class OffsetTracker(dict):
    """Object to control offsets of the given topics.

    Tracks all the partitions of the given topics with two params:
    current offset and maximum offset (partition length).

    Args:
        consumer (confluent_kafka.Consumer): Kafka consumer.
        topic_names (list): Names of topics to track.
    """

    def __init__(self, consumer: Consumer, topic_names: list):
        super().__init__()

        self._consumer = consumer
        self._topics = self._read_topics(topic_names)

        # read/init current offsets
        self._cur_offsets = dlt.current.resource_state().setdefault(
            "offsets", {t_name: {} for t_name in topic_names}
        )

        self._init_partition_offsets()

    def _read_topics(self, topic_names):
        """Read the given topics metadata from Kafka.

        Reads all the topics at once, instead of requesting
        each in a separate call. Returns only those needed.

        Args:
            topic_names (list): Names of topics to be read.

        Returns:
            dict: Metadata of the given topics.
        """
        tracked_topics = {}
        topics = self._consumer.list_topics().topics

        for t_name in topic_names:
            tracked_topics[t_name] = topics[t_name]

        return tracked_topics

    def _init_partition_offsets(self):
        """Designate current and maximum offsets for every partition.

        Current offsets are read from the state, if present. Set equal
        to the partition beginning otherwise.
        """
        all_parts = []
        for t_name, topic in self._topics.items():
            self[t_name] = {}

            # init all the topic partitions from the partitions' metadata
            parts = [TopicPartition(t_name, part) for part in topic.partitions]

            # designate current and maximum offsets for every partition
            for i, part in enumerate(parts):
                cur_offset = self._cur_offsets[t_name].get(
                    str(part.partition), OFFSET_BEGINNING
                )

                self[t_name][str(part.partition)] = {
                    "cur": cur_offset,
                    "max": self._consumer.get_watermark_offsets(part)[1],
                }

                parts[i].offset = cur_offset

            all_parts += parts

        # assign the current offsets to the consumer
        self._consumer.assign(all_parts)

    @property
    def has_unread(self) -> bool:
        """Check if there are unread messages in the tracked topics.

        Returns:
            bool: True, if there are messages to read, False if all
                the current offsets are equal to their maximums.
        """
        for parts in self.values():
            for part in parts.values():
                if part["cur"] + 1 < part["max"]:
                    return True

    def update(self, msg: Message):
        """Update partition offset from the given message.

        Args:
            msg (confluent_kafka.Message): A read Kafka message.
        """
        topic = msg.topic()
        partition = str(msg.partition())

        offset = self[topic][partition]
        offset["cur"] = msg.offset()

        self._cur_offsets[topic][partition] = msg.offset()
