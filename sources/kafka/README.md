# Kafka

Maintenance notes of the [Kafka](https://kafka.apache.org/) source. Setup and usage are in the
[dlt docs](https://dlthub.com/docs/dlt-ecosystem/verified-sources/kafka).

## Changelog

### 1.0.0
**Upgrade**: safe from any earlier commit (#306 to #607) after you install
`confluent-kafka>=2.13.2` and `dlt>=1.28.1`. The offsets in the state carry over and the columns do
not change (tested from the last unnumbered commit). The first run also loads the messages that
earlier versions held back. To reload instead, run
`pipeline.run(kafka_consumer(...), refresh="drop_sources")`. Messages that retention deleted are
lost.
- **breaking** `OffsetTracker` is removed. `helpers.py` is split into `helpers/client.py`,
  `helpers/credentials.py` and `helpers/processors.py`. Import `KafkaCredentials` and
  `default_msg_processor` from the `kafka` package
- fixed `kafka_consumer`: a partition with one unread message was not read, and a single new
  message waited for the next one
- fixed `kafka_consumer`: a topic added to `topics` of an existing pipeline failed with `KeyError`
- fixed `kafka_consumer`: runs did not end when a partition ended with a transaction marker and
  another partition kept receiving messages
- fixed `kafka_consumer`: `start_from` after the last message of a partition loaded that message
- fixed `kafka_consumer`: message errors raised `KafkaError`, which `except Exception` does not
  catch. Errors like `TOPIC_AUTHORIZATION_FAILED` ended the run without data and without a failure
- fixed `kafka_consumer`: an unreachable broker made the run hang, and a missing topic failed with
  `KeyError`
- fixed `kafka_consumer`: tombstones and keys or values that are not UTF-8 failed the run.
  Tombstones load `_kafka__data` as null, and such keys and values load as binary
- changed `kafka_consumer`: `start_from` is a lower bound. When the stored offset of a partition is
  later, the partition continues after it
- changed `kafka_consumer`: with `end_time`, the stored offsets are not used or updated, and
  `start_from` is optional
- changed `kafka_consumer`: offsets are not committed to the consumer group. `group_id` and
  `security_protocol` are optional
- changed `kafka_consumer`: offsets deleted by retention and recreated topics are logged as
  warnings. The stored offset moves to the first offset of the partition, so a recreated topic
  loads all its new messages
- changed `default_msg_processor`: timestamps are stdlib `datetime` in UTC with exact
  milliseconds. They are `None` when the broker keeps no timestamps
- new `kafka_consumer`: `request_timeout` of the metadata and offset requests
- new `kafka_consumer`: `schema_registry` with `SchemaRegistryCredentials`. `msg_processor` accepts
  a `BatchProcessor` that converts the messages of one topic and one schema at once
- new `KafkaCredentials`: SSL and mTLS with PEM strings or files, SCRAM, OAuth with OIDC, and any
  librdkafka property in `consumer_config`
- new `KafkaMskIamCredentials`: Amazon MSK with IAM access control
- new `JsonProcessor`: loads JSON values as columns with the `python` or `pyarrow` backend. The JSON
  Schema in the Schema Registry or the schema of a Kafka Connect envelope types the columns
- new `AvroProcessor`: loads Avro values written with the Schema Registry as columns, typed by
  their schema, with the `python` or `pyarrow` backend. It needs `fastavro`, for example from
  `confluent-kafka[avro]`

Versions before 1.0.0 have no `version.py`. `.dlt/.sources` records the commit of a copy.

## Extension points

- Message conversion: pass a `msg_processor` function to `kafka_consumer`. The function must return
  `_kafka.topic`, which names the table. `helpers/processors.py` holds the default function and
  its helpers.
- A message format: subclass `BatchProcessor` of `helpers/processors.py`. `schema_id` groups the
  messages. `table_schema` returns the table and the columns of a schema, and `kafka_consumer`
  emits them as table hints when they change. `__call__` converts a group. `JsonProcessor` and
  `AvroProcessor` are examples.
- Schema types: JSON Schema in `json_schema_fields` of `helpers/json_schema.py`, Avro in
  `avro_fields` of `helpers/avro.py`, and Kafka Connect and Debezium types in `connect_fields` and
  `logical_field` of `helpers/kafka_connect.py`. Value conversions are in `helpers/columns.py`.
- Credentials: when many users need a librdkafka property, add it as a field of `KafkaCredentials`
  in `helpers/credentials.py`. Name the field like the property, with underscores instead of
  periods. Other properties go to `consumer_config`.
- Token based auth: write a subclass of `KafkaCredentials` that sets `oauth_cb`, like
  `KafkaMskIamCredentials`. Put it before `KafkaCredentials` in the `credentials` union of
  `kafka_consumer`. dlt resolves the first spec that has all its required fields.
- Reading: `partition_ranges` and `read_batches` in `helpers/client.py` work with any
  `confluent_kafka.Consumer`.

## Maintenance & Troubleshooting

- A run reads up to the high watermark that each partition had when the run started. Under the
  default `isolation.level=read_committed`, this is the last stable offset. Messages of open
  transactions load on a later run, and messages of aborted transactions do not load (local
  Kafka). To read open transactions, set `"isolation.level": "read_uncommitted"` in
  `consumer_config`. This setting also loads the messages of aborted transactions.
- Since `confluent-kafka` 2.15.1, `consume` waits for `batch_size` messages until `batch_timeout`.
  The last batch of every run waits the full `batch_timeout`
  ([PR 2235](https://github.com/confluentinc/confluent-kafka-python/pull/2235)). A consumer passed
  as `credentials` without `enable.partition.eof` also ends with an empty batch.
- When no message arrives within `batch_timeout`, the run ends early with a warning. The warning
  names the partitions that were not read to the end. The next run continues after the last
  message read. For slow clusters, increase `batch_timeout`.
- Throughput of 300000 messages in 4 partitions to `filesystem`, in messages/s (local Kafka):

  | Processor | Values | `jsonl` | `parquet` |
  |---|---|---|---|
  | `default_msg_processor` | JSON, 198 bytes | 23000 | |
  | `JsonProcessor(backend="python")` | JSON, 198 bytes | 21000 | |
  | `JsonProcessor(backend="pyarrow")` | JSON, 198 bytes | 33000 | 109000 |
  | `AvroProcessor(backend="python")` | Avro, 48 bytes | 19000 | |
  | `AvroProcessor(backend="pyarrow")` | Avro, 48 bytes | 26000 | 83000 |

  Consuming alone runs at about 139000 messages/s.
- Protobuf values are not read. Schema ids of AWS Glue (a `0x03` byte) and Azure Event Hubs are
  not read, and their values are decode errors. `default_msg_processor` loads the Confluent
  framing ([wire format](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format))
  as text.
- When `JsonProcessor` or `AvroProcessor` cannot decode a message, the run fails before the offset
  of that message is stored. The next run starts at that message again. To log and skip such
  messages, set `on_decode_error="skip"`.
- Producers with the Java `LongSerializer` write keys as 8 binary bytes. Keys that are not UTF-8
  and have no Avro schema are decode errors for `JsonProcessor` and `AvroProcessor`.
  `default_msg_processor` loads them as binary.
- Debezium writes the PostgreSQL `infinity` timestamp as 9223372036825200000
  ([Debezium docs](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)).
  No timestamp column holds this value, so `AvroProcessor` reports a decode error.
- `AvroProcessor` does not apply the data contract rules of a schema. Encrypted fields load as
  ciphertext, and transforms and migrations do not run. A warning names the schema
  ([data contracts](https://docs.confluent.io/platform/current/schema-registry/fundamentals/data-contracts.html)).
- The `pyarrow` backend of `JsonProcessor` keeps the Arrow type that a field had in the first batch
  of a run. dlt fails a load when the Arrow type of a column changes within the load (local run).
  A value that does not fit, for example `2.5` after integers, is a decode error. Schema Registry
  types, columns applied to the resource, or the `python` backend prevent the error. Without a
  schema, lists load as nested tables with `python` and as `json` with `pyarrow`.
- A backend switch on a loaded table fails in both directions. Rows get NOT NULL `_dlt_load_id`
  and `_dlt_id` columns, and Arrow tables do not (`test_local_backend_switch`). Before the first
  load, set `add_dlt_load_id` and `add_dlt_id` of `[normalize.parquet_normalizer]`.
  `backend="auto"` switches to `pyarrow` when pyarrow gets installed.
- `start_from` and `end_time` find offsets with `offsets_for_times`: the first offset with a
  timestamp at or after the given time. Windows are exact only when timestamps grow with the
  offsets of a partition. Producers that set `CreateTime` do not guarantee this. Batches retried
  by producers without idempotence do not guarantee it either (local Kafka).
- A stored offset before the first offset of a partition means that retention deleted messages
  before they were read. A stored offset after the end of a partition means that the topic was
  recreated or truncated. dlt logs both, and reads the partition from its first offset (local
  Kafka). When a recreated topic has more messages than the stored offset, dlt cannot detect the
  change, and the run continues after the stored offset.
- Deletes stay in the destination, because the tables are append only. A tombstone loads as a row
  with a null `_kafka__data`, and earlier rows of its key stay (`test_local_incremental_changes`).
  Debezium deletes read with `envelope="kafka_connect"` load as rows too. For the latest value per key,
  deduplicate on `_kafka__key` by the largest `_kafka__offset`, per partition.
- `_kafka_msg_id` is a hash of topic, partition and key. Messages with the same key share it, so it
  is not a primary key. `_kafka__partition` and `_kafka__offset` identify a message in a table.
- A wrong password or a missing client certificate fails with `_TRANSPORT` `Failed to get
  metadata` after `request_timeout`. librdkafka logs the cause, for example
  `SASL authentication error`, to stderr (local Kafka).
- The PyPI wheels have no GSSAPI on Linux. Kerberos needs `confluent-kafka` built from source
  ([install docs](https://github.com/confluentinc/confluent-kafka-python/blob/master/INSTALL.md)).
- Amazon MSK with IAM needs `aws-msk-iam-sasl-signer-python` and Kafka 2.7.1 or later on the
  cluster ([AWS docs](https://docs.aws.amazon.com/msk/latest/developerguide/iam-access-control.html)).
- Azure Event Hubs: use `SASL_SSL` with `PLAIN` on port 9093. The user is `$ConnectionString`, and
  the password is the connection string
  ([Microsoft docs](https://learn.microsoft.com/en-us/azure/event-hubs/azure-event-hubs-apache-kafka-overview)).
