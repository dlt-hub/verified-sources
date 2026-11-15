# Kafka tests

- `test_kafka_source.py` needs the local Kafka, the other tests run without it:
  `tests/kafka/certs/generate.sh && docker compose -f tests/kafka/docker-compose.yml up -d --wait`.
  It has a `PLAINTEXT` listener on 9092, `SASL_PLAINTEXT` with PLAIN and SCRAM-SHA-512 on 9094 and
  `SSL` with client certificates on 9095. A listener named `SSL` makes the image configure SSL
  itself from `/etc/kafka/secrets`, so it is named `MTLS`. The certificates are generated and not
  committed.
- The compose file also runs a Schema Registry (`confluentinc/cp-schema-registry`) on 8081. It
  stores schemas in Kafka through the `INTERNAL` listener on 19092, advertised as `kafka:19092`: the
  other listeners advertise `localhost`. The image has no `curl`, its healthcheck uses bash.
  The registry rejects a JSON Schema version that adds a property to an open content model, the
  evolution tests close it with `additionalProperties: false`.
- CI starts the local Kafka in the Linux job of `test_on_local_destinations.yml` when files of the
  source or its tests change. The local tests are skipped on Windows and in jobs without duckdb.
- The coordinator writes transaction markers after `abort_transaction` and
  `commit_transaction` return. Plain messages produced right after them can come first.
  `test_local_aborted_transactions` compares values, not offsets.
- Test producers are idempotent. Right after a topic is created, a retried batch of a producer
  without idempotence lands after later batches, and timestamps no longer grow with offsets.
- `test_local_compacted_topic` waits up to 90 s for the log cleaner, which backs off for 15 s
  between runs.
- Run the tests with the oldest and newest `confluent-kafka` and `fastavro`:
  `uv run --with confluent-kafka==2.13.2 --with fastavro==1.7.4 pytest tests/kafka`.
