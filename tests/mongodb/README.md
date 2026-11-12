# MongoDB tests

- `test_local_*` need the local MongoDB: `docker compose -f tests/mongodb/docker-compose.yml up -d`.
  It runs MongoDB 7 because 8.0 and later crash at start on Linux 6.19 and newer, like GitHub runners
  ([SERVER-121912](https://jira.mongodb.org/browse/SERVER-121912)).
- Incremental and ordering tests run only on the local MongoDB. The other tests in
  `test_mongodb_source.py` read samples of the `sample_mflix` dataset from the `connection_url` in
  `sources/.dlt/secrets.toml`, `test_all_resources` loads all of it.
- Run the `pyarrow` backend tests with the oldest and newest `pymongoarrow`:
  `uv run --with pymongoarrow==1.16.1 pytest tests/mongodb`.
