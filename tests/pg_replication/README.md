# Postgres replication tests

`test_pg_replication_source.py` needs Postgres with `wal_level=logical` and a user with the
`REPLICATION` attribute in `sources.pg_replication.credentials`. `tests/postgres/docker-compose.yml`
starts one. Other versions run the same way, e.g.
`docker run -p 5434:5432 -e POSTGRES_USER=loader -e POSTGRES_PASSWORD=loader -e POSTGRES_DB=dlt_data postgres:12 -c wal_level=logical`.

## Seeding

The credential-free tests decode pgoutput messages captured from Postgres 17:

1. Start Postgres with logical decoding, see above.
2. `uv run python -m tests.pg_replication.capture_messages` from the repository root rewrites
   `tests/pg_replication/pgoutput_messages.json` with the messages of its `SCENARIOS`.
3. Update `EXPECTED_SEGMENTS` of `test_helpers.py` when a scenario changes.
