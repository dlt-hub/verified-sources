#!/bin/bash

set -e

docker exec dlt_postgres_db \
    psql -x -U loader -d dlt_data \
    -c "select *,
               pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn))         as replicationSlotLag,
               pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) as confirmedLag
        from pg_replication_slots;"