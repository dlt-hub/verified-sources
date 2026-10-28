"""Captures pgoutput messages into `pgoutput_messages.json` used by credential-free tests.

Run against a Postgres with `wal_level=logical` configured in `sources.pg_replication.credentials`:
`uv run python -m tests.pg_replication.capture_messages`
"""

import json
import os
import select
import time
from typing import Any, Dict, List, Sequence, Tuple

import psycopg2
from psycopg2.extras import LogicalReplicationConnection

import dlt
from dlt.sources.credentials import ConnectionStringCredentials

from sources.pg_replication.schema_types import SESSION_OPTIONS

SCHEMA = "capture"
BIG = "x" * 2500

SCENARIOS: Dict[str, Tuple[str, Sequence[str], Sequence[str]]] = {
    "basic": (
        """
        CREATE TYPE {s}.mood AS ENUM ('sad', 'ok');
        CREATE TABLE {s}.items (
            id int PRIMARY KEY, name text, amount numeric(10,2), ts timestamptz, flag bool,
            doc jsonb, tags text[], mood {s}.mood, note varchar(20), data bytea, day date
        );
        """,
        [
            "INSERT INTO {s}.items VALUES (1, 'one', 1.25, '2024-01-02 10:11:12.5+02', true,"
            " '{{\"a\": [1, 2]}}', '{{a,\"b c\",NULL}}', 'ok', 'n1', '\\xdead', '2024-01-02'),"
            " (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'infinity')",
            "UPDATE {s}.items SET name = 'uno' WHERE id = 1",
            "DELETE FROM {s}.items WHERE id = 2",
        ],
        ["items"],
    ),
    "pk_update": (
        "CREATE TABLE {s}.items (id int PRIMARY KEY, name text);",
        [
            "INSERT INTO {s}.items VALUES (1, 'one')",
            "UPDATE {s}.items SET id = 10 WHERE id = 1",
        ],
        ["items"],
    ),
    "toast_default": (
        "CREATE TABLE {s}.items (id int PRIMARY KEY, small text, big text);"
        "ALTER TABLE {s}.items ALTER COLUMN big SET STORAGE EXTERNAL;",
        [
            "INSERT INTO {s}.items VALUES (1, 'a', '" + BIG + "')",
            "UPDATE {s}.items SET small = 'b' WHERE id = 1",
        ],
        ["items"],
    ),
    "toast_full": (
        "CREATE TABLE {s}.items (id int PRIMARY KEY, small text, big text);"
        "ALTER TABLE {s}.items ALTER COLUMN big SET STORAGE EXTERNAL;"
        "ALTER TABLE {s}.items REPLICA IDENTITY FULL;",
        [
            "INSERT INTO {s}.items VALUES (1, 'a', '" + BIG + "')",
            "UPDATE {s}.items SET small = 'b' WHERE id = 1",
        ],
        ["items"],
    ),
    "mid_txn_relation": (
        "CREATE TABLE {s}.x (id int); CREATE TABLE {s}.y (id int);",
        [
            "INSERT INTO {s}.x VALUES (1); INSERT INTO {s}.y VALUES (1);",
            "BEGIN; INSERT INTO {s}.y VALUES (2); ALTER TABLE {s}.x ADD COLUMN c int;"
            " INSERT INTO {s}.x VALUES (2, 2); COMMIT;",
        ],
        ["x", "y"],
    ),
    "dlt_table": (
        "CREATE TABLE {s}._dlt_items_s_snap (id int); CREATE TABLE {s}.items (id int);",
        [
            "INSERT INTO {s}._dlt_items_s_snap VALUES (1); INSERT INTO {s}.items VALUES (1);"
        ],
        ["_dlt_items_s_snap", "items"],
    ),
}


def capture(
    dsn: str, name: str, ddl: str, statements: Sequence[str], tables: Sequence[str]
) -> List[List[Any]]:
    slot = pub = f"capture_{name}"
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE; CREATE SCHEMA {SCHEMA};")
    cur.execute(ddl.format(s=SCHEMA))
    published = ", ".join(f"{SCHEMA}.{table}" for table in tables)
    cur.execute(f"CREATE PUBLICATION {pub} FOR TABLE {published};")
    cur.execute("SELECT pg_create_logical_replication_slot(%s, 'pgoutput');", (slot,))
    try:
        for statement in statements:
            cur.execute(statement.format(s=SCHEMA))
        cur.execute("SELECT pg_current_wal_flush_lsn() - '0/0';")
        upto_lsn = int(cur.fetchone()[0])
        rep_conn = psycopg2.connect(
            dsn,
            connection_factory=LogicalReplicationConnection,
            options=SESSION_OPTIONS,
        )
        rep_cur = rep_conn.cursor()
        rep_cur.start_replication(
            slot_name=slot,
            decode=False,
            options={"publication_names": pub, "proto_version": "1"},
        )
        messages: List[List[Any]] = []
        in_transaction = False
        started_at = time.monotonic()
        while time.monotonic() - started_at < 30:
            msg = rep_cur.read_message()
            if msg:
                op = chr(msg.payload[0])
                in_transaction = op != "C" and (in_transaction or op == "B")
                messages.append([msg.data_start, msg.payload.hex()])
                continue
            if not in_transaction and rep_cur.wal_end >= upto_lsn:
                break
            rep_cur.send_feedback(reply=True, force=True)
            select.select([rep_cur], [], [], 0.5)
        rep_conn.close()
        return messages
    finally:
        cur.execute("SELECT pg_drop_replication_slot(%s);", (slot,))
        cur.execute(f"DROP PUBLICATION {pub}; DROP SCHEMA {SCHEMA} CASCADE;")
        conn.close()


if __name__ == "__main__":
    dsn = dlt.secrets.get(
        "sources.pg_replication.credentials", ConnectionStringCredentials
    ).to_native_representation()
    captured = {name: capture(dsn, name, *spec) for name, spec in SCENARIOS.items()}
    with open(
        os.path.join(os.path.dirname(__file__), "pgoutput_messages.json"),
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(captured, f, indent=1)
