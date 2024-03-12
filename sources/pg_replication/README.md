## Prerequisites

The Postgres user needs to have the `LOGIN` and `REPLICATION` attributes assigned:

```sql
CREATE ROLE replication_user WITH LOGIN REPLICATION;
```

It also needs `CREATE` privilege on the database:

```sql
GRANT CREATE ON DATABASE dlt_data TO replication_user;
```

## Add credentials
1. Open `.dlt/secrets.toml`.
2. Enter the credentials

    ```toml
    [sources.pg_replication]
    credentials="postgresql://replication_user:<<password>>@localhost:5432/dlt_data"
    ```