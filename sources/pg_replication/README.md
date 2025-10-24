# Postgres replication
[Postgres](https://www.postgresql.org/) is one of the most popular relational database management systems. This verified source uses Postgres' replication functionality to efficiently process changes in tables (a process often referred to as _Change Data Capture_ or CDC). It uses [logical decoding](https://www.postgresql.org/docs/current/logicaldecoding.html) and the standard built-in `pgoutput` [output plugin](https://www.postgresql.org/docs/current/logicaldecoding-output-plugin.html).

Resources that can be loaded using this verified source are:

| Name                 | Description                                     |
|----------------------|-------------------------------------------------|
| replication_resource | Load published messages from a replication slot |
| init_replication     | Initialize replication and optionally return snapshot resources for initial data load  |

## Initialize the pipeline

```bash
dlt init pg_replication duckdb
```

This uses `duckdb` as destination, but you can choose any of the supported [destinations](https://dlthub.com/docs/dlt-ecosystem/destinations/).

## Set up user

The Postgres user needs to have the `LOGIN` and `REPLICATION` attributes assigned:

```sql
CREATE ROLE replication_user WITH LOGIN REPLICATION;
```

It also needs `CREATE` privilege on the database:

```sql
GRANT CREATE ON DATABASE dlt_data TO replication_user;
```

If not a superuser, the user must have ownership of the tables that need to be replicated:

```sql
ALTER TABLE your_table OWNER TO replication_user;  
```


### Set up RDS
1. You must enable replication for RDS Postgres instance via **Parameter Group**: https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_PostgreSQL.Replication.ReadReplicas.html
2. `WITH LOGIN REPLICATION;` does not work on RDS, instead do:
```sql
GRANT rds_replication TO replication_user;
```
3. Do not fallback to non SSL connection by setting connection parameters:
```toml
sources.pg_replication.credentials="postgresql://loader:password@host.rds.amazonaws.com:5432/dlt_data?sslmode=require&connect_timeout=300"
```


## Add credentials
1. Open `.dlt/secrets.toml`.
2. Enter your Postgres credentials:

    ```toml
    [sources.pg_replication]
    credentials="postgresql://replication_user:<<password>>@localhost:5432/dlt_data"
    ```
3. Enter credentials for your chosen destination as per the [docs](https://dlthub.com/docs/dlt-ecosystem/destinations/).

## Run the pipeline

1. Install the necessary dependencies by running the following command:

   ```bash
   pip install -r requirements.txt
   ```

1. Now the pipeline can be run by using the command:

   ```bash
   python pg_replication_pipeline.py
   ```

1. To make sure that everything is loaded as expected, use the command:

   ```bash
   dlt pipeline pg_replication_pipeline show
   ```