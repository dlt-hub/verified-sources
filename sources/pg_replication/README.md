## Prerequisites

The Postgres user needs to have the `LOGIN` and `REPLICATION` attributes assigned:

```sql
CREATE ROLE replication_user WITH LOGIN REPLICATION;
```

It also needs `CREATE` privilege on the database:

```sql
GRANT CREATE ON DATABASE dlt_data TO replication_user;
```
