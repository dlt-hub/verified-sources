# Test PostgreSQL instance

This docker compose file will spin up the test postgreSQL instance that can be used to run tests or do ad hoc loading. The `postgres.env` will name the default database `dlt_data`, the sys admin user `loader` and set the password to `loader`. You may also use the `01_init.sql` to create database with more granular permissions.

Below is a relevant `toml` fragment that you can put into `secrets.toml`.
```toml
destination.postgres.credentials="postgres://loader:loader@localhost:5432/dlt_data"
```

To start the instance, from the `tests/postgres` folder
```
docker-compose up --build -d
```

remove `-d` to see the log

To complete wipe out the instance, run
```
docker-compose down -v
```