# ServiceNow tests

- Unit tests, one module for each source module: `test_client.py`, `test_convert.py`, `test_credentials.py`,
  `test_query.py`, `test_schema.py`, `test_table.py` and `test_servicenow.py`. `FakeInstance` in `utils.py` is a
  fake ServiceNow instance. It answers Table API and CSV export requests with `requests_mock` and evaluates the
  encoded queries that the source sends. The unit tests need no credentials.
- `test_servicenow_source.py`: live tests against the seeded test instance. They only read data.

## Test instance

The tests use a Personal Developer Instance (PDI) from https://developer.servicenow.com. It hibernates when idle and
is reclaimed after 10 days without a login. Wake it up before you run the tests.

1. Request a PDI and note its name, for example `dev12345`.
2. Create a user for the tests with the `admin` role: the seed creates a table and its fields. For basic
   authentication also grant `snc_basic_auth_api_access`.
3. Put the credentials in `sources/.dlt/secrets.toml`. CI reads the same keys from the secrets vault:

   ```toml
   [sources.servicenow.credentials]
   instance = "dev12345"
   username = "..."
   password = "..."
   ```

## Seeding

Run from the repository root. The script resolves credentials from `sources/.dlt` like the tests do:

```sh
uv run python -m tests.servicenow.seed.seed all  # or deploy | load | capture
```

- `deploy`: creates the `u_dlt_types` table with a field of every type and the choices of `u_choice`, and the
  `u_dlt_pages` and `u_dlt_incremental` tables.
- `load`:
  - upserts the records in `seed/data.py` on `u_seed_id` and deletes seeded records that are no longer in it.
    Then it creates and deletes 2 records once, so `sys_audit_delete` has records of the table.
  - inserts the missing records of `u_dlt_pages`: two full pages of 100 and a short one. `u_when` of the first 150
    is empty, more than a page of records without a cursor value.
  - inserts the missing records of `u_dlt_incremental`: 1050 records with unique `u_updated_at` values in 2024,
    five chunks of 200 and a short one for the standard incremental tests.

  The platform sets `sys_updated_on` on every write, so the seed cannot set it.
- `capture`: writes the `sys_dictionary` fields, a Table API page and a CSV export of `u_dlt_types` to `cases/`.
  The unit tests read them with every reader and compare the values with `seed/data.py`.
- `capture-hidden`, not part of `all`: writes a Table API page and a CSV export of `u_dlt_pages` while a field ACL
  hides `sys_updated_on`. The Table API leaves the field out. The export leaves its values empty and unquoted.
  Before you run it, create the ACL:
  1. Elevate to `security_admin`.
  2. Add a `record` ACL with operation `read` on `u_dlt_pages.sys_updated_on`, **Admin overrides** cleared and the
     script `answer = false;`.
  3. After the capture, deactivate the ACL.
