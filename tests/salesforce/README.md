# Salesforce tests

- `test_salesforce_table.py`: unit tests. Salesforce is mocked with `requests_mock`; real responses come from the
  fixtures in `cases/`. No credentials needed.
- `test_salesforce_source.py`: live tests against the seeded test org. They only read data.

## Test org

The tests use a Salesforce Developer Edition org (5 MB data storage, 15,000 API calls and 10,000 Bulk
API 2.0 query jobs per day). To set up a new one:

1. Sign up for a Developer Edition org and note its My Domain, e.g. `mycompany-dev-ed.develop.my.salesforce.com`.
2. In Setup, create an External Client App with OAuth enabled, the `api` scope and the client credentials flow
   enabled. Set **Run As** to a System Administrator: the seed deploys metadata as this user.
3. Put the credentials in `sources/.dlt/secrets.toml`. CI reads the same keys from the secrets vault:

   ```toml
   [sources.salesforce.credentials]
   consumer_key = "..."
   consumer_secret = "..."
   domain = "mycompany-dev-ed.develop.my"  # My Domain without .salesforce.com
   ```

## Seeding

Run from the repository root. The script resolves credentials from `sources/.dlt` like the tests do:

```sh
uv run python -m tests.salesforce.seed.seed all  # or deploy | load | capture
```

- `deploy`: creates the `DltTypes__c` and `DltPages__c` custom objects and adds the `Dlt_Seed_Id__c` external id
  to standard objects. It assigns the `Dlt_Seed` permission set to the Run As user and makes them a
  Marketing User, which is needed to create campaigns.
- `load`: writes the records in `seed/data.py`. Most are upserted on their seed id. Pricebook entries,
  opportunity line items, roles, tasks and events are created only when missing. Then it creates and deletes
  2 `DltTypes__c` records, a line item, and an opportunity with its line item, so the recycle bin has records
  for `queryAll`.
- `capture`: writes the `DltTypes__c` describe and its Bulk API result page to `cases/`. The unit tests compare
  these with `seed/data.py`.

`DltTypes__c` also has a record over 2 MiB in six long text fields and formula fields that divide, which
Salesforce rounds to the field scale.

`load` is safe to repeat. Pause between live test runs to stay within the org's daily limits.

## Seeded vs. existing data

A new org comes with its own users, roles, accounts, products and so on. The tests never count all rows of a
standard object:

- **Records of standard objects:** counted by `Dlt_Seed_Id__c IS NOT NULL`.
- **Tasks and events:** counted by the `dlt ` prefix of their subject.
- **Users and roles:** only lower bounds are checked.
- **`DltTypes__c` and `DltPages__c`:** hold only seeded records.

Deleted records stay in the recycle bin for 15 days, so the tests do not count them.

## Changing seed data

Upserts only add and change data, so the org can keep data that `seed/data.py` no longer has:

- **A value removed from a record** is not cleared. Set it to `None` instead.
- **A record removed from `data.py`** is not deleted. Delete it in Salesforce. The tests compare all
  `DltTypes__c` records and count every record with a seed id.
- **A field removed from the metadata** is not deleted from the object.
- **Records created when missing** (pricebook entries, roles, tasks, events) are not updated. Delete them
  in Salesforce to recreate them.

After changing `DltTypes__c` records or fields, run `load` and `capture`, then commit `cases/`. If Salesforce
stores a value differently than seeded (e.g., rounds a number or trims whitespace), add it to `STORED_AS` in
`seed/data.py`.
