# SharePoint tests

- `test_sharepoint_graph.py`, `test_sharepoint_filesystem.py`, `test_sharepoint_list.py`: unit tests. Graph is mocked with `requests_mock`. No credentials needed.
- `test_sharepoint_source.py`: live tests against the test site. They only read data. The expected values of `test_seeded_full_and_incremental` come from `seed/data.py`.

## Test site

The tests use a team site in a Microsoft 365 developer tenant, in the Pacific time zone, and an Entra ID app with
admin-consented application permissions: `Sites.Read.All` for the tests, `Files.ReadWrite.All` and `Sites.Manage.All` for the seed. To set up a new one:

1. Create a team site, for example `dlthub-test`.
2. Register an app in Entra ID, create a client secret, add the application permissions and grant admin consent.
3. Find the site id with `GET https://graph.microsoft.com/v1.0/sites/<host>.sharepoint.com:/sites/dlthub-test?$select=id`.
4. Put the credentials in `sources/.dlt/secrets.toml`. CI reads the same keys from the `DLT_SECRETS_TOML` secret:

   ```toml
   [sources.sharepoint.credentials]
   tenant_id = "..."
   client_id = "..."
   client_secret = "..."
   site_id = "<host>.sharepoint.com,<site-collection-guid>,<web-guid>"
   ```

## Seeding

Run from the repository root. The script resolves credentials from `sources/.dlt` like the tests do:

```sh
uv run python -m tests.sharepoint.seed.seed
```

It uploads the files of `seed/data.py` into the Documents library: the `dlt test` folder with csv, txt and
xlsx files in subfolders, `root_sample.csv` at the library root, `dlt_paging` with 205 one-row csv files, so a
listing needs two Graph pages, and `dlt_incremental` with 25 one-row csv files whose modification times are
unique random seconds of 2021, set through `fileSystemInfo`. Files that are no longer in `data.py` are deleted
from the last two folders. It creates the `dlt archive` document library when missing and uploads
`archive/report_2023.csv` there, a file the Documents library does not have. The run takes about a minute.

The lists cannot be created through the app and are made by hand once:

- `dlt toy tasks`, a blank list with columns `Status` (choice: Open, Done), `DueDate` (date only) and `Story Points` (number), and 5 items:

  | Title | Status | DueDate | Story Points |
  | --- | --- | --- | --- |
  | Write spec | Open | 2024-04-01 | 3 |
  | Build source | Open | 2024-04-08 | 8 |
  | Review PR | Done | 2024-04-15 | 2 |
  | Live test | Open | | 5 |
  | Merge | Done | 2024-04-30 | |

  SharePoint names the `Story Points` column `StoryPoints` internally; `included_columns` uses internal names.
- `dlt toy paging`, a list imported from a CSV with columns `n` and `label` and 1,000 rows, so the items need several Graph pages.
  The import names the columns `Title` (n) and `field_1` (label) internally:

  ```sh
  { echo 'n,label'; for i in $(seq 1 1000); do echo "$i,item $i"; done; } > dlt_toy_paging.csv
  ```
