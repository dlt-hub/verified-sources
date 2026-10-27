# SharePoint tests

- `test_sharepoint_filesystem.py`, `test_sharepoint_list.py`: unit tests. Graph is mocked with `requests_mock`. No credentials needed.
- `test_sharepoint_source.py`: live tests against the test site. They only read data.

## Test site

The tests use a SharePoint site in a Microsoft 365 developer tenant and an Entra ID app with the Microsoft Graph
application permission `Sites.Read.All` and admin consent. To set up a new one:

1. Create a team site, for example `dlthub-test`.
2. Register an app in Entra ID, create a client secret, add the `Sites.Read.All` application permission and grant admin consent.
3. Find the site id with `GET https://graph.microsoft.com/v1.0/sites/<host>.sharepoint.com:/sites/dlthub-test?$select=id`.
4. Put the credentials in `sources/.dlt/secrets.toml`. CI reads the same keys from the `DLT_SECRETS_TOML` secret:

   ```toml
   [sources.sharepoint.credentials]
   tenant_id = "..."
   client_id = "..."
   client_secret = "..."
   site_id = "<host>.sharepoint.com,<site-collection-guid>,<web-guid>"
   ```

## Test data

The live tests expect this content on the site. Create it by hand once; the tests never write to the site.

**Documents library**

- Folder `dlt test` with:
  - `report_2024_01.csv`: header `order_id,customer,amount,order_date` and rows `1,Alice,10.50,2024-01-03`, `2,Bob,7.25,2024-01-05`, `3,Carla,12.00,2024-01-09`
  - `report_2024_02.csv`: same header, rows `4,Dan,4.00,2024-02-01` and `5,Eve,,2024-02-11`
  - `report_2024_03.csv`: same header, row `7,Gus,1.00,2024-03-01`
  - `report_notes.txt` and `summary.csv` (`a,b` / `1,2`), which the glob must skip
  - subfolder `archive` with `report_2023_12.csv`: same header, row `0,Old,1.00,2023-12-01`
  - subfolder `R&D (2024)` with `report_mixed.xlsx`: sheet `Data` with header `id | code | due` and rows `1 | 101 | 2024-03-01`, `2 | A-7 | (empty)`, `3 | 103.5 | 2024-03-03`, plus a sheet `Notes` with any text
- `root_sample.csv` at the library root: `a,b` / `1,2` / `3,4`
- Folder `dlt_paging` with 205 one-row CSVs `page_001.csv` to `page_205.csv`, each `n` / `<number>`, so a listing needs two Graph pages

**Lists**

- `dlt toy tasks`, a blank list with columns `Status` (choice: Open, Done), `DueDate` (date only) and `Story Points` (number), and 5 items:

  | Title | Status | DueDate | Story Points |
  | --- | --- | --- | --- |
  | Write spec | Open | 2024-04-01 | 3 |
  | Build source | Open | 2024-04-08 | 8 |
  | Review PR | Done | 2024-04-15 | 2 |
  | Live test | Open | | 5 |
  | Merge | Done | 2024-04-30 | |

  SharePoint names the `Story Points` column `StoryPoints` internally; `select` uses internal names.
- `dlt toy paging`, a list imported from a CSV with columns `n` and `label` and 1,000 rows, so the items need several Graph pages.

The local files for the upload can be generated with:

```sh
for i in $(seq -w 1 205); do printf 'n\n%s\n' "$i" > page_$i.csv; done
{ echo 'n,label'; for i in $(seq 1 1000); do echo "$i,item $i"; done; } > dlt_toy_paging.csv
```
