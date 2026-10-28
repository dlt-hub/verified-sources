# SharePoint

Maintenance notes of the [SharePoint](https://learn.microsoft.com/graph/api/resources/sharepoint)
source for developers and coding agents. Setup and usage are in the
[dlt docs](https://dlthub.com/docs/dlt-ecosystem/verified-sources/sharepoint).

## Changelog

### 1.0.0
- new `sharepoint_list`: the items of one list, synced with Graph's delta query and merged on
  `id`; items deleted in SharePoint are hard deleted through `_deleted`. `included_columns`
  selects columns by their internal names, `table_name` names the table
- new `sharepoint_lists`: every visible generic list of the site, or the named ones, one
  `sharepoint_list` table per list
- new `sharepoint_files`: the rows of the csv, jsonl, parquet or excel files matching a glob in a
  library folder, listed by dlt's `filesystem` source over `SharepointFileSystem`, read
  incrementally on `modification_date` and merged on `_file_url`, so a modified or emptied file
  replaces its own rows. Each row carries `_file_url`, `_file_name` and `_file_modified_at`.
  Excel workbooks load one table per sheet
- new `SharepointFileSystem`: read-only fsspec filesystem registered as
  `sharepoint://<library>/<path>`, usable from pandas and other fsspec clients

## Extension points

- A file type: a value of `TFileType` and a branch of `readers.read_records`;
  `validate_reader_kwargs` rejects arguments that would fail only after the download.
- Another collection of the site, e.g. site pages or list attachments: a resource on
  `site_client(credentials).paginate(...)`, see `list_resource`.
- More file metadata: `ITEM_FIELDS` and `SharepointFileSystem._entry`.
- List columns to drop or keep: `UI_FIELDS` and `item_fields`.
- Deleted files: Graph's drive delta query would also delete their rows; today the listing
  walks the folder on every run and deleted files keep their rows.

## Seeding

The test site, the Entra app and the lists and files the live tests expect are described in
`tests/sharepoint/README.md`. The app has `Sites.Read.All` only, so the data is created by hand
once and the tests never write to the site. There are no captured responses; the unit tests build
Graph payloads from the API reference in `tests/sharepoint/utils.py`.

## Tests

- Unit tests mock Graph with `requests_mock` at the URL level, including the token endpoint and
  the pre-authenticated download host; `conftest.py` sets fake credentials for them.
- Live tests run on CI only when `[sources.sharepoint.credentials]` is in `DLT_SECRETS_TOML`.
- Never run two pytest processes in the repository at once: the storage fixture of one run wipes
  the load packages of the other.

## Operations

- **Throttling**: Graph answers 429 and 5xx with `Retry-After`, which dlt's REST client honours
  with its default retries.
- **Tokens**: the client credentials token is requested on the first call and refreshed a minute
  before it expires. `SharepointFileSystem` keeps the storage options, the secret included, on
  the instance, so do not log or pickle it.
- **Lists**: each run reads one delta page sequence per list, with the full fields of changed
  items only. A stored delta link that Graph rejects (404 or 410, after about 30 days without a
  run), or a changed `list_title` or `included_columns` behind a table, raises and asks for
  `refresh="drop_resources"`. Lookup and multi-value columns load as json; set
  `max_table_nesting` on the source for child tables.
- **Files**: every run lists the folder tree (`children`, 200 items per request) and downloads
  only files newer than the stored `modification_date`, in ranges through the pre-authenticated
  download url Graph redirects to. All files of one glob share one table, so files of different
  shapes widen it; use one `sharepoint_files` per file shape. Renamed files count as new files.
- **Permissions**: the app needs the Graph application permission `Sites.Read.All`, or
  `Sites.Selected` with read access granted to the site.
