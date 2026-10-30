# SharePoint

Maintenance notes of the [SharePoint](https://learn.microsoft.com/graph/api/resources/sharepoint)
source for developers and coding agents. Setup and usage are in the
[dlt docs](https://dlthub.com/docs/dlt-ecosystem/verified-sources/sharepoint).

## Changelog

### 1.0.0
- new `sharepoint_list`: a resource named after the list, its items synced with Graph's delta
  query and merged on `id`; items deleted in SharePoint keep their row with `_deleted` set, or
  are deleted with the `hard_delete` hint on `_deleted`, and `_dlt_list_idx` keeps the last
  occurrence of an item Graph sends twice in one run.
  `included_columns` selects columns by their internal names, `with_name` renames the table
- new `sharepoint_lists`: every visible generic list of the site, or the named ones, one
  `sharepoint_list` table per list; `hard_delete=True` sets the hint on every table
- new `sharepoint_folder`: dlt's `filesystem` resource over a library folder, new or modified
  files only; `read_files`: a transformer reading csv, jsonl, parquet or excel files into rows
  with `file_url`, `file_name` and `file_modified_at` (overwriting same-named file columns),
  merged on `file_url` so a modified file replaces its own rows, Excel workbooks one table
  per sheet, files downloaded in parallel on dlt's extract workers; `sharepoint_files`:
  the two piped together. `sharepoint_folder` also feeds dlt's `read_csv`, `read_jsonl` and
  `read_parquet`, and `read_files` also reads from dlt's `filesystem` resource
- new `SharepointFileSystem`: read-only fsspec filesystem registered as
  `sharepoint://<library>/<path>`, usable from pandas and other fsspec clients

## Extension points

- A file type: a value of `TFileType`, a `read_<type>` function in `readers.py` and its entry in
  `READERS`.
- Another collection of the site, e.g. site pages or list attachments: a resource on
  `site_client(**credentials).paginate(...)`, see `list_items`.
- Another reader: any transformer taking `FileItem`s can follow `sharepoint_folder`, and
  `read_files` can follow any listing of `FileItem`s, e.g. dlt's `filesystem` on a bucket.
- More file metadata: `ITEM_FIELDS` and `SharepointFileSystem._entry`.
- List columns to drop or keep: `item_row`, which keeps every field Graph returns except
  the `@odata.*` annotations.
- Lists `sharepoint_lists` picks without `list_titles`: `user_list_titles`.
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
- Live tests need `[sources.sharepoint.credentials]`, on CI from `DLT_SECRETS_TOML`; without it
  they fail, they do not skip.
- Never run two pytest processes in the repository at once: the storage fixture of one run wipes
  the load packages of the other.

## Operations

- **Throttling**: Graph answers 429 and 5xx with `Retry-After`, which dlt's REST client honours
  for up to `runtime.request_max_attempts` (5) attempts per request. SharePoint counts resource
  units per app and tenant (from 1,250 a minute in tenants up to 1,000 licenses); a file costs
  about 2 (its info and its `/content`), a page of a listing or of a delta query 1 or 2. Every
  request carries the User-Agent `ISV|dltHub|dlt-sharepoint/<version>`, which SharePoint
  prioritizes over undecorated traffic.
- **Tokens**: the client credentials token is requested on the first call and renewed when it
  expires. `SharepointFileSystem` keeps the storage options, the secret included, on the
  instance, so do not log or pickle it.
- **Lists**: each run reads one delta page sequence per list, `LIST_PAGE_SIZE` (500) items a
  page, with the full fields of changed items only. Graph may send an item more than once in a
  feed; `_dlt_list_idx`, dlt's column for positions in a list, counts the rows of a run and
  its `dedup_sort` makes the merge keep the last one. A deleted item comes as its id only, so
  by default its row is replaced by one with `_deleted` set and the other columns empty;
  `sharepoint_lists(hard_delete=True)`, or
  `apply_hints(columns={"_deleted": {"hard_delete": True}})` on a `sharepoint_list`, deletes
  the row instead. The delta link is stored per list and column set, so changing `included_columns`
  starts a full sync of the same table, in which rows deleted since the last run stay (use
  `refresh="drop_resources"` to drop them). A stored link that Graph rejects with 410 (when it can no
  longer track changes from that link, e.g. after a long time without a run) raises and asks for `refresh="drop_resources"`, because a plain
  re-read would keep rows deleted in between. Multi-value columns become child tables, as
  everywhere in dlt;
  setting `source.max_table_nesting = 0` on a `sharepoint_lists` source keeps them as json. The
  hint is not set on the resource: in dlt 1.31 a resource-level `max_table_nesting` overwrites
  the table's `x-normalizer` on a run without data, after which `drop_resources` no longer
  drops the table.
- **Picking lists**: without `list_titles`, `sharepoint_lists` reads the site's lists when the
  source is created and loads every visible `genericList`, so a list added in SharePoint becomes a
  new table on the next run; pin `list_titles` for a fixed set of tables. Titles that normalize to
  the same table name share one table and overwrite each other's items, because item ids count
  from 1 in every list.
- **Files**: every run lists the folder tree (`children`, Graph's default page of 200 items)
  and downloads only files newer than, or not yet seen at, the stored `modification_date`, in
  ranges through the pre-authenticated download url Graph redirects to. `read_files` yields one
  `dlt.defer` call per file: files up to `PREFETCH_MAX_BYTES` (16 MiB) download whole on dlt's
  extract workers (`extract.workers`, 5 by default) and are parsed in the main thread, larger
  ones are read in ranges as their rows are parsed. A download host that ignores `Range` and
  sends the whole file is cut to the requested range. The watermark is kept
  per `table_name`, so a changed folder or glob needs `refresh="drop_resources"`, as with any
  incremental resource. All files of one glob share one table, so files of different shapes
  widen it; use one `sharepoint_files` per file shape. Renamed files count as new files; a
  workbook replaced by a blank file keeps its sheet tables.
- **Permissions**: the app needs the Graph application permission `Sites.Read.All`, or
  `Sites.Selected` with read access granted to the site.
