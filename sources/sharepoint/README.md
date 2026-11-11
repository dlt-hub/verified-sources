# SharePoint

Maintenance notes of the SharePoint source for developers and coding agents. The source reads list
items with the Graph [delta query](https://learn.microsoft.com/graph/api/listitem-delta) and the
files of document libraries through a read-only fsspec filesystem over Graph, with dlt's
`filesystem` resource. Setup and usage are in the
[dlt docs](https://dlthub.com/docs/dlt-ecosystem/verified-sources/sharepoint).

## Changelog

### 1.1.0
**Upgrade**: safe, the incremental state carries over.
- new `sharepoint_files` resource `<table_name>_files`: the file listing, not selected by default.
  Select it to load file metadata with the rows
- fixed `SharepointFileSystem`: fsspec clients such as pandas read the default library instead of
  the library named in the url

### 1.0.0
**Upgrade**: new source.
- new `sharepoint_list` and `sharepoint_lists`: list items synced with the delta query and merged
  on `id`, items deleted in SharePoint kept with `_deleted` or deleted with the `hard_delete` hint,
  `included_columns` with internal column names
- new `sharepoint_folder`, `read_files` and `sharepoint_files`: new or modified files of a library
  folder, from `start_date` on or with your own `incremental` on `modification_date`; rows of csv,
  jsonl, parquet and excel files (one table per sheet) with `_dlt_file_url`, `_dlt_file_name` and
  `_dlt_file_modified_at`, merged on `_dlt_file_url`; `to_decimal` converter for amounts
- new `SharepointFileSystem`: fsspec filesystem registered as `sharepoint://<library>/<path>`

## Extension points

- A file type: a value of `TFileType` and a `read_<type>` reader in `READERS` of `readers.py`.
- Another collection of the site, e.g. site pages or attachments: a resource on
  `site_client(**credentials).paginate(...)`, see `list_items`.
- Another reader: any transformer taking `FileItem`s can follow `sharepoint_folder`, and
  `read_files` can follow any listing of `FileItem`s, e.g. dlt's `filesystem` on a bucket.
- File metadata: `ITEM_FIELDS` and `SharepointFileSystem._entry`.
- List fields to keep or drop: `item_row`. Lists `sharepoint_lists` picks without `list_titles`:
  `user_list_titles`.
- Deleted and copied files: the drive delta query would report them; today the listing walks the
  folder on every run.

## Maintenance & Troubleshooting

Facts from the test site (November 2026) or from Microsoft's docs.

- **Throttling**: SharePoint counts resource units per app and tenant, 1,250 to 6,250 a minute by
  license count. A `children` page or a delta page without a token costs 2 units, an item or a
  download 1, so a file costs 2: its `info` and its `/content`. 429 and 503 come with `Retry-After`,
  which dlt's client honours. The `ISV|dltHub|dlt-sharepoint/<version>` User-Agent is the format
  SharePoint prioritizes
  ([docs](https://learn.microsoft.com/sharepoint/dev/general-development/how-to-avoid-getting-throttled-or-blocked-in-sharepoint-online)).
- **Tokens**: Entra's client credentials tokens expire after `expires_in`, about an hour, and
  are re-obtained with the client secret. One auth per app serves every client of a process, so a
  source with many resources requests one token, renewed under a lock a minute before it expires.
- **Delta query**: an item may appear more than once in a feed and the last occurrence is its
  state, deleted items come as `id` with the `deleted` facet and no fields, and a delta link encodes
  the `$select` and `$expand` of the first request, hence the state key per column set. A 410 means
  Graph can no longer continue from the link (docs). `$top=500` is accepted: the 1,000 seeded items
  come in two pages (test site), the maximum is not documented.
- **List values**: number columns come as floats, `3` as `3.0`. A date-only column comes as
  midnight of the site's time zone in UTC, `2024-04-01T07:00:00Z` on the Pacific test site. A
  column shown as "Story Points" is `StoryPoints` internally (test site). Lookup and person columns
  come as `<name>LookupId` (docs).
- **Files**: `/content` redirects to a pre-authenticated host whose URL expires after about an hour
  (docs), so every range request starts at Graph. The host may ignore `Range` and send the whole
  file (docs). A `children` listing is not guaranteed to be complete while writes happen, only the
  drive delta query is (docs). OneNote notebooks are `package` items with children (docs) and are
  walked like folders. System libraries such as Site Assets are hidden from `/drives` unless
  `system` is selected (docs), so `library` cannot name them. Files download on the dlt extract
  workers, 5 by default (`workers` in the `[extract]` config section): 16 workers read 236 small
  csv files in 8 s (test site).
- **Timestamps**: `createdDateTime` and `lastModifiedDateTime` follow the client-reported
  `fileSystemInfo`, which sync clients and migration tools set to the local times: a Graph `copy`
  got a fresh modified time, an upload whose `fileSystemInfo` was patched to 2020 read back 2020 in
  both (test site, 2026-11-03). A file that appears with times older than the watermark is skipped
  until `refresh="drop_resources"`. `cTag` and `eTag` are opaque. The listing is in folder order,
  so chunked loads with `add_limit` and `row_order` are not supported.
- **Encodings**: Graph sends no charset. A csv or jsonl file is decoded by its BOM, else by
  `encoding` in `reader_kwargs`, else as UTF-8, so a csv saved by Excel in a Windows code page
  needs `encoding`.
- **Amounts**: Excel stores numbers as doubles and pandas parses csv numbers as doubles;
  `reader_kwargs={"converters": {"Amount": to_decimal}}` loads a column as `decimal`. Converters
  are functions, so they are passed in code, not in config.
- **Permissions**: `Sites.Read.All`, or `Sites.Selected` with read access to the site. Seeding
  needs `Files.ReadWrite.All`.
