# Google Sheets

## Prepare your data

We recommend to to use [Named Ranges](link to gsheets) to indicate which data should be extracted from a particular spreadsheet and this is how this source
will work by default - when called with without setting any other options. All the named ranges will be converted into tables named after them and stored in the
destination.
* You can let the spreadsheet users to add and remove tables by just adding/removing the ranges, you do not need to configure the pipeline again.
* You can indicate exactly the fragments of interest and only this data will be retrieved so it is the fastest.
* You can name database tables by changing the range names.

If you are not happy with the workflow above, you can:
* Disable it by setting `get_named_ranges` option to False
* Enable retrieving all sheets/tabs with `get_sheets` option set to True
* Pass a list of ranges as supported by Google Sheets in `range_names`

### Make sure your data has headers and is a proper table
**First row of any extracted range should contain headers**. Please make sure:
1. The header names are strings and are unique.
2. That all the columns that you intend to extract have a header.
3. That data starts exactly at the origin of the range - otherwise source will remove padding but it is a waste of resources!

When source detects any problems with headers or table layout **it will issue a WARNING in the log** so it makes sense to run your pipeline script manually/locally and fix all the problems.
1. Columns without headers will be removed and not extracted!
2. Columns with headers that does not contain any data will be removed.
2. If there's any problems with reading headers (ie. header is not string or is empty or not unique): **the headers row will be extracted as data** and automatic header names will be used.
3. Empty rows are ignored
4. `dlt` will normalize range names and headers into table and column names - so they may be different in the database than in google sheets. Prefer small cap names without special characters!

### Data Types
`dlt` normalizer will use first row of data to infer types and will try to coerce following rows - creating variant columns if that is not possible. This is a standard behavior.
**date time** and **date** types are also recognized and this happens via additional metadata that is retrieved for the first row.

## Passing the spreadsheet id/url and explicit range names


## The `spreadsheet_info` table



## Setup credentials
We recommend to use service account for any production deployments.