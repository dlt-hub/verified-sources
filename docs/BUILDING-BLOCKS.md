
# DLT Building Blocks

If you are new to `dlt` complete the [Getting started](https://dlthub.com/docs/getting-started) and the [Walkthroughs](https://dlthub.com/docs/walkthroughs/create-a-pipeline) so you have a feeling what is dlt and **how people will use your sources and example pipelinese**.

We strongly suggest that you build your sources out of existing **building blocks**.

* Declare your [resources](https://dlthub.com/docs/general-usage/resource) and group them in [sources](https://dlthub.com/docs/general-usage/source) using Python decorators.
* [Connect the transformers to the resources](https://dlthub.com/docs/general-usage/resource#feeding-data-from-one-resource-into-another) to load additional data or enrich it
* [Create your resources dynamically from data](https://dlthub.com/docs/general-usage/source#create-resources-dynamically)
* [Append, replace and merge your tables](https://dlthub.com/docs/general-usage/incremental-loading)
* [Transform your data before loading](https://dlthub.com/docs/general-usage/resource#customize-resources) and see some [examples of customizations like column renames and anonymization](https://dlthub.com/docs/customizations/customizing-pipelines/renaming_columns)
* [Set up "last value" incremental loading](https://dlthub.com/docs/general-usage/incremental-loading#incremental-loading-with-last-value)
* [Dispatch data to several tables from a single resource](https://dlthub.com/docs/general-usage/resource#dispatch-data-to-many-tables)
* [Set primary and merge keys, define the columns nullability and data types](https://dlthub.com/docs/general-usage/resource#define-schema)
* [Pass config and credentials into your sources and resources](https://dlthub.com/docs/general-usage/credentials)
* Use google oauth2 and service account credentials, database connection strings and define your own complex credentials: see examples below

Concepts to grasp
* [Credentials](https://dlthub.com/docs/general-usage/credentials) and their ["under the hood"](https://github.com/dlt-hub/dlt/blob/devel/docs/technical/secrets_and_config.md)
* [Schemas, naming conventions and data normalization](https://dlthub.com/docs/general-usage/schema).
* [How we distribute sources to our users](DISTRIBUTION.md)

Building blocks used right:
* [Create dynamic resources for tables by reflecting a whole database](https://github.com/dlt-hub/verified-sources/blob/master/sources/sql_database/__init__.py#L56)
* [Incrementally dispatch github events to separate tables](https://github.com/dlt-hub/verified-sources/blob/master/sources/github/__init__.py#L70)
* [Read the participants for each deal using transformers and pipe operator](https://github.com/dlt-hub/verified-sources/blob/master/sources/pipedrive/__init__.py#L67)
* [Read the events for each ticket by attaching transformer to resource explicitly](https://github.com/dlt-hub/verified-sources/blob/master/sources/hubspot/__init__.py#L125)
* [Set `tags` column data type to complex to load them as JSON/struct](https://github.com/dlt-hub/verified-sources/blob/master/sources/zendesk/__init__.py#L108)
* Typical use of `merge` with incremental load for endpoints returning a list of updates to entities in [Shopify source](https://github.com/dlt-hub/verified-sources/blob/master/sources/shopify_dlt/__init__.py#L36).
* A `dlt` mega-combo in `pipedrive` source, where the deals from `deal` endpoint are [fed into](https://github.com/dlt-hub/verified-sources/blob/master/sources/pipedrive/__init__.py#L113) `deals_flow` resource to obtain events for a particular deal. [Both resources use `merge` write disposition and incremental load to get just the newest updates](https://github.com/dlt-hub/verified-sources/blob/master/sources/pipedrive/__init__.py#L103). [The `deals_flow` is dispatching different event types to separate tables with `dlt.mark.with_table_name`](https://github.com/dlt-hub/verified-sources/blob/master/sources/pipedrive/__init__.py#L135).
* An example of using JSONPath expression to get cursor value for incremental loading. In pipedrive some objects have `timestamp` property and others `update_time`. [The dlt.sources.incremental('update_time|modified') expression lets you bind the incremental to either](https://github.com/dlt-hub/verified-sources/blob/master/sources/pipedrive/__init__.py#L176).
* If your source/resource needs google credentials, just use `dlt` built-in credentials as we do in [google sheets](https://github.com/dlt-hub/verified-sources/blob/master/sources/google_sheets/__init__.py#L26) and [google analytics](https://github.com/dlt-hub/verified-sources/blob/master/sources/google_analytics/__init__.py#L32). Also note how `credentials.to_native_credentials()` is used to initialize google api client.
* If your source/resource accepts several different credential types look how [we deal with 3 different types of Zendesk credentials](https://github.com/dlt-hub/verified-sources/blob/master/sources/zendesk/helpers/credentials.py#L10)
* See database connection string credentials [applied to sql_database source](https://github.com/dlt-hub/verified-sources/blob/master/sources/sql_database/__init__.py#L22)
